package executor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"batchscheduler/src/scheduler/planner"
	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/scheduler/steps"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// Executor 自己组织整条执行线。外部只提供数据读取器和唯一的 Report 回报通道。
type Executor struct {
	steps            *steps.Runner
	config           Config
	Report           func(context.Context, staging.Feedback) error
	mu               sync.Mutex
	requests         chan staging.Execution
	accepted         map[executionKey]bool
	started, stopped bool
}

type executionKey struct {
	Unit     workqueue.UnitKey
	ActionID string
}

func NewExecutor(views workqueue.GroupViewReader, resources *resourcecache.Cache, config Config) *Executor {
	runner := steps.NewRunner(views, resources)
	runner.Client, runner.BindTimeout = config.Client, config.BindTimeout
	return &Executor{steps: runner, config: config, requests: make(chan staging.Execution, 64), accepted: make(map[executionKey]bool)}
}

// SetGroupContextReader 在启动前接入 Buffer，让组规划能查到同组其他轮次与成功历史。
func (e *Executor) SetGroupContextReader(reader staging.GroupContextReader) {
	e.steps.Groups = reader
}

// SetPlanner 在启动前替换组规划插件；用户实现 planner 中的接口后，通过这里接入。
func (e *Executor) SetPlanner(p planner.Planner) {
	e.steps.Planner = p
}

// Execute 快速接收一次请求，不在 Apply 的评估线程里等待过滤或绑定。
// 返回 nil 只代表已接收。后续进度、结束或阻塞全部通过 Report 返回。
func (e *Executor) Execute(ctx context.Context, request staging.Execution) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", staging.ErrExecutionNotStarted, err)
	}
	if e.Report == nil || (request.Operation != staging.OperationRun && request.Operation != staging.OperationRecover) {
		return fmt.Errorf("%w: Report 未接入或执行请求类型无效", staging.ErrExecutionNotStarted)
	}
	if request.ActionID == "" || request.Unit.ID == "" || len(request.Members) == 0 {
		return fmt.Errorf("%w: 缺少执行身份或成员", staging.ErrExecutionNotStarted)
	}
	request = copyExecution(request)
	key := executionKey{Unit: request.Unit, ActionID: request.ActionID}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return fmt.Errorf("%w: 执行器已停止", staging.ErrExecutionNotStarted)
	}
	if e.accepted[key] {
		return nil
	}
	select {
	case e.requests <- request:
		e.accepted[key] = true
		return nil
	default:
		return fmt.Errorf("%w: 执行队列已满", staging.ErrExecutionNotStarted)
	}
}

// Run 只组织串行调度，把已 Assume 的任务交给 Inflight；main 仍只需启动一次 Run。
func (e *Executor) Run(ctx context.Context) error {
	if err := e.config.Validate(); err != nil {
		return err
	}
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return fmt.Errorf("执行工作线程只能启动一次")
	}
	e.started = true
	e.mu.Unlock()
	inflight := newInflight(e.config)
	inflight.start(ctx, e)
	defer func() {
		e.mu.Lock()
		e.stopped = true
		e.mu.Unlock()
		// 停止接单后，等已启动的绑定协程退出，main 才能关闭 Buffer 使用的数据库。
		inflight.closeAndWait()
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case request := <-e.requests:
			if ctx.Err() != nil {
				return nil
			}
			if err := inflight.Reserve(ctx); err != nil {
				return nil
			}
			a, err := e.prepareExecution(ctx, request)
			if a == nil {
				e.completeRequest(ctx, request, err)
				inflight.Release()
				continue
			}
			inflight.Submit(a)
		}
	}
}

// completeRequest 在调度失败或后台绑定结束后，移除这次请求的接收标记。
// 绑定还在运行时保留标记，同一请求再次提交不会重复启动。
func (e *Executor) completeRequest(ctx context.Context, request staging.Execution, err error) {
	e.mu.Lock()
	delete(e.accepted, executionKey{Unit: request.Unit, ActionID: request.ActionID})
	e.mu.Unlock()
	if err != nil && ctx.Err() == nil {
		slog.Error("执行结果反馈失败，保留 Buffer 记录等待处理", "unit", request.Unit, "actionID", request.ActionID, "error", err)
	}
}

func copyExecution(request staging.Execution) staging.Execution {
	request.Definition = request.Definition.DeepCopy()
	request.Members = append([]workqueue.PodRef(nil), request.Members...)
	if request.Plan != nil {
		plan := make(map[workqueue.PodRef]string, len(request.Plan))
		for ref, node := range request.Plan {
			plan[ref] = node
		}
		request.Plan = plan
	}
	if request.Failure != nil {
		failure := *request.Failure
		request.Failure = &failure
	}
	return request
}
