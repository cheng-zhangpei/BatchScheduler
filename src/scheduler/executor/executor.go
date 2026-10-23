package executor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"batchscheduler/src/scheduler/steps"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// Executor 自己组织整条执行线。外部只提供数据读取器和唯一的 Report 回报通道。
type Executor struct {
	steps            *steps.Runner
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

func NewExecutor(views workqueue.GroupViewReader, nodes corelisters.NodeLister, pods corelisters.PodLister) *Executor {
	return &Executor{steps: steps.NewRunner(views, nodes, pods), requests: make(chan staging.Execution, 64), accepted: make(map[executionKey]bool)}
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

// Run 是唯一的执行工作线程，由 main 在 informer 就绪后启动。
// Executor 内部调用步骤和 Report，不需要 Apply 再开一个监控线程。
func (e *Executor) Run(ctx context.Context) error {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return fmt.Errorf("执行工作线程只能启动一次")
	}
	e.started = true
	e.mu.Unlock()
	defer func() { e.mu.Lock(); e.stopped = true; e.mu.Unlock() }()
	for {
		select {
		case <-ctx.Done():
			return nil
		case request := <-e.requests:
			if ctx.Err() != nil {
				return nil
			}
			err := e.runExecution(ctx, request)
			e.mu.Lock()
			delete(e.accepted, executionKey{Unit: request.Unit, ActionID: request.ActionID})
			e.mu.Unlock()
			if err != nil && ctx.Err() == nil {
				slog.Error("执行结果反馈失败，保留 Buffer 记录等待处理", "unit", request.Unit, "actionID", request.ActionID, "error", err)
			}
		}
	}
}

func copyExecution(request staging.Execution) staging.Execution {
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
