package main

import (
	"context"
	"fmt"

	"batchscheduler/src/group"
	execution "batchscheduler/src/scheduler/executor"
	"batchscheduler/src/staging"
	"batchscheduler/src/utils"
	"batchscheduler/src/workqueue"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// workQueueHandlers 保存下面三个处理函数需要的队列和节点查询器。
type workQueueHandlers struct {
	queues *workqueue.WorkQueue
	nodes  corelisters.NodeLister
}

// newStagingHandlers 把队列查询、迁移、首次入队和结束回送函数交给 Buffer。
// Execute 只连接执行器入口，具体执行步骤由 Executor 自己组织。
func newStagingHandlers(queues *workqueue.WorkQueue, nodes corelisters.NodeLister, executor *execution.Executor) staging.Handlers {
	h := workQueueHandlers{queues: queues, nodes: nodes}
	return staging.Handlers{
		QueueSnapshot: queues.GroupSnapshot,
		WaitingGroups: queues.WaitingGroups,
		Transition:    h.transition,
		Execute:       executor.Execute,
		SelectGroup:   h.selectGroup,
		Handoff:       h.handoff,
	}
}

// transition 把 Buffer 的队列迁移决定交给 WorkQueue。
func (h workQueueHandlers) transition(ctx context.Context, request workqueue.TransitionRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return h.queues.Transition(request)
}

// selectGroup 过滤组视图、读取当前节点，再交给 Select 判断首次入队。
func (h workQueueHandlers) selectGroup(_ context.Context, view *group.View) error {
	if !utils.ShouldEnqueue(view) {
		return nil
	}
	nodes, err := h.nodes.List(labels.Everything())
	if err != nil {
		return err
	}
	return h.queues.Select(workqueue.SelectRequest{View: view, Nodes: nodes})
}

// handoff 处理旧 Unit 的结束通知：Release 只清旧记录，Return 还要通过 Select 接住下一轮。
// 返回成功后，Buffer 才把旧 Unit 标 Done 并删除旧 Record。
func (h workQueueHandlers) handoff(ctx context.Context, request staging.Handoff) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch request.Decision.Action {
	case staging.ActionRelease:
		return h.queues.ReleaseUnit(request.Unit.Key())
	case staging.ActionReturn:
		if request.Decision.To != workqueue.QueueFail {
			return fmt.Errorf("失败回送目前只支持进入 Fail")
		}
		// Evaluate 已确认恢复和资源收尾完成；Select 接住下一轮，Buffer 再删旧 Record。
		return h.queues.Select(workqueue.SelectRequest{Return: &workqueue.ReturnRequest{
			Previous: request.Unit.Key(), Members: request.Decision.Members,
			Failure: request.Decision.Failure, RecoveryComplete: true,
		}})
	default:
		return fmt.Errorf("未知 Handoff 动作: %s", request.Decision.Action)
	}
}
