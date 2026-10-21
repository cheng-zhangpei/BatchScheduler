package errorengine

import (
	"time"

	"batchscheduler/src/workqueue"
)

// decideQueuedError 先看任务在 Fail 还是 Retry，再调用对应的判断函数。
func decideQueuedError(input Input) (Decision, bool) {
	switch input.Queued.State.Queue {
	case workqueue.QueueFail:
		return decideFailedQueue(input)
	case workqueue.QueueRetry:
		return decideRetryQueue(input)
	default:
		return Decision{}, false
	}
}

// decideFailedQueue 判断 Fail 中的任务能否去 Retry：先确认恢复完成，再检查错误类型。
func decideFailedQueue(input Input) (Decision, bool) {
	entry := input.Queued
	if !entry.Failure.RecoveryComplete {
		return Decision{}, false
	}
	request := workqueue.TransitionRequest{
		Unit: entry.Unit, ExpectedRevision: entry.State.Revision,
		From: workqueue.QueueFail, To: workqueue.QueueRetry,
		Reason: entry.Failure.Reason, RecoveryComplete: true,
	}
	switch entry.Failure.Reason {
	case workqueue.ReasonUnschedulable, workqueue.ReasonInsufficientResources, workqueue.ReasonNodeUnavailable,
		workqueue.ReasonPlacementConstraints, workqueue.ReasonDependencyNotReady:
		// 这些错误可以进入 Retry，但之后仍要等资源、节点等相关条件变化。
	case workqueue.ReasonTransientSchedulingError, workqueue.ReasonBindingRejected:
		// 第一版统一退避一秒；具体时长后续再讨论，不在此新增复杂策略。
		request.RetryAfter = input.Now.Add(time.Second)
	default:
		// 权限、无效声明、绑定未知及未识别错误都不能自动重试。
		return Decision{}, false
	}
	return Decision{Action: Transition, Reason: request.Reason, Transition: &request}, true
}

// decideRetryQueue 判断 Retry 中的任务能否回 Active。
// 时间型等截止时间；事件型还必须收到过对应变化，例如资源不足要等资源变化。
func decideRetryQueue(input Input) (Decision, bool) {
	entry := input.Queued
	wake := workqueue.RetryWake{Now: input.Now}
	if input.Wake != nil {
		wake.Changed = input.Wake.Changed
	}
	plan := entry.Retry.Plan
	if plan.Condition == workqueue.RetryOnBackoff {
		if plan.NotBefore.IsZero() || input.Now.Before(plan.NotBefore) {
			return Decision{}, false
		}
	} else if !entry.Retry.ChangeObserved && !plan.Condition.Matches(wake.Changed) {
		return Decision{}, false
	}
	if input.Now.Before(plan.NotBefore) && wake.Changed == "" {
		return Decision{}, false
	}
	// 相关事件先到、退避还没到时，也通知 Transition 把“变化发生过”记下来。
	// 它这次不会搬任务；以后时间到了，就不必再等一次相同事件。
	request := workqueue.TransitionRequest{Unit: entry.Unit, ExpectedRevision: entry.State.Revision,
		From: workqueue.QueueRetry, To: workqueue.QueueActive, Wake: &wake}
	return Decision{Action: Transition, Reason: "RetryConditionObserved", Transition: &request}, true
}
