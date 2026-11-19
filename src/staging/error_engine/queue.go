package errorengine

import "batchscheduler/src/workqueue"

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
	// 匹配这些错误就会返回一个transition的决定
	//todo(cheng) 对于这些错误的重试还要细细权衡
	case workqueue.ReasonUnschedulable, workqueue.ReasonInsufficientResources, workqueue.ReasonNodeUnavailable,
		workqueue.ReasonPlacementConstraints, workqueue.ReasonDependencyNotReady,
		workqueue.ReasonTransientSchedulingError, workqueue.ReasonBindingRejected:
		// WorkQueue 根据成员失败次数登记退避截止时间。
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
		// wake.Changed表示这次收到的事件。
		wake.Changed = input.Wake.Changed
	}
	plan := entry.Retry.Plan
	// 只等退避的任务，检查截止时间。没到就返回 false，到了就往下走。
	if plan.Condition == workqueue.RetryOnBackoff {
		if plan.NotBefore.IsZero() || input.Now.Before(plan.NotBefore) {
			return Decision{}, false
		}
		// 等资源等事件的任务，检查有没有收到相关变化。之前没收到、这次也没收到，就返回 false。
	} else if !entry.Retry.ChangeObserved && !plan.Condition.Matches(wake.Changed) {
		return Decision{}, false
	}
	// 相关事件先到、退避还没到时，也通知 Transition 把“变化发生过”记下来。
	if input.Now.Before(plan.NotBefore) && wake.Changed == "" {
		return Decision{}, false
	}
	// 它这次不会搬任务；以后时间到了，就不必再等一次相同事件。
	request := workqueue.TransitionRequest{Unit: entry.Unit, ExpectedRevision: entry.State.Revision,
		From: workqueue.QueueRetry, To: workqueue.QueueActive, Wake: &wake}
	return Decision{Action: Transition, Reason: "RetryConditionObserved", Transition: &request}, true
}
