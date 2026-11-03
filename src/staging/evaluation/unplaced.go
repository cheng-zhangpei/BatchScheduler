package evaluation

import (
	"time"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	errorengine "batchscheduler/src/staging/error_engine"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// evaluateUnplacedMembers 只安排 Partial 的未放置成员，不结束整个 Unit。
// 已暂占成员继续执行；可等待集群变化的未放置成员直接回送 Retry。
// 返回 false 只表示这里没有提出“部分回送”，外面的 EvaluateRecord 仍会继续判断等待、恢复等动作。
func evaluateUnplacedMembers(d staging.Decision, r *staging.Record, invalidGroup bool) (staging.Decision, bool) {
	if invalidGroup || r.Unit.Mode() != workqueue.AllowPartial || r.Resources == nil ||
		len(r.Resources.Assumed) == 0 || len(r.Resources.Unplaced) == 0 || r.Failure != nil {
		return d, false
	}
	// ExecutionPhase 来自 Executor.Report，表示执行器已经做到哪一步：
	// Assumed：已接纳成员的暂占成功，可以让它们继续执行、让未放置成员单独回送。
	// Binding：已接纳成员正在绑定；本函数只处理未参与绑定的 Unplaced，不等其他成员结束。
	// Bound：已接纳成员的绑定已成功；剩下的 Unplaced 仍需要交回，不能直接丢掉。
	switch r.ExecutionPhase {
	case staging.PhaseAssumed, staging.PhaseBinding, staging.PhaseBound:
	default:
		// Filter/Score/Plan 阶段尚未确认接纳结果，不能仅凭候选位置拆出下一轮。
		// Recovering/Recovered 阶段则是在处理整轮失败，交给原来的恢复、回送流程。
		return d, false
	}
	if r.Observed == nil {
		// Observe 还没为这份 Record 填入集群观察，暂时不知道原组和这些 Pod 当前是否还在。
		return d, false
	}
	if !r.Observed.Exists {
		// Observe 没找到原 UID 的组，可能已删除或同名重建；这里不再给旧组组织重试成员。
		return d, false
	}
	if r.Observed.Deleting {
		// 组还存在，但用户已经请求删除。接下来应处理旧轮次的结束，而不是为它补发任务。
		return d, false
	}

	// 1. 同一种错误的成员一起交回。错误不同的成员留待下一次评估，避免混用重试条件。
	var failure *group.FailureInfo
	var members []workqueue.PodRef // 这个集合里面的都有同一种错误
	for _, member := range r.Resources.Unplaced {
		if r.ReturnedMembers[member.Pod] {
			continue // 已经接到新 Unit，不再重复交回。
		}
		fact, known := r.Observed.Members[member.Pod]
		state, held := r.Members[member.Pod]
		// 旧 Pod 已删除、替换或绑定，就不能再拿这个旧引用组织一次调度。
		if !known || !fact.Known || !fact.Present || fact.Deleting || fact.NodeName != "" ||
			(fact.Status.Phase != "" && fact.Status.Phase != corev1.PodPending) ||
			!held || state.Phase != group.PodUnscheduled {
			continue
		}
		if failure == nil {
			copyFailure := member.Failure
			failure = &copyFailure
		}
		if member.Failure.Reason == failure.Reason {
			members = append(members, member.Pod)
		}
	}
	if len(members) == 0 {
		return d, false
	}

	// 2. 使用同一个错误引擎入口。这里的“收尾完成”只指这些未放置成员，不是整轮。
	result, ok := errorengine.Decide(errorengine.Input{
		Failure: failure, Members: members, RecoveryComplete: true, ResponsibilitiesSettled: true,
	})
	if !ok || result.Action != errorengine.ReturnFail {
		return d, false
	}

	// 3. 交给 Apply 的是部分回送，不能套用会删除整个 Record 的 ActionReturn。
	d.Action, d.To = staging.ActionReturnMembers, workqueue.QueueFail
	if _, err := workqueue.PlanRetry(result.Reason, time.Time{}); err == nil {
		d.To = workqueue.QueueRetry
	}
	d.Reason, d.Failure, d.Members = result.Reason, result.Failure, result.Members
	return d, true
}
