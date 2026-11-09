package evaluation

import (
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// ObservedBindingSuccess 确认单 Pod 的 Bind 已返回未知结果，但集群里同一个 Pod 已落在计划节点。
// 最后一条反馈必须对应当前 Pending，才能区分“Worker 已返回”和“Bind 仍在执行”。
func ObservedBindingSuccess(record *staging.Record) (workqueue.PodRef, bool) {
	feedback := record.LastFeedback
	if record.PendingAction == "" || record.PendingDecision != staging.ActionExecute || record.PendingOperation != staging.OperationRun ||
		feedback == nil || feedback.ActionID != record.PendingAction || feedback.Kind != staging.FeedbackUnknown ||
		feedback.Failure == nil || feedback.Failure.Reason != workqueue.ReasonBindingUnknown {
		return workqueue.PodRef{}, false
	}
	observed := record.Observed
	members := record.Unit.Members()
	if len(members) != 1 || observed == nil || !observed.Exists || observed.Deleting || observed.Generation != record.Generation {
		return workqueue.PodRef{}, false
	}
	ref := members[0]
	fact := observed.Members[ref] // 这个索引包含 Pod UID，同名重建的 Pod 不能确认旧请求。
	node := record.Plan[ref]
	return ref, fact.Known && fact.Present && !fact.Deleting && node != "" && fact.NodeName == node
}

// evaluateCompletion 检查能否结束本轮：都已绑定，且没有失败或过期问题。
// 没有异常且都绑定了但收尾未完成时，返回 Wait；全部收尾后才返回 Release。
func evaluateCompletion(d staging.Decision, record *staging.Record, facts attemptFacts) (staging.Decision, bool) {
	if facts.invalidated || facts.failure != nil || !facts.allBound {
		return d, false
	}
	d.Reason = "MembersBoundAwaitingSettlement"
	if record.ResponsibilitiesSettled {
		d.Action, d.Reason = staging.ActionRelease, "AttemptComplete"
	}
	return d, true
}
