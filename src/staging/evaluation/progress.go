package evaluation

import (
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// evaluateProgress 只启动一次 Run，不再生成 Assume、Bind 等中间步骤的决定。
func evaluateProgress(d staging.Decision, record *staging.Record, facts attemptFacts) staging.Decision {
	if record.ExecutionBlocked {
		d.Reason = "ExecutionBlocked"
		return d
	}
	// 这里就是回答能不能启动一次executor.Run了
	if record.ExecutionPhase == "" && len(record.Plan) == 0 && facts.allUnscheduled {
		if facts.anyBound {
			// 防止外部已经绑定部分成员后，继续按旧计划预占或重复绑定。
			d.Reason = "PartialBindingAwaitingFeedback"
			return d
		}
		d.Action, d.Operation, d.Reason = staging.ActionExecute, staging.OperationRun, "ReadyToExecute"
		return d
	}
	d.Reason = "AwaitingExecutionFeedback"
	return d
}

// waitingForAtomicMembers 只针对尚未启动的 Atomic。执行过的轮次继续按原反馈处理。
func waitingForAtomicMembers(record *staging.Record, facts attemptFacts) bool {
	if record.Unit.Mode() != workqueue.AllTogether || record.ExecutionPhase != "" || len(record.Plan) != 0 {
		return false
	}
	definition := record.Unit.Definition()
	return definition == nil || len(facts.members) != int(definition.Spec.Members)
}
