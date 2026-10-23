package evaluation

import (
	"batchscheduler/src/staging"
)

// evaluateProgress 只启动一次 Run，不再生成 Assume、Bind 等中间步骤的决定。
func evaluateProgress(d staging.Decision, record *staging.Record, facts attemptFacts) staging.Decision {
	if record.ExecutionBlocked {
		d.Reason = "ExecutionBlocked"
		return d
	}
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
