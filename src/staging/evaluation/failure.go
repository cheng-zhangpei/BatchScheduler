package evaluation

import (
	"batchscheduler/src/staging"
	errorengine "batchscheduler/src/staging/error_engine"
	"batchscheduler/src/workqueue"
)

// evaluateErrors 把已知错误交给错误引擎，再给结果加上 Unit 编号和 Record 版本。
// Observe 已把支持的 Pod 调度异常映射成 FailureInfo；未支持的原因仍需逐条补规则。
func evaluateErrors(d staging.Decision, record *staging.Record, facts attemptFacts) (staging.Decision, bool) {
	members := facts.failedMembers
	if facts.invalidated || record.Unit.Mode() == workqueue.AllTogether || record.Failure != nil {
		// Atomic 处理整轮；整操作错误无法定位成员时，Partial 也处理整轮。
		members = facts.members
	}
	result, ok := errorengine.Decide(errorengine.Input{
		Invalidated: facts.invalidated, Failure: facts.failure, Members: members,
		RecoveryComplete: record.RecoveryComplete, ResponsibilitiesSettled: record.ResponsibilitiesSettled,
	})
	if !ok {
		return d, false
	}
	d.Reason, d.Failure, d.Members = result.Reason, result.Failure, result.Members
	switch result.Action {
	case errorengine.Recover:
		d.Action, d.Operation = staging.ActionExecute, staging.OperationRecover
	case errorengine.ReturnFail:
		d.Action, d.To = staging.ActionReturn, workqueue.QueueFail
	case errorengine.Release:
		d.Action = staging.ActionRelease
	}
	return d, true
}
