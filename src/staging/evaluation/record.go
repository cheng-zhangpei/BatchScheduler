package evaluation

import (
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// EvaluateRecord 检查一个 Unit 下一步做什么，只返回一个 Decision，不修改 Buffer。
// 外面的 Evaluate 对各 Unit 分别调用它，所以一批可以同时有人完成、有人失败、有人等待。
func EvaluateRecord(key workqueue.UnitKey, record *staging.Record, invalidGroup bool) staging.Decision {
	decision := staging.Decision{Group: key.Group, Unit: &key, Revision: record.Revision, Action: staging.ActionWait}

	// 1. 先检查能不能继续判断：操作没做完就等，观察信息不足也先等。
	if reason := evaluationWaitReason(record); reason != "" {
		decision.Reason = reason
		return decision
	}
	facts := summarizeAttempt(record, invalidGroup)
	if facts.unknownMember {
		decision.Reason = "MemberObservationUnknown"
		return decision
	}

	// 2. 报喜：识别已经成功的本轮。只产生 Release，不在这里删除记录。
	if completed, ok := evaluateCompletion(decision, record, facts); ok {
		return completed
	}

	// 3. 报忧：本轮过期或明确失败，交给错误引擎决定如何处理。
	if problem, ok := evaluateErrors(decision, record, facts); ok {
		return problem
	}

	// 4. 只启动整轮执行；预占、绑定等步骤由 Executor 内部推进。
	return evaluateProgress(decision, record, facts)
}
