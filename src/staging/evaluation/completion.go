package evaluation

import (
	"batchscheduler/src/staging"
)

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
