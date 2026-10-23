package evaluation

import (
	"batchscheduler/src/group"
	"batchscheduler/src/staging"
)

// evaluationWaitReason 返回“为什么现在要等”，没有原因则返回空字符串。
func evaluationWaitReason(record *staging.Record) string {
	if record.ExecutionBlocked && record.PendingAction != "" {
		return "ExecutionBlocked"
	}
	if record.PendingAction != "" {
		// 防止第一次操作未结束，就因重复通知再执行一次。
		return "OperationInFlight"
	}
	for _, state := range record.Members {
		if state.Phase == group.PodBinding || state.Phase == group.PodBindingUnknown {
			// 防止绑定结果未知时重绑或撤销仍在使用的预留。
			return "BindingResultPending"
		}
	}
	if record.Observed == nil {
		return "AwaitingObservation"
	}
	return ""
}
