package workqueue

import "batchscheduler/src/group"

// DefaultPolicy 只选择新 Unit 的首次入队位置。
// assemble 已经挑出当前可接收的 Pod；这里不再逐个扫描 View 验证一遍。
// Atomic 是否已经收齐，由 Buffer 在启动执行前判断，队列不等整组到齐。
func DefaultPolicy(input SelectionInput, unit Unit) (QueueDecision, error) {
	wait := QueueDecision{To: QueueNone}
	state := unit.State()
	if state.Queue != QueueNone || len(unit.Members()) == 0 {
		return wait, nil
	}
	if input.GroupState.Phase == group.GroupDeleting || input.GroupState.Phase == group.GroupRecovering {
		return wait, nil
	}
	if unit.Mode() == AllTogether && input.GroupState.LastFailure != nil {
		f := input.GroupState.LastFailure
		return QueueDecision{To: QueueFail, Reason: f.Reason, Message: f.Message}, nil
	}
	for _, ref := range unit.Members() {
		pod := input.PodStates[ref.UID]
		if pod.Phase != group.PodUnscheduled {
			return wait, nil
		}
		if f := pod.LastFailure; f != nil {
			return QueueDecision{To: QueueFail, Reason: f.Reason, Message: f.Message}, nil
		}
	}
	return QueueDecision{To: QueueActive}, nil
}
