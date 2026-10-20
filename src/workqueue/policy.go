package workqueue

import (
	"batchscheduler/src/group"
	corev1 "k8s.io/api/core/v1"
)

// DefaultPolicy 只决定首次入队，不执行回滚、资源检查或队列间迁移。
// 组级调度结果由未来的感知机汇总；待确定区的结果明确后再交回 Select。
// LastFailure 只采用感知机/调度路径确认的内部记录，不把 Pending 或容器等待等同于失败。
func DefaultPolicy(input SelectionInput, unit Unit) (QueueDecision, error) {
	wait := QueueDecision{To: QueueNone}
	if input.View == nil || input.View.Group == nil || input.GroupState == nil {
		return wait, nil
	}
	if input.View.Group.DeletionTimestamp != nil || input.GroupState.Phase == group.GroupDeleting {
		return wait, nil
	}
	state := unit.State()
	if state.Queue != QueueNone || (state.Phase != UnitNew && state.Phase != UnitFrozen) {
		return wait, nil
	}
	members := unit.Members()
	if len(members) == 0 {
		return wait, nil
	}
	if unit.Mode() == AllTogether {
		// 首次提交要求整组当前成员全部归入同一个单元，不提前验证节点可放置性。
		if len(members) != int(input.View.Group.Spec.Members) || len(input.View.Members) != len(members) {
			return wait, nil
		}
	}
	// 验证本轮成员仍存在且属于本组，不让旧引用随新事件直接入队。
	for _, ref := range members {
		var pod *corev1.Pod
		for _, observed := range input.View.Members {
			if observed.Pod != nil && observed.Pod.UID == ref.UID && observed.Pod.Name == ref.Name && observed.Group == unit.Key().Group {
				pod = observed.Pod
				break
			}
		}
		if pod == nil || pod.DeletionTimestamp != nil {
			return wait, nil
		}
		podState := input.PodStates[ref.UID]
		if podState == nil || podState.UnitID != unit.Key().ID {
			return wait, nil
		}
		if podState.Phase == group.PodBinding || podState.Phase == group.PodBindingUnknown {
			return wait, nil // 绑定结果待确定，不在这里推断失败或重新投递。
		}
	}
	// 先处理已确认异常，再考虑普通调度机会。
	// Atomic 使用组级失败范围；Partial 的默认单元只有一个 Pod。
	if unit.Mode() == AllTogether && input.GroupState.LastFailure != nil {
		failure := input.GroupState.LastFailure
		return QueueDecision{To: QueueFail, Reason: failure.Reason, Message: failure.Message}, nil
	}
	for _, ref := range members {
		if failure := input.PodStates[ref.UID].LastFailure; failure != nil {
			return QueueDecision{To: QueueFail, Reason: failure.Reason, Message: failure.Message}, nil
		}
	}
	if unit.Mode() == AllTogether && input.GroupState.Phase == group.GroupRecovering {
		return wait, nil // 等感知和恢复路径明确下一步，不能当作新任务直接提交。
	}
	for _, ref := range members {
		if input.PodStates[ref.UID].Phase != group.PodUnscheduled {
			return wait, nil
		}
		for _, member := range input.View.Members {
			if member.Pod != nil && member.Pod.UID == ref.UID {
				pod := member.Pod
				if pod.Spec.NodeName != "" || (pod.Status.Phase != corev1.PodPending && pod.Status.Phase != "") {
					return wait, nil
				}
			}
		}
	}
	// Retry 由 Fail 的恢复判断决定；FastRetry 待严格准入规则确定后接入。
	return QueueDecision{To: QueueActive}, nil
}
