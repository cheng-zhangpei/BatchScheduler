package workqueue

import (
	"fmt"

	"batchscheduler/src/group"
	corev1 "k8s.io/api/core/v1"
)

// ReturnRequest 是 Buffer 明确交回的一轮失败任务，不是普通的 Pod 变化通知。
// Previous 指向旧执行轮次；Members 指定下一轮成员，可以一次包含多个 Pod。
// RecoveryComplete 必须由 Buffer 确认：恢复和资源收尾完成后，才允许重新排队。
type ReturnRequest struct {
	Previous         UnitKey
	Members          []PodRef
	Failure          *group.FailureInfo
	RecoveryComplete bool
}

// selectReturn 仍属于 Select：按已有失败决策组装新 Unit，首次放入 Fail。
// 不再调用普通 Policy 猜测目标队列，也不把仍被 Buffer 持有的成员当作重复提交跳过。
// 三步：检查旧轮次和回送成员 → 准备新 Unit → 替换队列记录并入队。
// 成功后 Buffer 才删除旧 Record；这里不修改 Buffer，也不操作集群资源。
func (w *WorkQueue) selectReturn(request ReturnRequest) error {
	if !request.RecoveryComplete || request.Failure == nil || request.Failure.Reason == "" || len(request.Members) == 0 {
		return fmt.Errorf("失败回送需要成员、错误原因及恢复完成确认")
	}
	if w.Views == nil {
		return fmt.Errorf("失败回送缺少组视图读取器")
	}
	view, err := w.Views.Get(request.Previous.Group.Namespace, request.Previous.Group.Name)
	if err != nil {
		return fmt.Errorf("失败回送读取组视图: %w", err)
	}

	w.selectMu.Lock()
	defer w.selectMu.Unlock()
	record := w.groups[request.Previous.Group]
	if record == nil || record.units[request.Previous.ID] == nil {
		return fmt.Errorf("失败回送的旧 Unit 已不存在，不能再次创建下一轮")
	}
	old := record.units[request.Previous.ID]
	if err := validateReturn(request, old, record, view); err != nil {
		return err
	}

	// 先在局部准备下一轮。工厂或 Unit 方法报错时，旧记录和实际队列都还没动。
	next, err := w.newUnit(request.Previous.Group, old.Mode())
	if err != nil {
		return err
	}
	for _, ref := range request.Members {
		if err := next.AddMember(ref); err != nil {
			return err
		}
	}
	decision := QueueDecision{To: QueueFail, Reason: request.Failure.Reason,
		Message: request.Failure.Message, RecoveryComplete: true}
	if err := prepareEnqueue(next, decision); err != nil {
		return err
	}

	// 到这里已经没有会返回错误的步骤。一次替换旧关联，并把新任务放进 Fail。
	// 普通 Select 同时使用 selectMu，不能趁成员换轮次的间隙重复组装。
	removeUnit(record, old)
	record.units[next.Key().ID] = next
	record.group.UnitIDs[next.Key().ID] = struct{}{}
	for _, ref := range next.Members() {
		failure := *request.Failure
		record.pods[ref.UID] = &group.PodState{UnitID: next.Key().ID, Phase: group.PodUnscheduled, LastFailure: &failure}
	}
	w.publishUnit(next, decision)
	return nil
}

// validateReturn 只检查这张回送任务单是否还能执行，不修改任何记录。
func validateReturn(request ReturnRequest, old Unit, record *selectionState, view *group.View) error {
	state := old.State()
	if state.Phase != UnitProcessing || state.Queue != QueueNone {
		return fmt.Errorf("只能回送已交给 Buffer 处理的旧 Unit")
	}
	key := request.Previous.Group
	if view == nil || view.Group == nil || view.Group.Namespace != key.Namespace || view.Group.Name != key.Name || view.Group.UID != key.UID || view.Group.DeletionTimestamp != nil {
		return fmt.Errorf("原组已删除或重建，不能把旧成员重新入队")
	}
	mode := SubmitMode(view.Group.Spec.SubmitMode)
	if mode == "" {
		mode = AllTogether
	}
	if mode != old.Mode() {
		return fmt.Errorf("组提交模式已变化，不能按旧模式回送")
	}

	// 只允许回送旧 Unit 中的成员，不能顺带取走同组其他 Unit 正在处理的 Pod。
	oldMembers := make(map[PodRef]bool)
	for _, ref := range old.Members() {
		oldMembers[ref] = true
	}
	returned := make(map[PodRef]bool)
	for _, ref := range request.Members {
		if !oldMembers[ref] || returned[ref] {
			return fmt.Errorf("回送成员不属于旧 Unit 或重复: %s", ref.Name)
		}
		returned[ref] = true
	}
	if mode == AllTogether && (len(returned) != len(oldMembers) || len(oldMembers) != int(view.Group.Spec.Members) || len(view.Members) != len(oldMembers)) {
		return fmt.Errorf("Atomic 必须回送整轮成员，不能只回送失败的几个 Pod")
	}
	observed := make(map[PodRef]*corev1.Pod)
	for _, member := range view.Members {
		if member.Group == key && member.Pod != nil {
			observed[PodRef{Name: member.Pod.Name, UID: member.Pod.UID}] = member.Pod
		}
	}
	for ref := range oldMembers {
		podState := record.pods[ref.UID]
		if podState == nil || podState.UnitID != old.Key().ID {
			return fmt.Errorf("旧 Unit 的成员关联已变化: %s", ref.Name)
		}
		pod := observed[ref]
		if pod == nil {
			return fmt.Errorf("原成员已不存在，需重新评估: %s", ref.Name)
		}
		if returned[ref] {
			// 已绑定、删除中或终态 Pod 不能靠改本地队列重新调度。
			if pod.Spec.NodeName != "" || pod.DeletionTimestamp != nil || (pod.Status.Phase != "" && pod.Status.Phase != corev1.PodPending) {
				return fmt.Errorf("回送成员已绑定、删除中或已结束: %s", ref.Name)
			}
		}
	}
	// Partial 没被列入失败范围的成员不进 Fail。若它还未绑定，Buffer 删旧 Record 后
	// 会重新通知评估，让它通过普通 Select 再次组装；已绑定或已结束的成员则不再入队。
	return nil
}
