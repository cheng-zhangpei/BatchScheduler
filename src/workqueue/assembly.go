package workqueue

import (
	"fmt"

	"batchscheduler/src/group"
	corev1 "k8s.io/api/core/v1"
)

// assemble 先实现按 Pod 查归属、复用组装单元；不在这里决定封口条件。
// 两种模式都复用尚未冻结的同组单元；Partial 不必等整组到齐，也不限制为单 Pod。
// 通过 Buffer.ContainsPod 排除已经在执行的成员；Active 尚未出队时仍能追加。
// 只处理组成员；不要求成员显式指定本调度器的 schedulerName。
func (w *WorkQueue) assemble(input SelectionInput, record *selectionState, key group.Key) error {
	mode := SubmitMode(input.View.Group.Spec.SubmitMode)
	if mode == "" {
		mode = AllTogether
	}
	if mode != AllTogether && mode != AllowPartial {
		return fmt.Errorf("未知的组提交模式: %q", mode)
	}
	definition := group.DefinitionFor(input.View.Group)
	current, err := w.pendingMembers(input.View, key)
	if err != nil {
		return err
	}
	if err := refreshAssembly(record, mode, definition, current); err != nil {
		return err
	}
	for _, member := range input.View.Members {
		pod := member.Pod
		if !current[PodRef{Name: pod.Name, UID: pod.UID}] {
			continue
		}
		state := record.pods[pod.UID]
		if state == nil {
			state = &group.PodState{Phase: group.PodUnscheduled}
			record.pods[pod.UID] = state
		}
		if state.UnitID != "" {
			if record.units[state.UnitID] == nil {
				return fmt.Errorf("Pod %s 引用了不存在的 Unit %s", pod.Name, state.UnitID)
			}
			continue // 已有归属，事件更新不能重复创建或追加。
		}
		if state.Phase != group.PodUnscheduled {
			continue // 本地已预留/绑定中的 Pod 不能再次组装。
		}
		target := assemblyTarget(record, mode, state.LastFailure)
		if target == nil {
			var err error
			target, err = w.newUnit(key, mode, definition)
			if err != nil {
				return err
			}
			record.units[target.Key().ID] = target
			record.group.UnitIDs[target.Key().ID] = struct{}{}
		}
		if err := target.AddMember(PodRef{Name: pod.Name, UID: pod.UID}); err != nil {
			return err
		}
		state.UnitID = target.Key().ID
	}
	return nil
}

// pendingMembers 一次挑出还没绑定、仍可执行、也没有交给 Buffer 的成员。
func (w *WorkQueue) pendingMembers(view *group.View, key group.Key) (map[PodRef]bool, error) {
	current := make(map[PodRef]bool, len(view.Members))
	for _, member := range view.Members {
		pod := member.Pod
		if pod == nil || pod.UID == "" || member.Group != key {
			return nil, fmt.Errorf("Select 收到无效成员或成员组身份不匹配")
		}
		if pod.Spec.NodeName != "" || pod.DeletionTimestamp != nil || (pod.Status.Phase != "" && pod.Status.Phase != corev1.PodPending) {
			continue
		}
		if w.Staging != nil && w.Staging.ContainsPod(key, pod.UID) {
			continue
		}
		current[PodRef{Name: pod.Name, UID: pod.UID}] = true
	}
	return current, nil
}

// refreshAssembly 让尚未出队的 Unit 跟随最新声明，并移除已消失、已绑定的成员。
// Fail、Retry 和已经交出的 Unit 不在这里改名单。
func refreshAssembly(record *selectionState, mode SubmitMode, definition *group.Definition, current map[PodRef]bool) error {
	for _, unit := range record.units {
		if unit.Frozen() {
			continue
		}
		if unit.Mode() == mode {
			if err := unit.SetDefinition(definition); err != nil {
				return err
			}
		}
		for _, ref := range unit.Members() {
			if current[ref] {
				continue
			}
			if err := unit.RemoveMember(ref); err != nil {
				return err
			}
			if state := record.pods[ref.UID]; state != nil && state.UnitID == unit.Key().ID {
				delete(record.pods, ref.UID)
			}
		}
	}
	return nil
}

// assemblyTarget 优先复用同组可追加的 Unit。尚未首次入队的失败成员另外成批，
// 已经从 Retry 回到 Active 的成员则可以和新成员一起尝试。
func assemblyTarget(record *selectionState, mode SubmitMode, failure *group.FailureInfo) Unit {
	for _, unit := range record.units {
		if unit.Mode() != mode || unit.Frozen() {
			continue
		}
		if mode == AllowPartial && unit.State().Phase == UnitNew && !sameAssemblyFailure(unit, record, failure) {
			continue
		}
		return unit
	}
	return nil
}

// sameAssemblyFailure 让同一 Partial Unit 的成员使用相同的首次入队处理方式。
// 例如正常成员一起进 Active，资源不足的成员另成一批进 Fail，不把两批混起来。
func sameAssemblyFailure(unit Unit, record *selectionState, failure *group.FailureInfo) bool {
	for _, ref := range unit.Members() {
		existing := record.pods[ref.UID].LastFailure
		if existing == nil || failure == nil {
			if existing != nil || failure != nil {
				return false
			}
		} else if existing.Reason != failure.Reason || existing.Message != failure.Message {
			return false
		}
	}
	return true
}
