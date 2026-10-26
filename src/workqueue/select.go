package workqueue

import (
	"fmt"

	"batchscheduler/src/group"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

type SelectionScope string

const (
	GroupScope SelectionScope = "Group"
	PodScope   SelectionScope = "Pod"
)

// GroupViewReader 只从缓存补齐上下文，不在事件处理路径查询 API Server。
type GroupViewReader interface {
	Get(namespace, name string) (*group.View, error)
}

// StagingLookup 是未来暂存区的查询边界，不在 workqueue 内实现 binding 或 compaction。
// 已暂存的 Pod 不得重新组装；归属释放和回送由后续暂存区协议协调。
type StagingLookup interface {
	ContainsPod(group.Key, types.UID) bool
}

// SelectRequest 描述触发来源，Pod、View、Return 必须且只能提供一个。
// 调用方不指定 Scope，组级或 Pod 级处理由 Select 根据来源和组模式确定。
type SelectRequest struct {
	Pod    *corev1.Pod
	View   *group.View
	Nodes  []*corev1.Node
	Return *ReturnRequest // Buffer 已经确认失败并完成恢复，交回指定成员。
}

// SelectionInput 汇合 informer 事实和调度器内部记录。
// Nodes 只是 informer 观察值，不包含 Assume 资源占用，也不是一致性资源快照。
// Policy 同步读取这些记录，不得修改、保留指针或执行 API/Agent 调用。
type SelectionInput struct {
	Scope      SelectionScope
	Pod        *corev1.Pod // 触发这次检查的 Pod；Partial 仍可组装同组其他可执行成员。
	View       *group.View
	Nodes      []*corev1.Node
	GroupState *group.State
	PodStates  map[types.UID]*group.PodState
}

// QueueDecision 可以指向四个队列中的任意一个；QueueNone 表示暂不入队。
// Fail 需给出错误，Retry 需给出等待条件，FastRetry 的准入留待单独讨论。
type QueueDecision struct {
	RecoveryComplete bool // 仅 Fail 使用：已恢复或明确没有执行过任何资源操作。
	To               QueueName
	Reason           string
	Message          string
	WaitingFor       string
}

type SelectionPolicy func(SelectionInput, Unit) (QueueDecision, error)

// selectionState 保存单个组的内部记录，成员事实仍来自 View。
type selectionState struct {
	group group.State
	pods  map[types.UID]*group.PodState
	units map[string]Unit
}

// Select 是唯一的首次入队入口：识别处理粒度 → 组装单元 → 策略判断 → 直接分发。
// 已入队单元的队列间迁移由 Transition 负责。
// 普通事件通过 Pod/View 进入；Buffer 失败回送通过 Return 进入，不再走普通事件的归属过滤。
func (w *WorkQueue) Select(request SelectRequest) error {
	sources := 0
	if request.Pod != nil {
		sources++
	}
	if request.View != nil {
		sources++
	}
	if request.Return != nil {
		sources++
	}
	if sources != 1 {
		return fmt.Errorf("Select 必须且只能提供 Pod、组视图或失败回送中的一个")
	}
	if request.Return != nil {
		return w.selectReturn(*request.Return)
	}
	if request.Pod != nil {
		return w.selectPod(request.Pod, request.Nodes)
	}
	return w.selectView(request.View, request.Nodes, nil)
}

// selectPod 解析 Pod 来源，无 Group 归属直接忽略；不要求指定 schedulerName。
// 事件只提供身份，实际判断读取当前组视图；旧 UID 的事件不能影响同名新对象。
// Pod 事件只是触发检查；读到组视图后，组装本轮可接收的同组成员。
func (w *WorkQueue) selectPod(pod *corev1.Pod, nodes []*corev1.Node) error {
	key, belongs := group.GroupForPod(pod)
	if !belongs {
		return nil
	}
	if w.Views == nil {
		return fmt.Errorf("Select 缺少组视图读取器")
	}
	view, err := w.Views.Get(key.Namespace, key.Name)
	if apierrors.IsNotFound(err) {
		return nil // Pod 和 CR 缓存短暂错位，等待后续组事件。
	}
	if err != nil {
		return err
	}
	if view == nil || view.Group == nil || view.Group.UID != key.UID {
		return nil
	}
	for _, member := range view.Members {
		if member.Pod != nil && member.Pod.UID == pod.UID && member.Pod.Name == pod.Name && member.Group == key {
			return w.selectView(view, nodes, member.Pod)
		}
	}
	return nil // 事件成员已不在当前组视图，不用事件旧值重新创建单元。
}

func (w *WorkQueue) selectView(view *group.View, nodes []*corev1.Node, pod *corev1.Pod) error {
	if view == nil || view.Group == nil || view.Group.UID == "" || view.Group.Spec.Members <= 0 {
		return fmt.Errorf("Select 缺少有效的 PodGroup 视图")
	}
	if view.Group.DeletionTimestamp != nil {
		return nil
	}
	w.selectMu.Lock()
	defer w.selectMu.Unlock()
	key := group.Key{Namespace: view.Group.Namespace, Name: view.Group.Name, UID: view.Group.UID}
	record := w.groups[key]
	if record == nil {
		record = &selectionState{
			group: group.State{Phase: group.GroupWaiting, UnitIDs: make(map[string]struct{})},
			pods:  make(map[types.UID]*group.PodState), units: make(map[string]Unit),
		}
		w.groups[key] = record
	}
	// TODO：根据成员事实和已有尝试汇总组状态，处理删除、声明变更和失效成员。
	input := SelectionInput{Scope: GroupScope, View: view, Nodes: nodes, GroupState: &record.group, PodStates: record.pods}
	if pod != nil && SubmitMode(view.Group.Spec.SubmitMode) == AllowPartial {
		input.Scope, input.Pod = PodScope, pod
	}
	if err := w.assemble(input, record, key); err != nil {
		return err
	}
	if w.Policy == nil {
		return nil // 策略尚未配置，保留组装中的单元，不自动入队。
	}
	for _, unit := range record.units {
		if w.isStaged(unit) {
			continue
		}
		state := unit.State()
		if state.Phase != UnitNew && state.Phase != UnitFrozen {
			continue // 已入队、处理中或已结束的单元由后续流转规则负责。
		}
		decision, err := w.Policy(input, unit)
		if err != nil {
			return err
		}
		if decision.To == QueueNone {
			continue
		}
		if err := prepareEnqueue(unit, decision); err != nil {
			return err
		}
		w.publishUnit(unit, decision)
	}
	return nil
}

func (w *WorkQueue) isStaged(unit Unit) bool {
	if w.Staging == nil {
		return false
	}
	for _, member := range unit.Members() {
		if w.Staging.ContainsPod(unit.Key().Group, member.UID) {
			return true
		}
	}
	return false
}

// assemble 先实现按 Pod 查归属、复用组装单元；不在这里决定封口条件。
// 两种模式都复用尚未冻结的同组单元；Partial 不必等整组到齐，也不限制为单 Pod。
// 通过 Buffer.ContainsPod 排除已经在执行的成员，不追加到已入队或已执行的 Unit。
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
	// 视图可能包含同名重建的 Pod。先校验全组，再清理尚未冻结单元的旧引用。
	current := make(map[PodRef]bool, len(input.View.Members))
	for _, member := range input.View.Members {
		if member.Pod == nil || member.Pod.UID == "" || member.Group != key {
			return fmt.Errorf("Select 收到无效成员或成员组身份不匹配")
		}
		current[PodRef{Name: member.Pod.Name, UID: member.Pod.UID}] = true
	}
	for _, unit := range record.units {
		if unit.State().Phase != UnitNew || unit.Frozen() || w.isStaged(unit) {
			continue
		}
		// 仍在组装时可以跟随声明更新；已冻结的旧轮次保留原来的配置。
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
				state.UnitID = ""
			}
		}
	}
	for _, member := range input.View.Members {
		pod := member.Pod
		if w.Staging != nil && w.Staging.ContainsPod(key, pod.UID) {
			continue // 不因队列中找不到它而重复创建 Unit。
		}
		state := record.pods[pod.UID]
		if state == nil {
			state = &group.PodState{Phase: group.PodUnscheduled}
			record.pods[pod.UID] = state
		}
		if pod.Spec.NodeName != "" {
			state.Phase = group.PodBound
			continue
		}
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
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
		var target Unit
		for _, unit := range record.units {
			if unit.Mode() == mode && unit.State().Phase == UnitNew && !unit.Frozen() && !w.isStaged(unit) {
				// 已有明确失败的成员与正常成员分开，避免一个失败拖住整批正常任务。
				if mode == AllowPartial && !sameAssemblyFailure(unit, record, state.LastFailure) {
					continue
				}
				target = unit
				break
			}
		}
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
