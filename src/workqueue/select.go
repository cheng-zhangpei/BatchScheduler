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

// SelectRequest 描述触发来源，Pod 与 View 必须且只能提供一个。
// 调用方不指定 Scope，组级或 Pod 级处理由 Select 根据来源和组模式确定。
type SelectRequest struct {
	Pod   *corev1.Pod
	View  *group.View
	Nodes []*corev1.Node
}

// SelectionInput 汇合 informer 事实和调度器内部记录。
// Nodes 只是 informer 观察值，不包含 Assume 资源占用，也不是一致性资源快照。
// Policy 同步读取这些记录，不得修改、保留指针或执行 API/Agent 调用。
type SelectionInput struct {
	Scope      SelectionScope
	Pod        *corev1.Pod // PodScope 时为缓存中的当前 Pod；Atomic 会提升为 GroupScope。
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
// TODO：感知机的待确定区确认结果后可重新提交视图；旧单元归属释放后才能组织新一轮。
// main 初始同步及 Center 的组级 Apply 都可调用；旧单元归属清理仍待交接适配器接入。
func (w *WorkQueue) Select(request SelectRequest) error {
	if (request.Pod == nil) == (request.View == nil) {
		return fmt.Errorf("Select 必须且只能提供 Pod 或组视图中的一个")
	}
	if request.Pod != nil {
		return w.selectPod(request.Pod, request.Nodes)
	}
	return w.selectView(request.View, request.Nodes, nil)
}

// selectPod 解析 Pod 来源，无 Group 归属直接忽略；不要求指定 schedulerName。
// 事件只提供身份，实际判断读取当前组视图；旧 UID 的事件不能影响同名新对象。
// Atomic 提升为组级处理，Partial 只处理本次 Pod，不顺带投递其他成员。
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
		if !selectedUnit(input, unit) || w.isStaged(unit) {
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
		if !isQueue(decision.To) {
			return fmt.Errorf("Select 收到未知目标队列: %q", decision.To)
		}
		if err := unit.Freeze(); err != nil {
			return err
		}
		var failure *group.FailureInfo
		if decision.Reason != "" || decision.Message != "" {
			failure = &group.FailureInfo{Reason: decision.Reason, Message: decision.Message}
		}
		// 先记录状态，再发布到队列，避免消费者取到仍标记为 Frozen 的单元。
		if err := unit.MarkQueued(decision.To, failure, decision.WaitingFor); err != nil {
			return err
		}
		switch decision.To {
		case QueueActive:
			w.Active.Add(unit)
		case QueueFail:
			w.Fail.Put(Failure{Unit: unit, Reason: decision.Reason, Message: decision.Message, RecoveryComplete: decision.RecoveryComplete})
		case QueueRetry:
			w.Retry.Put(Retry{Unit: unit, WaitingFor: decision.WaitingFor})
		case QueueFastRetry:
			w.FastRetry.Add(unit)
		}
	}
	return nil
}

func selectedUnit(input SelectionInput, unit Unit) bool {
	if input.Scope != PodScope {
		return true
	}
	for _, member := range unit.Members() {
		if member.UID == input.Pod.UID {
			return true
		}
	}
	return false
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
// Atomic 复用整组的组装单元；Partial 当前每个 Pod 一个单元，保持异常处理的细粒度。
// Partial 以后仍可扩展批量提交；组调度结果的整体判断留给感知机。
// 只处理组成员；不要求成员显式指定本调度器的 schedulerName。
func (w *WorkQueue) assemble(input SelectionInput, record *selectionState, key group.Key) error {
	mode := SubmitMode(input.View.Group.Spec.SubmitMode)
	if mode == "" {
		mode = AllTogether
	}
	if mode != AllTogether && mode != AllowPartial {
		return fmt.Errorf("未知的组提交模式: %q", mode)
	}
	// 视图可能包含同名重建的 Pod。先校验全组，再清理尚未冻结单元的旧引用。
	current := make(map[PodRef]bool, len(input.View.Members))
	for _, member := range input.View.Members {
		if member.Pod == nil || member.Pod.UID == "" || member.Group != key {
			return fmt.Errorf("Select 收到无效成员或成员组身份不匹配")
		}
		current[PodRef{Name: member.Pod.Name, UID: member.Pod.UID}] = true
	}
	for _, unit := range record.units {
		if !selectedUnit(input, unit) || unit.State().Phase != UnitNew || unit.Frozen() || w.isStaged(unit) {
			continue
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
		if input.Scope == PodScope && pod.UID != input.Pod.UID {
			continue
		}
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
			if mode == AllTogether && unit.Mode() == mode && unit.State().Phase == UnitNew && !unit.Frozen() {
				target = unit
				break
			}
		}
		if target == nil {
			w.nextID++
			id := fmt.Sprintf("unit-%d", w.nextID)
			target = w.MakeUnit(UnitKey{Group: key, ID: id}, mode)
			if target == nil || target.Key() != (UnitKey{Group: key, ID: id}) || target.Mode() != mode {
				return fmt.Errorf("单元工厂返回了空单元或不匹配的身份/模式")
			}
			record.units[id] = target
			record.group.UnitIDs[id] = struct{}{}
		}
		if err := target.AddMember(PodRef{Name: pod.Name, UID: pod.UID}); err != nil {
			return err
		}
		state.UnitID = target.Key().ID
	}
	return nil
}
