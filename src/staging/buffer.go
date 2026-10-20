package staging

import (
	"context"
	"fmt"
	"sync"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	clientqueue "k8s.io/client-go/util/workqueue"
)

// Buffer 实现 Center 接收、观察、评估、动作分发、反馈和释放，以及独立评估 worker。
// 外部调度/绑定执行器和 workqueue 交接由注入的 Handlers 实现，不在这里冒充完成。
//
// records 按执行单元保存独立记录；groups 将同组单元关联起来；owners 防止
// 成员重复在途。Partial 可以同组多单元，Atomic 同时最多接收一个整组单元。
// 三份登记受同一把锁保护，失败时不得留下部分记录。
type Buffer struct {
	mu sync.RWMutex
	// 串行化 Observe 的读缓存和提交，避免较早读取的快照较晚写回。
	// 不在持有 mu 时调用 Store，避免阻塞 ContainsPod/Accept 的索引操作。
	observeMu   sync.Mutex
	views       workqueue.GroupViewReader
	records     map[workqueue.UnitKey]*Record
	groups      map[group.Key]map[workqueue.UnitKey]struct{}
	owners      map[group.Key]map[types.UID]workqueue.UnitKey
	evaluations clientqueue.TypedInterface[group.Key]
	runMu       sync.Mutex
	runStarted  bool
	handlers    Handlers
	nextAction  uint64
}

// NewBuffer 注入缓存视图读取边界并建立去重评估队列，不启动协程或调度。
// views 应为 group.Store，不应在 Get 中发起网络调用或回调 Buffer。
func NewBuffer(views workqueue.GroupViewReader, handlers ...Handlers) *Buffer {
	var effects Handlers
	if len(handlers) > 0 {
		effects = handlers[0]
	}
	return &Buffer{
		handlers: effects,
		views:    views, records: make(map[workqueue.UnitKey]*Record),
		groups:      make(map[group.Key]map[workqueue.UnitKey]struct{}),
		owners:      make(map[group.Key]map[types.UID]workqueue.UnitKey),
		evaluations: clientqueue.NewTyped[group.Key](),
	}
}

// Accept 仅接住已经出队的冻结单元，不执行调度、不合并成员、不清理旧记录。
// 现阶段 Active.Pop 尚未提供交接凭据，Unit 的 Queued 状态也未更新；因此这里只
// 校验来源状态，无法证明已经实际 Pop。调用方须先出队；消费协议以后统一接入。
// 登记成功意味着 Buffer 持有本轮责任
func (b *Buffer) Accept(ctx context.Context, unit workqueue.Unit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if unit == nil {
		return fmt.Errorf("Accept 缺少 Unit")
	}
	key, mode, members := unit.Key(), unit.Mode(), unit.Members()
	// 身份不完整无法可靠去重；未冻结成员可能在登记后改变，使成员索引失真。
	if key.ID == "" || key.Group.Namespace == "" || key.Group.Name == "" || key.Group.UID == "" || !unit.Frozen() || len(members) == 0 {
		return fmt.Errorf("Accept 要求完整身份及非空、已冻结的 Unit")
	}
	if mode != workqueue.AllTogether && mode != workqueue.AllowPartial {
		return fmt.Errorf("Accept 收到未知提交模式: %q", mode)
	}
	names, refs := make(map[string]bool), make(map[workqueue.PodRef]bool)
	uids := make(map[types.UID]bool)
	for _, ref := range members {
		// 不盲信替换的 Unit 实现；重复名称/UID 会造成成员丢失或覆盖。
		if ref.Name == "" || ref.UID == "" || names[ref.Name] || uids[ref.UID] {
			return fmt.Errorf("Accept 成员身份不完整或重复")
		}
		names[ref.Name], uids[ref.UID], refs[ref] = true, true, true
	}

	// 已接收请求可在 Pod 已绑定、组已变化后重放：先查本地交接记录，
	// 不重新执行，也不拿变化后的集群事实推翻已经成功的同一次登记。
	b.mu.RLock()
	exists, err := b.checkDuplicate(key, mode, refs)
	b.mu.RUnlock()
	if exists || err != nil {
		return err
	}
	state := unit.State()
	// Fail/Retry 尚未获准执行；不能绕过 Transition 直接塞入缓冲区。
	if state.Phase != workqueue.UnitQueued || (state.Queue != workqueue.QueueActive && state.Queue != workqueue.QueueFastRetry) {
		// 同一次交接可能刚被并发 Accept 改为 Processing；等待登记锁后再查一次，
		// 不把已经成功接收的重复请求误判成非法来源。
		b.mu.RLock()
		exists, err := b.checkDuplicate(key, mode, refs)
		b.mu.RUnlock()
		if exists || err != nil {
			return err
		}
		return fmt.Errorf("Accept 只接收 Active/FastRetry 来源的待执行单元")
	}
	if b.views == nil {
		return fmt.Errorf("Accept 缺少组视图读取器")
	}
	// 在锁外读缓存；不持有缓冲区锁调用外部组件。缓存不是跨资源原子快照，
	// 这里做入场校验，真正执行前仍须复查，不能把 Accept 当作资源预留证明。
	view, err := b.views.Get(key.Group.Namespace, key.Group.Name)
	if err != nil {
		return fmt.Errorf("Accept 读取组视图: %w", err)
	}
	if err := validateAdmission(view, key.Group, mode, refs); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	// 读缓存期间另一个 Accept 可能已经完成登记；锁内必须再次查重。
	if exists, err := b.checkDuplicate(key, mode, refs); exists || err != nil {
		return err
	}
	for otherKey := range b.groups[key.Group] {
		other := b.records[otherKey]
		// Atomic 是整组的一轮，不能让两个整组轮次并行；旧轮次存在时
		// 不擅自判断它过期并删除。旧模式也参与检查，防止修改 CR 绕过旧责任。
		if mode == workqueue.AllTogether || other.Unit.Mode() == workqueue.AllTogether {
			return fmt.Errorf("Atomic 组仍有未释放单元 %s，不能接收另一轮", otherKey.ID)
		}
		for ref := range other.Members {
			// 同名重建虽 UID 不同，也不能借新 UID 跨过旧成员尚未结清的责任。
			if names[ref.Name] {
				return fmt.Errorf("成员名称 %s 仍由单元 %s 持有", ref.Name, otherKey.ID)
			}
		}
	}
	for _, ref := range members {
		if owner, ok := b.owners[key.Group][ref.UID]; ok {
			return fmt.Errorf("成员 %s 仍由单元 %s 持有", ref.Name, owner.ID)
		}
	}

	// 所有拒绝条件先检查完，再一次性建立记录与索引
	record := &Record{Unit: unit, Members: make(map[workqueue.PodRef]group.PodState, len(members)), Revision: 1, Generation: view.Group.Generation}
	// 所有拒绝条件通过后登记处理中状态；实际 Pop 仍是调用方交接契约。
	if err := unit.MarkProcessing(); err != nil {
		return err
	}
	if b.groups[key.Group] == nil {
		b.groups[key.Group] = make(map[workqueue.UnitKey]struct{})
		b.owners[key.Group] = make(map[types.UID]workqueue.UnitKey)
	}
	for _, ref := range members {
		record.Members[ref] = group.PodState{Phase: group.PodUnscheduled, UnitID: key.ID}
		b.owners[key.Group][ref.UID] = key
	}
	b.records[key] = record
	b.groups[key.Group][key] = struct{}{}
	b.evaluations.Add(key.Group)
	return nil
}

// checkDuplicate 在持锁时调用。相同键必须有相同模式和成员集合，成员顺序无关。
// 不能覆盖既有 Record，否则会丢失在途操作、结果和资源清理责任。
func (b *Buffer) checkDuplicate(key workqueue.UnitKey, mode workqueue.SubmitMode, refs map[workqueue.PodRef]bool) (bool, error) {
	record, ok := b.records[key]
	if !ok {
		return false, nil
	}
	if record.Unit.Mode() != mode || len(record.Members) != len(refs) {
		return true, fmt.Errorf("UnitKey 重用但模式或成员不同: %s", key.ID)
	}
	for ref := range record.Members {
		if !refs[ref] {
			return true, fmt.Errorf("UnitKey 重用但成员不同: %s", key.ID)
		}
	}
	return true, nil
}

func validateAdmission(view *group.View, key group.Key, mode workqueue.SubmitMode, refs map[workqueue.PodRef]bool) error {
	// 同名 CR 重建必须视为新组；删除中的组不再接受新执行责任。
	if view == nil || view.Group == nil || view.Group.Namespace != key.Namespace || view.Group.Name != key.Name || view.Group.UID != key.UID || view.Group.DeletionTimestamp != nil {
		return fmt.Errorf("Accept 组身份已变化或组正在删除")
	}
	declared := workqueue.SubmitMode(view.Group.Spec.SubmitMode)
	if declared == "" {
		declared = workqueue.AllTogether
	}
	// 不允许靠 Unit 自报 Partial 绕开 CR 的 Atomic 约束。
	if declared != mode || view.Group.Spec.Members <= 0 {
		return fmt.Errorf("Accept 单元模式与组声明不符，或组成员数无效")
	}
	if mode == workqueue.AllTogether && (len(refs) != int(view.Group.Spec.Members) || len(view.Members) != len(refs)) {
		return fmt.Errorf("Atomic 单元必须覆盖当前声明的完整组成员")
	}
	found := make(map[workqueue.PodRef]bool, len(refs))
	for _, member := range view.Members {
		pod := member.Pod
		if pod == nil {
			continue
		}
		ref := workqueue.PodRef{Name: pod.Name, UID: pod.UID}
		if !refs[ref] {
			continue // Partial 不要求同组其他成员同时进入。
		}
		owner, belongs := group.GroupForPod(pod)
		// 排队后成员可能重建、改归属、绑定、结束或删除；旧 Unit 不能直接继续执行。
		if found[ref] || !belongs || owner != key || member.Group != key || pod.Namespace != key.Namespace || pod.DeletionTimestamp != nil || pod.Spec.NodeName != "" || (pod.Status.Phase != "" && pod.Status.Phase != corev1.PodPending) {
			return fmt.Errorf("Accept 成员 %s 的当前身份或状态不允许执行", ref.Name)
		}
		found[ref] = true
	}
	if len(found) != len(refs) {
		return fmt.Errorf("Accept 成员已缺失或被同名新 Pod 替换")
	}
	return nil
}

func (b *Buffer) ContainsPod(key group.Key, uid types.UID) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, exists := b.owners[key][uid]
	return exists
}
