package buffer

import (
	"context"
	"fmt"
	"sync"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	clientqueue "k8s.io/client-go/util/workqueue"
)

// Buffer 保存每个 Unit 的执行记录，接收集群变化，并安排下一步操作。
// records 查执行进度；groups 查这个组有哪些 Unit；owners 查哪个 Unit 正在处理某个 Pod。
// 三个 map 指向同一批任务，不是三份独立任务。新增和删除时必须一起更新。
type Buffer struct {
	mu sync.RWMutex // 保护下面的记录；一段代码改完后，另一段代码才能继续改。
	// 一次只允许一个 Observe 读缓存并写回，避免先读到的旧信息反而后写进去。
	observeMu   sync.Mutex
	views       workqueue.GroupViewReader
	records     map[workqueue.UnitKey]*staging.Record         // Unit 编号 → 执行记录。
	groups      map[group.Key]map[workqueue.UnitKey]struct{}  // 组编号 → Buffer 中的 Unit 编号集合。
	owners      map[group.Key]map[types.UID]workqueue.UnitKey // 组编号 + Pod UID → 正在处理它的 Unit。
	evaluations clientqueue.TypedInterface[group.Key]         // 只放“哪个组需要再评估”，不放 Unit。
	runMu       sync.Mutex
	runStarted  bool
	handlers    staging.Handlers
	nextAction  uint64
	retryHints  map[workqueue.UnitKey]retryHint // 某个等待重试的 Unit 收到过什么变化提示。
}

// NewBuffer 建好 map 和评估通知队列，保存外部函数。它不会自动开始执行任务。
// views 通常是 group.Store，Get 只读本地 informer 缓存，不应发网络请求。
func NewBuffer(views workqueue.GroupViewReader, handlers ...staging.Handlers) *Buffer {
	var effects staging.Handlers
	if len(handlers) > 0 {
		effects = handlers[0]
	}
	return &Buffer{
		retryHints: make(map[workqueue.UnitKey]retryHint),
		handlers:   effects,
		views:      views, records: make(map[workqueue.UnitKey]*staging.Record),
		groups:      make(map[group.Key]map[workqueue.UnitKey]struct{}),
		owners:      make(map[group.Key]map[types.UID]workqueue.UnitKey),
		evaluations: clientqueue.NewTyped[group.Key](),
	}
}

// Accept 给已经出队的 Unit 建执行记录，记下“这些 Pod 现在由它处理”。
// 不计算节点、不合并 Unit。调用者必须先 Pop；当前代码还不能证明它真的出过队。
func (b *Buffer) Accept(ctx context.Context, unit workqueue.Unit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if unit == nil {
		return fmt.Errorf("Accept 缺少 Unit")
	}
	key, mode, members := unit.Key(), unit.Mode(), unit.Members()
	// 需要完整编号才能认出重复任务；成员必须固定，否则 owners 会记错由谁处理。
	if key.ID == "" || key.Group.Namespace == "" || key.Group.Name == "" || key.Group.UID == "" || !unit.Frozen() || len(members) == 0 {
		return fmt.Errorf("Accept 要求完整身份及非空、已冻结的 Unit")
	}
	if mode != workqueue.AllTogether && mode != workqueue.AllowPartial {
		return fmt.Errorf("Accept 收到未知提交模式: %q", mode)
	}
	names, refs := make(map[string]bool), make(map[workqueue.PodRef]bool)
	uids := make(map[types.UID]bool)
	for _, ref := range members {
		// 同一成员不能出现两次，也不能让两个成员重用同一个名称或 UID。
		if ref.Name == "" || ref.UID == "" || names[ref.Name] || uids[ref.UID] {
			return fmt.Errorf("Accept 成员身份不完整或重复")
		}
		names[ref.Name], uids[ref.UID], refs[ref] = true, true, true
	}

	// 如果同一个 Unit 已经接收过，直接返回，不重建进度。
	// 即使它现在已经绑定，也不能因为重复调用而覆盖原来的记录。
	b.mu.RLock()
	exists, err := b.checkDuplicate(key, mode, refs)
	b.mu.RUnlock()
	if exists || err != nil {
		return err
	}
	state := unit.State()
	// Fail/Retry 尚未获准执行；不能绕过 Transition 直接塞入缓冲区。
	if state.Phase != workqueue.UnitQueued || (state.Queue != workqueue.QueueActive && state.Queue != workqueue.QueueFastRetry) {
		// 另一个 Accept 可能刚接收了它并改成 Processing，再查一次确认是否只是重复。
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
	// 先读当前组和 Pod，确认排队期间没有删除、重建或绑定。
	// 这里没有预占资源，检查通过也不保证后面一定能调度。
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
	// 刚才读缓存时，另一个 Accept 也可能接收了同一 Unit，所以在写入前再查一次。
	if exists, err := b.checkDuplicate(key, mode, refs); exists || err != nil {
		return err
	}
	for otherKey := range b.groups[key.Group] {
		other := b.records[otherKey]
		// Atomic 同时只执行一轮。旧 Atomic 没结束时，改成 Partial 也不能另开一轮。
		if mode == workqueue.AllTogether || other.Unit.Mode() == workqueue.AllTogether {
			return fmt.Errorf("Atomic 组仍有未释放单元 %s，不能接收另一轮", otherKey.ID)
		}
		for ref := range other.Members {
			// 同名新 Pod 先等旧 Pod 的这轮处理结束，避免旧结果影响新任务。
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

	// 前面只检查。从这里开始一起登记进度、组内编号和 Pod 处理关系。
	record := &staging.Record{Unit: unit, Members: make(map[workqueue.PodRef]group.PodState, len(members)), Revision: 1, Generation: view.Group.Generation}
	// 把 Unit 标为正在处理；它是否真的从队列取出，仍由调用者保证。
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
	// 不等下一次 informer 事件，主动通知评估线程检查这个新任务。
	b.evaluations.Add(key.Group)
	return nil
}

// checkDuplicate 检查这个 Unit 编号是否已经登记过，调用者先保护好 records 再调用。
// 已登记且内容相同就算重复；编号相同但模式/成员不同则报错，不能覆盖旧记录。
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

// validateAdmission 检查“这个 Unit 排队之后，原来的组和成员还允许继续执行吗”。
func validateAdmission(view *group.View, key group.Key, mode workqueue.SubmitMode, refs map[workqueue.PodRef]bool) error {
	// 名称相同但 UID 换了就是另一个组；正在删除的组也不能接新任务。
	if view == nil || view.Group == nil || view.Group.Namespace != key.Namespace || view.Group.Name != key.Name || view.Group.UID != key.UID || view.Group.DeletionTimestamp != nil {
		return fmt.Errorf("Accept 组身份已变化或组正在删除")
	}
	declared := workqueue.SubmitMode(view.Group.Spec.SubmitMode)
	if declared == "" {
		declared = workqueue.AllTogether
	}
	// 组要求 Atomic，Unit 就不能自己改成 Partial。
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

// ContainsPod 只查 owners：这个 Pod 是否正由 Buffer 中的某个 Unit 处理。
func (b *Buffer) ContainsPod(key group.Key, uid types.UID) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, exists := b.owners[key][uid]
	return exists
}
