package staging

import (
	"context"
	"fmt"
	"reflect"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Observe 是独立于 Accept 的观察入口；只更新事实，不启动绑定、释放或回送。
// 未被 Accept 持有的组也可以观察，不为它凭空创建 Unit 或成员执行归属。
// 当前 main 在 informer 回调直接调用 Observe，独立 worker 消费提示后执行 Evaluate。
// 无本地 Unit 时，最新资源仍由 Store 保持，后续评估可据此组织任务。
// 不保存无人持有的组快照，避免短任务流量让暂存区变成第二份无限增长的缓存。
func (b *Buffer) Observe(ctx context.Context, observation Observation) error {
	return b.observe(ctx, observation, true)
}

// notify=false 仅供评估 worker 刷新事实，不重复通知自身。
func (b *Buffer) observe(ctx context.Context, observation Observation, notify bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := observation.Group
	if key.Namespace == "" || key.Name == "" || key.UID == "" {
		return fmt.Errorf("Observe 缺少完整组身份")
	}
	if b.views == nil {
		return fmt.Errorf("Observe 缺少组视图读取器")
	}
	b.observeMu.Lock()
	defer b.observeMu.Unlock()
	// 事件只触发重查，不使用事件中的旧 Pod/旧 View 回写本轮事实。
	view, err := b.views.Get(key.Namespace, key.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("Observe 读取组视图: %w", err)
	}
	if err != nil {
		view = nil // NotFound 是资源变化，不是“调度失败”。
	}
	exists := view != nil && view.Group != nil && (group.Key{Namespace: view.Group.Namespace, Name: view.Group.Name, UID: view.Group.UID}) == key
	current := make(map[workqueue.PodRef]group.PodView)
	if exists {
		for _, member := range view.Members {
			if member.Pod == nil || member.Group != key {
				return fmt.Errorf("Observe 收到无效成员视图")
			}
			current[workqueue.PodRef{Name: member.Pod.Name, UID: member.Pod.UID}] = member
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	for unitKey := range b.groups[key] {
		record := b.records[unitKey]
		facts := &ObservedGroup{Exists: exists, Members: make(map[workqueue.PodRef]ObservedPod, len(record.Members))}
		if exists {
			facts.Deleting = view.Group.DeletionTimestamp != nil
			facts.Generation = view.Group.Generation
		}
		for ref := range record.Members {
			fact := ObservedPod{Known: exists}
			if member, ok := current[ref]; ok {
				pod := member.Pod
				fact.Present, fact.Deleting, fact.NodeName = true, pod.DeletionTimestamp != nil, pod.Spec.NodeName
				fact.Status = *pod.Status.DeepCopy()
			}
			facts.Members[ref] = fact
		}
		// 不更改 Members/owners，不把 Pending、容器崩溃或成员缺失直接映射成
		// 调度错误，也不因组删除而丢失在途责任；后续决策读取这两份状态再判断。
		if !reflect.DeepEqual(record.Observed, facts) {
			record.Observed = facts
			record.Revision++
		}
	}
	// 这里只入队，不在 informer 回调执行评估；同组重复通知会合并。
	// 即使没有本地 Unit 也提交提示，让评估侧处理新组/新成员。
	if notify {
		b.evaluations.Add(key)
	}
	return nil
}
