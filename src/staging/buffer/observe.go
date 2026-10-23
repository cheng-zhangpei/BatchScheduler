package buffer

import (
	"context"
	"fmt"
	"reflect"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	errorengine "batchscheduler/src/staging/error_engine"
	"batchscheduler/src/workqueue"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Observe 收到“这个组变了”的通知后，重读缓存、更新已有 Record，再通知评估。
// 没有 Record 也通知评估，因为可能有新成员需要入队；但不会额外长期保存这个组。
// 读取 Pod 后，把支持的调度异常匹配成内部错误；这里不执行绑定、恢复或删除。
func (b *Buffer) Observe(ctx context.Context, observation staging.Observation) error {
	return b.observe(ctx, observation, true)
}

// notify=false 用于评估或 Apply 前再读一次缓存，读完不重复通知评估线程。
// 否则评估线程每读一次，就给自己再排一次任务，会一直空转。
func (b *Buffer) observe(ctx context.Context, observation staging.Observation, notify bool) error {
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
	// 这把锁覆盖“读取缓存 → 写回观察”的整个过程。
	// 否则两次读取可能倒着写回：后读的新状态先写，先读的旧状态后写。
	defer b.observeMu.Unlock()
	// 事件里可能带着旧 Pod；这里只用组编号，实际信息重新向 Store 查询。
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
		// 每个 Unit 只保存自己成员的观察，不复制整个组的所有 Pod。
		record := b.records[unitKey]
		facts := &staging.ObservedGroup{Exists: exists, Members: make(map[workqueue.PodRef]staging.ObservedPod, len(record.Members))}
		if exists {
			facts.Deleting = view.Group.DeletionTimestamp != nil
			facts.Generation = view.Group.Generation
		}
		for ref := range record.Members {
			fact := staging.ObservedPod{Known: exists}
			if member, ok := current[ref]; ok {
				pod := member.Pod
				fact.Present, fact.Deleting, fact.NodeName = true, pod.DeletionTimestamp != nil, pod.Spec.NodeName
				fact.Status = *pod.Status.DeepCopy()
				// 原生错误在这里转为内部错误，Evaluate 以后只读 Failure，不再解析 Reason。
				fact.Failure = errorengine.MapPodSchedulingError(pod)
			}
			facts.Members[ref] = fact
		}
		// 只替换 Observed：旧观察可以丢弃，不保留每次变化的历史。
		// Members 里的执行进度和 owners 里的处理关系都不在这里改。
		if !reflect.DeepEqual(record.Observed, facts) {
			record.Observed = facts
			record.Revision++
		}
	}
	// 放的是 Group Key，意思是“稍后检查这个组”；不是把 Pod/Unit 再入队一次。
	if notify {
		b.evaluations.Add(key)
	}
	return nil
}
