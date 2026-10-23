package buffer

import (
	"context"
	"fmt"
	"sort"
	"time"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	errorengine "batchscheduler/src/staging/error_engine"
	"batchscheduler/src/staging/evaluation"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Evaluate 检查一个组的三类内容：Buffer 中正在处理的 Unit、Fail/Retry 中等待的 Unit、
// 以及还没绑定且不在 Buffer 中的 Pod。把需要做的事作为 Decision 列表返回。
// 这里只做决定，不修改 Buffer，不搬队列、不合并 Unit。阅读说明见 docs/buffer.md。
// Observe 已匹配三种标准 Pod 调度原因；其他原生原因、API 请求错误、真实 Execute 待补。
func (b *Buffer) Evaluate(ctx context.Context, key group.Key) ([]staging.Decision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if key.Namespace == "" || key.Name == "" || key.UID == "" || b.views == nil {
		return nil, fmt.Errorf("Evaluate 缺少组身份或缓存读取器")
	}
	view, err := b.views.Get(key.Namespace, key.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	exists := err == nil && view != nil && view.Group != nil && (group.Key{Namespace: view.Group.Namespace, Name: view.Group.Name, UID: view.Group.UID}) == key
	// 查询 Fail/Retry 时不要先占住 Buffer 的锁：WorkQueue 读到一半可能要调用 ContainsPod，
	// 而 ContainsPod 也要读 Buffer。如果两边各等对方放开，就都走不下去了。
	// 这里只读取，不出队。
	var queued []workqueue.QueuedUnit
	if b.handlers.QueueSnapshot != nil {
		queued = b.handlers.QueueSnapshot(key)
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	keys := make([]workqueue.UnitKey, 0, len(b.groups[key]))
	// 收集这个组已经进入 Buffer 的 Unit 编号。
	for unitKey := range b.groups[key] {
		keys = append(keys, unitKey)
	}
	// 按编号排列，让每次评估输出顺序固定；这不是调度优先级。
	sort.Slice(keys, func(i, j int) bool { return keys[i].ID < keys[j].ID })
	decisions := make([]staging.Decision, 0, len(keys)+1)
	// 1. 逐个检查 Buffer 中的 Unit，决定继续执行、等待、恢复还是结束本轮。
	for _, unitKey := range keys {
		record := b.records[unitKey]
		// 组没了或正在删除，这个 Unit 就不能继续按原计划执行。
		invalidGroup := !exists || view.Group.DeletionTimestamp != nil
		if exists {
			// 当前组没填写提交模式时，默认要求整组一起提交。
			mode := workqueue.SubmitMode(view.Group.Spec.SubmitMode)
			if mode == "" {
				mode = workqueue.AllTogether
			}
			// 组的模式或声明版本变了，原计划也要作废。
			invalidGroup = invalidGroup || mode != record.Unit.Mode() || view.Group.Generation != record.Generation
		}
		// 根据执行进度和观察结果，为这个 Unit 生成一个决定；这里不执行它。
		decisions = append(decisions, evaluation.EvaluateRecord(unitKey, record, invalidGroup))
	}
	// 2. 检查 Fail、Retry 中的 Unit，决定哪些可以 Fail → Retry 或 Retry → Active。
	if exists && view.Group.DeletionTimestamp == nil {
		decisions = append(decisions, b.evaluateQueuedUnits(queued, view, time.Now())...)
	}
	// 3. 检查组里的 Pod：有没有还没绑定、也没进入 Buffer 的成员需要交给 Select 看看？
	// 它可能已经在队列中，是否需要创建/入队由 Select 判断，避免重复任务。
	if exists && view.Group.DeletionTimestamp == nil {
		for _, member := range view.Members {
			pod := member.Pod
			// 跳过无效成员、已绑定、正在删除，以及已经运行或结束的 Pod。
			if pod == nil || member.Group != key || pod.Spec.NodeName != "" || pod.DeletionTimestamp != nil || (pod.Status.Phase != "" && pod.Status.Phase != corev1.PodPending) {
				continue
			}
			if _, held := b.owners[key][pod.UID]; !held {
				// 找到一个就够了：Select 会检查整个组，不必为每个 Pod 发一次相同请求。
				decisions = append(decisions, staging.Decision{Group: key, Action: staging.ActionSelect, Reason: "AssessUnheldMembers"})
				break
			}
		}
	}
	return decisions, nil
}

// evaluateQueuedUnits 逐个检查 Fail/Retry 任务，交给错误引擎决定是否迁移。
// 调用者已经保护好 Buffer 记录，这里只读提示，不修改它们。
func (b *Buffer) evaluateQueuedUnits(entries []workqueue.QueuedUnit, view *group.View, now time.Time) []staging.Decision {
	var decisions []staging.Decision
	for _, entry := range entries {
		key := entry.Unit.Key()
		if b.records[key] != nil || !queuedMembersCurrent(view, entry.Unit) {
			continue
		}
		input := errorengine.Input{Queued: &entry, Now: now}
		if hint, ok := b.retryHints[key]; ok && hint.revision == entry.State.Revision {
			wake := hint.wake
			input.Wake = &wake
		}
		result, ok := errorengine.Decide(input)
		if ok && result.Action == errorengine.Transition {
			decisions = append(decisions, staging.Decision{Group: key.Group, Unit: &key,
				Action: staging.ActionTransition, Reason: result.Reason, Transition: result.Transition})
		}
	}
	return decisions
}

// queuedMembersCurrent 检查排队的成员现在是否仍是原来的 Pod，且还没绑定或结束。
// 防止对同名新 Pod、已经结束或已绑定的 Pod 发出旧 Unit 的重试决定。
func queuedMembersCurrent(view *group.View, unit workqueue.Unit) bool {
	mode := workqueue.SubmitMode(view.Group.Spec.SubmitMode)
	if mode == "" {
		mode = workqueue.AllTogether
	}
	if mode != unit.Mode() {
		return false
	}
	for _, ref := range unit.Members() {
		found := false
		for _, member := range view.Members {
			pod := member.Pod
			if member.Group == unit.Key().Group && pod != nil && pod.Name == ref.Name && pod.UID == ref.UID {
				found = pod.Spec.NodeName == "" && pod.DeletionTimestamp == nil && (pod.Status.Phase == "" || pod.Status.Phase == corev1.PodPending)
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
