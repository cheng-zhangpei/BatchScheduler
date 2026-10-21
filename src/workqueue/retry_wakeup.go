package workqueue

import (
	"errors"
	"fmt"
	"time"

	"batchscheduler/src/group"
	corev1 "k8s.io/api/core/v1"
)

// RetryWake 告诉一个 Retry 任务“哪种条件变了，以及现在几点”。
// Changed 为空表示只检查时间，不代表资源有改善。来源是 Buffer 的评估决定，不是原始 Pod。
// main 已接节点等事件和定时通知；更多依赖资源的事件还要逐项增加。
type RetryWake struct {
	Changed RetryCondition
	Now     time.Time
}

// ErrRetryNotReady 表示有效提示尚不足以激活，条目继续等待，并非调度失败。
var ErrRetryNotReady = errors.New("重试条件尚未满足")

func (w *WorkQueue) activateRetry(request TransitionRequest) error {
	if request.Unit == nil || request.Wake == nil || request.Wake.Now.IsZero() {
		return fmt.Errorf("Retry → Active 需要 Unit、唤醒提示和当前时间")
	}
	w.selectMu.Lock()
	defer w.selectMu.Unlock()
	key := request.Unit.Key()
	retry, ok := w.Retry.Get(key)
	if !ok {
		return fmt.Errorf("单元 %s 不在 Retry，提示已过期", key.ID)
	}
	unit := retry.Unit
	state := unit.State()
	if request.ExpectedRevision != 0 && state.Revision != request.ExpectedRevision {
		return fmt.Errorf("Retry 迁移决策已过期")
	}
	if state.Phase != UnitQueued || state.Queue != QueueRetry {
		return fmt.Errorf("Retry 条目与单元状态不一致")
	}
	// 变化先到、时间还没到，也先记住 ChangeObserved=true；到期后就不用再等同类变化。
	if retry.Plan.Condition != RetryOnBackoff && retry.Plan.Condition.Matches(request.Wake.Changed) {
		retry.ChangeObserved = true
		w.Retry.Put(retry)
	}
	if request.Wake.Now.Before(retry.Plan.NotBefore) {
		return ErrRetryNotReady
	}
	switch retry.Plan.Condition {
	case RetryOnScheduling, RetryOnResources, RetryOnNodes, RetryOnPlacement, RetryOnDependency:
		if !retry.ChangeObserved {
			return ErrRetryNotReady
		}
	case RetryOnBackoff:
		if retry.Plan.NotBefore.IsZero() {
			return fmt.Errorf("时间型重试缺少截止时间")
		}
	default:
		return fmt.Errorf("未知重试条件: %q", retry.Plan.Condition)
	}
	// 收到事件不代表一定能重试：先确认原来的组和成员还在，且没有其他执行正在处理。
	if w.Views == nil {
		return fmt.Errorf("激活重试缺少组视图读取器")
	}
	view, err := w.Views.Get(key.Group.Namespace, key.Group.Name)
	if err != nil {
		return fmt.Errorf("激活重试读取组视图: %w", err)
	}
	if view == nil || view.Group == nil || view.Group.UID != key.Group.UID || view.Group.DeletionTimestamp != nil {
		return fmt.Errorf("组身份已变化或正在删除")
	}
	record := w.groups[key.Group]
	if record == nil || record.units[key.ID] == nil || record.group.Phase == group.GroupDeleting || w.isStaged(unit) {
		return fmt.Errorf("单元归属无效或仍在缓冲区")
	}
	for _, member := range unit.Members() {
		podState := record.pods[member.UID]
		if podState == nil || podState.UnitID != key.ID || podState.Phase != group.PodUnscheduled {
			return fmt.Errorf("成员归属或内部状态不允许激活")
		}
		found := false
		for _, observed := range view.Members {
			pod := observed.Pod
			if observed.Group == key.Group && pod != nil && pod.Name == member.Name && pod.UID == member.UID {
				found = pod.DeletionTimestamp == nil && pod.Spec.NodeName == "" && (pod.Status.Phase == corev1.PodPending || pod.Status.Phase == "")
				break
			}
		}
		if !found {
			return fmt.Errorf("成员 %s 已变化、结束或已绑定，不能激活旧单元", member.Name)
		}
	}
	// 前面检查都通过，才把同一个 Unit 从 Retry 搬到 Active；成员集合不改变。
	if err := unit.MarkActive(); err != nil {
		return err
	}
	w.Active.Add(unit)
	w.Retry.Remove(key)
	return nil
}
