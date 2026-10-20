package workqueue

import (
	"errors"
	"fmt"
	"time"

	"batchscheduler/src/group"
	corev1 "k8s.io/api/core/v1"
)

// RetryWake 是一个已筛选到目标 Unit 的唤醒提示，不是原始 informer 事件。
// Changed 为空表示只检查时间；事件入口须先判断该变化是否可能帮助此 Unit。
// Now 显式传入，方便逐步接入定时器和确定性测试。
// TODO：接入 informer 事件筛选、候选索引和定时器；这里不扫描所有 Retry。
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
	if state.Phase != UnitQueued || state.Queue != QueueRetry {
		return fmt.Errorf("Retry 条目与单元状态不一致")
	}
	// 即使退避尚未到期，也记住相关事件；时间到期时可以再次使用。
	if retry.Plan.Condition != RetryOnBackoff && request.Wake.Changed == retry.Plan.Condition && request.Wake.Changed != "" {
		retry.ChangeObserved = true
		w.Retry.Put(retry)
	}
	if request.Wake.Now.Before(retry.Plan.NotBefore) {
		return ErrRetryNotReady
	}
	switch retry.Plan.Condition {
	case RetryOnResources, RetryOnNodes, RetryOnPlacement, RetryOnDependency:
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
	// 相关事件只是值得重试的提示；激活前还要重查缓存事实及内部归属。
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
	// =========================================前面是一大堆的过滤，过滤出可以放入active的内容=======================
	if err := unit.MarkActive(); err != nil {
		return err
	}
	w.Active.Add(unit)
	w.Retry.Remove(key)
	return nil
}
