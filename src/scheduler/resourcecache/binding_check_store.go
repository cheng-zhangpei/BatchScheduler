package resourcecache

import (
	"fmt"
	"time"

	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// TrackBindingChecks 在绑定报告 Unknown 后登记本轮实际暂占的成员。
// Plan 不包含 Unplaced；同一次执行重复登记时，沿用原来的时间和次数。
func (c *Cache) TrackBindingChecks(unit workqueue.UnitKey, actionID string, plan map[workqueue.PodRef]string, now time.Time) error {
	if unit.ID == "" || actionID == "" || len(plan) == 0 {
		return fmt.Errorf("绑定核查需要执行身份和实际暂占计划")
	}
	for ref, node := range plan {
		if ref.Name == "" || ref.UID == "" || node == "" {
			return fmt.Errorf("绑定核查成员缺少 Pod 身份或原节点")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for ref, node := range plan {
		key := BindingCheckKey{Unit: unit, ActionID: actionID, Pod: ref}
		if _, exists := c.bindingChecks[key]; !exists {
			c.bindingChecks[key] = BindingCheck{Key: key, NodeName: node, State: BindingCheckPending, TimedOutAt: now}
		}
	}
	return nil
}

// BindingChecks 取出待核查记录的副本；调用方拿到副本后再发 GET，不占着资源锁等网络。
func (c *Cache) BindingChecks() []BindingCheck {
	c.mu.RLock()
	defer c.mu.RUnlock()
	checks := make([]BindingCheck, 0, len(c.bindingChecks))
	for _, check := range c.bindingChecks {
		checks = append(checks, check)
	}
	return checks
}

// ObserveBindingCheck 接受一次成功 GET 的结果。
// 已绑定时更新实际占用；仍未绑定时按最小间隔累计次数。返回 true 表示该成员已在原节点。
func (c *Cache) ObserveBindingCheck(key BindingCheckKey, pod *corev1.Pod, now time.Time, interval time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	check, exists := c.bindingChecks[key]
	if !exists || pod.UID != key.Pod.UID || pod.Name != key.Pod.Name || pod.Namespace != key.Unit.Group.Namespace {
		return false, nil
	}
	current := c.pods[pod.UID]
	if current == nil {
		return false, nil // 删除事件已清账，较早发出的 GET 不能把旧 Pod 加回来。
	}
	assumption, assumed := c.assumed[pod.UID]
	if assumed && (assumption.Unit != key.Unit || assumption.ActionID != key.ActionID) {
		return false, nil // 已属于后来的执行，不用旧查询修改它。
	}
	if pod.Spec.NodeName != "" {
		if assumed {
			if err := c.updatePod(pod.DeepCopy()); err != nil {
				return false, err
			}
		}
		check.LastCheckedAt, check.UnboundChecks = now, 0
		c.bindingChecks[key] = check
		return pod.Spec.NodeName == check.NodeName && c.pods[pod.UID].Spec.NodeName == check.NodeName &&
			pod.DeletionTimestamp == nil && c.pods[pod.UID].DeletionTimestamp == nil, nil
	}
	// 缓存若已确认绑定，较早的“未绑定”查询不再增加次数。
	if !assumed || pod.DeletionTimestamp != nil {
		return false, nil
	}
	previous := check.LastCheckedAt
	if previous.IsZero() {
		previous = check.TimedOutAt
	}
	if !now.Before(previous.Add(interval)) {
		check.LastCheckedAt = now
		check.UnboundChecks++
		c.bindingChecks[key] = check
	}
	return false, nil
}

// CompleteBindingChecks 在 Buffer 接住最终确认后移除核查记录，不调用 Forget。
func (c *Cache) CompleteBindingChecks(keys []BindingCheckKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		delete(c.bindingChecks, key)
	}
}

// ReadyToRelease 只回答是否达到约定次数；实际 Forget 和转入 LRU 留给下一步接线。
func (check BindingCheck) ReadyToRelease(policy BindingCheckPolicy) bool {
	return check.State == BindingCheckPending && policy.RequiredUnboundChecks > 0 && check.UnboundChecks >= policy.RequiredUnboundChecks
}
