package resourcecache

import (
	"fmt"
	"time"

	"batchscheduler/src/workqueue"
)

// TrackBindingChecks 在绑定报告 Unknown 后登记本轮实际暂占的成员。
// Plan 不包含 Unplaced；同一次执行重复登记时，不能延长原来的截止时间。
func (c *Cache) TrackBindingChecks(unit workqueue.UnitKey, actionID string, plan map[workqueue.PodRef]string, deadline time.Time) error {
	if unit.ID == "" || actionID == "" || len(plan) == 0 || deadline.IsZero() {
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
			c.bindingChecks[key] = BindingCheck{Key: key, NodeName: node, State: BindingCheckPending, Deadline: deadline}
			if c.bindingCheckPods[ref.UID] == nil {
				c.bindingCheckPods[ref.UID] = make(map[BindingCheckKey]struct{})
			}
			c.bindingCheckPods[ref.UID][key] = struct{}{}
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

// CompleteBindingChecks 在 Buffer 接住最终确认后移除核查记录，不调用 Forget。
func (c *Cache) CompleteBindingChecks(keys []BindingCheckKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		delete(c.bindingChecks, key)
		delete(c.bindingCheckPods[key.Pod.UID], key)
		if len(c.bindingCheckPods[key.Pod.UID]) == 0 {
			delete(c.bindingCheckPods, key.Pod.UID)
		}
	}
}

// ReadyToRelease 仅检查原始截止时间，不要求到期后再查询若干次。
func (check BindingCheck) ReadyToRelease(now time.Time) bool {
	return check.State == BindingCheckPending && !check.Deadline.IsZero() && !now.Before(check.Deadline)
}
