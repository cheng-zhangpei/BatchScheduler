package resourcecache

import (
	"fmt"

	"batchscheduler/src/workqueue"
)

// ConfirmBoundMembers 接住执行器已经确认成功的成员，不必等 informer 再次通知。
// 节点总账原本就包含这些占用；这里只取消暂占身份，不重新累加资源。
func (c *Cache) ConfirmBoundMembers(unit workqueue.UnitKey, actionID string, members []workqueue.PodRef) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ref := range members {
		if assumption, exists := c.assumed[ref.UID]; exists &&
			(assumption.Unit != unit || assumption.ActionID != actionID || assumption.Pod.Name != ref.Name) {
			return fmt.Errorf("已确认成员 %s 的暂占属于其他执行", ref.Name)
		}
	}
	for _, ref := range members {
		delete(c.assumed, ref.UID)
		if pod := c.pods[ref.UID]; pod != nil && !c.releasedChecks.empty() {
			c.releasedChecks.observeBound(pod)
		}
	}
	return nil
}
