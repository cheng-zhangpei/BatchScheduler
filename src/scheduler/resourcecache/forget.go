package resourcecache

import (
	"fmt"

	"batchscheduler/src/workqueue"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Forget 撤销指定执行的一批成员暂占，只使用缓存中保存的 Pod 作为扣账依据。
// 暂占已不存在时直接跳过，包括已经转为真实绑定的 Pod，不删除它们的实际占用。
// 先在副本上撤销，全部通过后一起写回；执行身份不一致时不撤销本次任何成员。
// 调用方须先确认可以撤销，例如已明确绑定失败；本方法不查询 API Server 判断绑定结果。
func (c *Cache) Forget(unit workqueue.UnitKey, actionID string, members []workqueue.PodRef) error {
	if unit.ID == "" || actionID == "" {
		return fmt.Errorf("Forget 缺少 Unit 或执行编号")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// 第一段：查暂占明细，核对归属，再从对应节点副本中移除 Pod。
	nodes := make(map[string]*framework.NodeInfo)
	pending := make([]Assumption, 0, len(members))
	seen := make(map[types.UID]bool)
	for _, ref := range members {
		if ref.Name == "" || ref.UID == "" {
			return fmt.Errorf("Forget 成员缺少名称或 UID")
		}
		record, exists := c.assumed[ref.UID]
		if !exists {
			continue
		}
		if record.Unit != unit || record.ActionID != actionID || record.Pod.Name != ref.Name {
			return fmt.Errorf("Pod %s 的暂占不属于这次执行，不能 Forget", ref.Name)
		}
		if seen[ref.UID] {
			continue
		}
		name := record.Pod.Spec.NodeName
		if nodes[name] == nil {
			current := c.nodes[name]
			if current == nil {
				return fmt.Errorf("Forget 找不到暂占 Pod %s 所在节点 %s 的资源账", ref.Name, name)
			}
			nodes[name] = current.SnapshotConcrete()
		}
		// 这里将pod移出去
		if err := nodes[name].RemovePod(klog.Background(), record.Pod); err != nil {
			return fmt.Errorf("Forget 撤销 Pod %s 的暂占: %w", ref.Name, err)
		}
		seen[ref.UID] = true
		pending = append(pending, record)
	}

	// 第二段：一起提交节点余额，并删除本次实际撤销的 Pod 索引和暂占记录。
	for name, node := range nodes {
		if node.Node() == nil && len(node.Pods) == 0 {
			delete(c.nodes, name)
		} else {
			c.nodes[name] = node
		}
	}
	for _, record := range pending {
		delete(c.pods, record.Pod.UID)
		delete(c.assumed, record.Pod.UID)
	}
	return nil
}
