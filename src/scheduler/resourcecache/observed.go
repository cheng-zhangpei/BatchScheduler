package resourcecache

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// UpdateNode 填入节点容量和属性，保留已经记入的 Pod 占用。
// Pod 初始事件可能先到，不能因为 Node 后到就重建一份空账。
func (c *Cache) UpdateNode(node *corev1.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodeInfo(node.Name).SetNode(node.DeepCopy())
}

// RemoveNode 将节点移出可调度视图；原 Pod 的账等各自删除通知到来再清理。
func (c *Cache) RemoveNode(node *corev1.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	info := c.nodes[node.Name]
	if info == nil || info.Node() == nil || info.Node().UID != node.UID {
		return
	}
	info.RemoveNode()
	if len(info.Pods) == 0 {
		delete(c.nodes, node.Name)
	}
}

// UpdatePod 只把已绑定 Pod 计入资源账，不限制 namespace、调度器或所属组。
// 同一个 UID 再次到来时，先撤销旧记录，再按新对象记入，避免重复累加资源。
func (c *Cache) UpdatePod(pod *corev1.Pod) error {
	if pod.Spec.NodeName == "" {
		return nil
	}
	if pod.UID == "" {
		return fmt.Errorf("已绑定 Pod %s/%s 缺少 UID", pod.Namespace, pod.Name)
	}
	copy := pod.DeepCopy()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.removePod(copy.UID); err != nil {
		return err
	}
	c.nodeInfo(copy.Spec.NodeName).AddPod(copy)
	c.pods[copy.UID] = copy
	// 真实绑定接住同一个 Pod 后，只取消暂占身份，上面已经保留它的实际占用。
	delete(c.assumed, copy.UID)
	return nil
}

// RemovePod 根据 UID 撤销当前记录；重复删除不会再扣一次资源。
func (c *Cache) RemovePod(pod *corev1.Pod) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.removePod(pod.UID); err != nil {
		return err
	}
	delete(c.assumed, pod.UID)
	return nil
}

// nodeInfo 取得节点账；Node 尚未到达时，先建一份只有 Pod 占用的账。
// 调用者已经持有写锁。
func (c *Cache) nodeInfo(name string) *framework.NodeInfo {
	if c.nodes[name] == nil {
		c.nodes[name] = framework.NewNodeInfo()
	}
	return c.nodes[name]
}

// removePod 用保存的旧 Pod 撤销占用，不依赖删除事件里的资源字段。
// 调用者已经持有写锁。
func (c *Cache) removePod(uid types.UID) error {
	previous := c.pods[uid]
	if previous == nil {
		return nil
	}
	info := c.nodes[previous.Spec.NodeName]
	if err := info.RemovePod(klog.Background(), previous); err != nil {
		return err
	}
	delete(c.pods, uid)
	if info.Node() == nil && len(info.Pods) == 0 {
		delete(c.nodes, previous.Spec.NodeName)
	}
	return nil
}
