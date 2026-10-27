package resourcecache

import (
	corev1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

// Snapshot 是一轮调度看到的资源账。
// Nodes 包含真实占用和暂占；BoundPods 只列已确认绑定的 Pod，用来寻找同组运行成员。
type Snapshot struct {
	Nodes     map[string]fwk.NodeInfo
	BoundPods []*corev1.Pod
}

// Snapshot 一次取出节点资源和已绑定成员，避免两次读取夹入新的资源变更。
func (c *Cache) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	view := Snapshot{Nodes: c.snapshotNodes()}
	for uid, pod := range c.pods {
		if _, assumed := c.assumed[uid]; !assumed && view.Nodes[pod.Spec.NodeName] != nil {
			view.BoundPods = append(view.BoundPods, pod.DeepCopy())
		}
	}
	return view
}

// SnapshotNodeInfos 返回资源统计和 Pod 列表的副本，查看时不会改动共享总账。
// 还没收到 Node 的占位账不作为可调度节点；其中的 Pod 占用继续保留。
// 与上游 Snapshot 一样，副本中的 Pod/Node 对象仍共享，只能读取。
func (c *Cache) SnapshotNodeInfos() map[string]fwk.NodeInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshotNodes()
}

// snapshotNodes 复制节点资源账，由已持有读锁的两个快照入口共用。
func (c *Cache) snapshotNodes() map[string]fwk.NodeInfo {
	nodes := make(map[string]fwk.NodeInfo, len(c.nodes))
	for name, info := range c.nodes {
		if info.Node() != nil {
			nodes[name] = info.Snapshot()
		}
	}
	return nodes
}
