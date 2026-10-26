package communication

import (
	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	fwk "k8s.io/kube-scheduler/framework"
)

// proximityLevels 同时给出枚举顺序和距离编号：Node=0、Rack=1、Zone=2、Cluster=3。
var proximityLevels = []scheduling.TopologyLevel{
	scheduling.TopologyNode, scheduling.TopologyRack, scheduling.TopologyZone, scheduling.TopologyCluster,
}

// levelRank 把等级换成编号，用于计算实际比期望远了几步，以及 3-rank 的靠近分。
func levelRank(level scheduling.TopologyLevel) int {
	for rank, known := range proximityLevels {
		if known == level {
			return rank
		}
	}
	return len(proximityLevels) - 1
}

// actualLevel 依次比较节点名、机架标签、可用区标签，返回首先匹配的等级。
// 相同节点优先归 Node；不同节点再读 Plugin 配置的 RackLabel 和 ZoneLabel。
// 标签非空且相等才算同域，其余归 Cluster。结果是标签层级，不是网络测速值。
func (p *Plugin) actualLevel(nodes map[string]fwk.NodeInfo, from, to string) scheduling.TopologyLevel {
	if from == to {
		return scheduling.TopologyNode
	}
	left, right := nodes[from].Node().Labels, nodes[to].Node().Labels
	if p.RackLabel != "" && left[p.RackLabel] != "" && left[p.RackLabel] == right[p.RackLabel] {
		return scheduling.TopologyRack
	}
	if p.ZoneLabel != "" && left[p.ZoneLabel] != "" && left[p.ZoneLabel] == right[p.ZoneLabel] {
		return scheduling.TopologyZone
	}
	return scheduling.TopologyCluster
}
