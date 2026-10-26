package communication

import (
	"fmt"
	"maps"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/scheduler/planner"
	"batchscheduler/src/scheduler/upstream"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// trial 保存一套正在构造的组方案及其资源账，只在本次 Place 中使用。
// 一份 Unit 可以尝试多套 trial；plan 记录位置和评价，nodes 记录放完后的累计占用。
// 每增加一条链路都从这份已累计的结果继续放，最终只把 plan 交给后面的评分步骤。
type trial struct {
	plan        planner.CandidatePlan
	nodes       map[string]fwk.NodeInfo
	degradation []int // 按实际处理顺序追加降级步数，betterTrial 从第一项起比较。
}

// newTrial 建一套尚未安排成员的空方案，并复制输入中的节点资源账。
// 副本起点包含集群原有的 Pod 占用，后面只追加本轮试放的成员。
func newTrial(input scheduler.GroupScoreInput) *trial {
	t := &trial{plan: planner.CandidatePlan{Placements: make(map[workqueue.PodRef]string)}}
	t.nodes = snapshotNodes(input.NodeInfos)
	return t
}

// clone 从当前进度分出另一条选择：复制已选位置、当前资源占用、评分说明和降级记录。
// 复制的是 t.nodes 中已经加过成员的资源账，不是重新读取最初的 input.NodeInfos。
func (t *trial) clone() *trial {
	copy := &trial{plan: t.plan, nodes: snapshotNodes(t.nodes)}
	copy.plan.Placements = maps.Clone(t.plan.Placements)
	copy.plan.Notes = append([]string(nil), t.plan.Notes...)
	copy.plan.Communication = append([]planner.CommunicationResult(nil), t.plan.Communication...)
	copy.degradation = append([]int(nil), t.degradation...)
	return copy
}

// snapshotNodes 逐节点调用上游 Snapshot，得到独立的资源统计、端口和 Pod 列表。
// 试放只往这些副本追加 PodInfo；其中已有的 Pod/Node 对象仍按只读方式使用。
func snapshotNodes(nodes map[string]fwk.NodeInfo) map[string]fwk.NodeInfo {
	copy := make(map[string]fwk.NodeInfo, len(nodes))
	for name, node := range nodes {
		copy[name] = node.Snapshot()
	}
	return copy
}

// podRef 取本次实际对象的名称和 UID，与前面 PodScores 使用同一种键。
func podRef(pod *corev1.Pod) workqueue.PodRef {
	return workqueue.PodRef{Name: pod.Name, UID: pod.UID}
}

// place 尝试将一个成员放到指定候选节点，成功时更新本 trial 的位置、资源和 Pod 分数。
// 返回 true,nil 表示这个选择可用，false,nil 表示换个节点继续试。
//
// 更新顺序是：检查已有位置 → 检查累计资源与端口 → AddPodInfo → 记录位置和基础分。
// AddPodInfo 交给上游计算成员的资源占用，所以后一个成员能看到前一个成员的消耗。
func (t *trial) place(pod *corev1.Pod, candidate scheduler.NodeScore) (bool, error) {
	ref := podRef(pod)
	// A—B 与 A—C 共用的 A，只放一次。位置一致就沿用，位置不同则这次组合不成立。
	if assigned, exists := t.plan.Placements[ref]; exists {
		return assigned == candidate.Node, nil
	}
	node := t.nodes[candidate.Node]
	if node == nil {
		return false, fmt.Errorf("候选节点 %s 不在本轮 NodeInfo 中", candidate.Node)
	}
	// node 已经包含本方案前面安排的成员，检查的是“再放一个”后的累计可行性。
	if !upstream.FitsForPlacement(pod, node) {
		return false, nil
	}
	info, err := framework.NewPodInfo(pod)
	if err != nil {
		return false, err
	}
	// 从这一行起，后续成员会在增加后的资源、端口和 Pod 数量上继续计算。
	node.AddPodInfo(info)
	t.plan.Placements[ref] = candidate.Node
	t.plan.Scores.Pod.Value += candidate.Score
	t.plan.Scores.Pod.Message = "所选节点的 Pod 基础分之和，每个成员只计一次"
	return true, nil
}

// candidates 取某个成员这次可以尝试的位置。
// 尚未安排的成员沿用 input.PodScores 的顺序；已经安排的成员只返回原来的节点。
func (t *trial) candidates(pod *corev1.Pod, input scheduler.GroupScoreInput) []scheduler.NodeScore {
	if assigned, exists := t.plan.Placements[podRef(pod)]; exists {
		// 此处不用再带基础分：place 会沿用已计入的分数，不给共享成员重复加分。
		return []scheduler.NodeScore{{Node: assigned}}
	}
	return input.PodScores[podRef(pod)]
}
