package upstream

import (
	"context"
	"fmt"
	"sort"

	"batchscheduler/src/scheduler"
	corev1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// PodScore 调用原生评分插件，不加入我们自己的组评分规则。
type PodScore struct {
	nodes   map[string]fwk.NodeInfo
	plugins []weightedScorePlugin
}

var _ scheduler.PodScore = (*PodScore)(nil)

// NewPodScore 复用本轮 Filter 的节点视图，避免重新读缓存后两步使用不同的资源数据。
// Filter 和 Score 都只读这份视图；后续组内试放的资源变化要另行处理。
func NewPodScore(ctx context.Context, filter *PodFilter) (*PodScore, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if filter == nil {
		return nil, fmt.Errorf("Score 缺少本轮 Filter 的节点视图")
	}
	plugins, err := newUpstreamScorePlugins(ctx)
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]fwk.NodeInfo, len(filter.nodes))
	for _, node := range filter.nodes {
		nodes[node.Node().Name] = node
	}
	return &PodScore{nodes: nodes, plugins: plugins}, nil
}

// Score 依次完成每个插件的 PreScore、逐节点 Score、NormalizeScore，再加权汇总。
// 每个 Pod 使用自己的 CycleState；A 的亲和性、资源需求不能被 B 复用。
func (s *PodScore) Score(ctx context.Context, pod *corev1.Pod, candidates []string) ([]scheduler.NodeScore, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pod == nil {
		return nil, fmt.Errorf("原生评分需要 Pod")
	}
	nodes, err := s.candidateNodes(candidates)
	if err != nil {
		return nil, err
	}
	results := make([]scheduler.NodeScore, len(candidates))
	for i, name := range candidates {
		results[i] = scheduler.NodeScore{Node: name, PluginScores: make(map[string]int64)}
	}
	if len(nodes) == 0 {
		return results, nil // 没有候选就没有分数，不凭空加回被 Filter 排除的节点。
	}
	state := framework.NewCycleState()
	for _, weighted := range s.plugins {
		scores, err := scoreWithPlugin(ctx, weighted.plugin, state, pod, nodes)
		if err != nil {
			return nil, err
		}
		for i, score := range scores {
			results[i].PluginScores[weighted.plugin.Name()] = score.Score
			results[i].Score += score.Score * weighted.weight
		}
	}
	// 同分按节点名排序，让输出稳定；这里只排候选，不截断，也不决定绑定。
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score == results[j].Score {
			return results[i].Node < results[j].Node
		}
		return results[i].Score > results[j].Score
	})
	return results, nil
}

// candidateNodes 只取 Filter 交来的节点；缺失或重复说明两步输入接错了，不能静默忽略。
func (s *PodScore) candidateNodes(names []string) ([]fwk.NodeInfo, error) {
	nodes := make([]fwk.NodeInfo, 0, len(names))
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if s.nodes[name] == nil || seen[name] {
			return nil, fmt.Errorf("Score 候选节点不在本轮视图中或重复: %q", name)
		}
		seen[name] = true
		nodes = append(nodes, s.nodes[name])
	}
	return nodes, nil
}
