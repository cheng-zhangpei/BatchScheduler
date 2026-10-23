package upstream

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// scoreWithPlugin 完成一个原生插件对整份候选列表的评分。
// NormalizeScore 必须在所有候选都打完后调用，否则相对分数会算错。
func scoreWithPlugin(ctx context.Context, plugin framework.ScorePlugin, state *framework.CycleState, pod *corev1.Pod, nodes []fwk.NodeInfo) (framework.NodeScoreList, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pre, ok := plugin.(framework.PreScorePlugin); ok {
		status := pre.PreScore(ctx, state, pod, nodes)
		if status.IsSkip() {
			return nil, nil // 这个 Pod 不需要该插件，不再调用它的 Score 或 NormalizeScore。
		}
		if !status.IsSuccess() {
			return nil, fmt.Errorf("原生 PreScore %s: %w", plugin.Name(), status.AsError())
		}
	}
	scores := make(framework.NodeScoreList, len(nodes))
	for i, node := range nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		score, status := plugin.Score(ctx, state, pod, node)
		if !status.IsSuccess() {
			return nil, fmt.Errorf("原生 Score %s，节点 %s: %w", plugin.Name(), node.Node().Name, status.AsError())
		}
		scores[i] = framework.NodeScore{Name: node.Node().Name, Score: score}
	}
	if ext := plugin.ScoreExtensions(); ext != nil {
		if status := ext.NormalizeScore(ctx, state, pod, scores); !status.IsSuccess() {
			return nil, fmt.Errorf("原生 NormalizeScore %s: %w", plugin.Name(), status.AsError())
		}
	}
	for _, score := range scores {
		if score.Score < framework.MinNodeScore || score.Score > framework.MaxNodeScore {
			return nil, fmt.Errorf("原生 Score %s 的归一化分数越界: %d", plugin.Name(), score.Score)
		}
	}
	return scores, nil
}
