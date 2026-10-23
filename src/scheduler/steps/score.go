package steps

import (
	"context"
	"fmt"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/scheduler/upstream"
	"batchscheduler/src/workqueue"
)

// Score 为本轮每个 Pod 生成候选节点评分列表，连同 Mode 一起交给后面的组规划。
// 两种模式在这一步使用同样的原生评分；组评分如何解释 Mode，下一步再实现。
func (r *Runner) Score(ctx context.Context, a *Attempt, candidates map[workqueue.PodRef]scheduler.PodFilterResult) (scheduler.GroupScoreInput, error) {
	input := scheduler.GroupScoreInput{Mode: a.Request.Mode, PodScores: make(map[workqueue.PodRef][]scheduler.NodeScore)}
	if input.Mode != workqueue.AllTogether && input.Mode != workqueue.AllowPartial {
		return input, fmt.Errorf("Score 缺少有效的 Unit 提交模式: %q", input.Mode)
	}
	scorer, err := upstream.NewPodScore(ctx, a.filter)
	if err != nil {
		return input, err
	}
	if len(a.members) == 0 || len(a.members) != len(a.Request.Members) || len(candidates) != len(a.members) {
		return input, fmt.Errorf("Score 的成员和本轮 Filter 结果不完整")
	}
	for _, pod := range a.members {
		ref := workqueue.PodRef{Name: pod.Name, UID: pod.UID}
		filtered, ok := candidates[ref]
		if !ok {
			return input, fmt.Errorf("成员 %s 缺少 Filter 结果", ref.Name)
		}
		scores, err := scorer.Score(ctx, pod, filtered.Candidates)
		if err != nil {
			return input, fmt.Errorf("评分 Pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		input.PodScores[ref] = scores
	}
	return input, nil
}
