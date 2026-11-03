package planner

import (
	"fmt"
	"maps"
	"slices"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// selectBest 沿用候选生成插件的排序，核对每个成员都有位置或明确的未放置原因。
func selectBest(input scheduler.GroupScoreInput, plans []CandidatePlan) (PlanResult, error) {
	if len(input.Pods) == 0 || len(plans) == 0 {
		return PlanResult{}, fmt.Errorf("无法选定计划：缺少本轮成员或候选方案")
	}
	// 额外评分的比较规则还没定，不能计算出了集中分却在选定时忽略它。
	for _, plan := range plans {
		if plan.Scores.Consolidation != nil {
			return PlanResult{}, fmt.Errorf("%w: 通信与资源整合评分的最终取舍尚未实现", staging.ErrHandlerUnavailable)
		}
	}
	selected := plans[0]
	if len(selected.Placements) == 0 || len(selected.Placements)+len(selected.Unplaced) != len(input.Pods) {
		return PlanResult{}, fmt.Errorf("选中方案的成员数量与本轮不一致")
	}
	if len(selected.Unplaced) != 0 && input.Mode != workqueue.AllowPartial {
		return PlanResult{}, fmt.Errorf("Atomic 需要所有成员都有目标节点")
	}

	expected := make(map[workqueue.PodRef]bool, len(input.Pods))
	for _, pod := range input.Pods {
		if pod == nil {
			return PlanResult{}, fmt.Errorf("选定计划的本轮成员中存在空 Pod")
		}
		expected[workqueue.PodRef{Name: pod.Name, UID: pod.UID}] = true
	}
	for ref, node := range selected.Placements {
		if !expected[ref] || node == "" || input.NodeInfos[node] == nil {
			return PlanResult{}, fmt.Errorf("成员 %s 或目标节点 %q 不在本轮输入中", ref.Name, node)
		}
		delete(expected, ref)
	}
	for _, member := range selected.Unplaced {
		if !expected[member.Pod] || member.Failure.Reason == "" {
			return PlanResult{}, fmt.Errorf("未放置成员 %s 重复、不属于本轮或缺少原因", member.Pod.Name)
		}
		delete(expected, member.Pod)
	}
	return PlanResult{Placements: maps.Clone(selected.Placements), Unplaced: slices.Clone(selected.Unplaced)}, nil
}
