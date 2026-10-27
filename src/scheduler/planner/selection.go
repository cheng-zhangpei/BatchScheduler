package planner

import (
	"fmt"
	"maps"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// selectBest 沿用候选生成插件的排序，取第一套完整方案，不重新计算通信分。
// 这里只核对成员齐全、节点存在；累计资源和端口检查由前面的 Place 负责。
// 返回位置表的副本，Executor 后续保存或调整计划时不会改到插件留下的候选。
func selectBest(input scheduler.GroupScoreInput, plans []CandidatePlan) (map[workqueue.PodRef]string, error) {
	if len(input.Pods) == 0 || len(plans) == 0 {
		return nil, fmt.Errorf("无法选定计划：缺少本轮成员或候选方案")
	}
	// 额外评分的比较规则还没定，不能计算出了集中分却在选定时忽略它。
	for _, plan := range plans {
		if plan.Scores.Consolidation != nil {
			return nil, fmt.Errorf("%w: 通信与资源整合评分的最终取舍尚未实现", staging.ErrHandlerUnavailable)
		}
	}
	selected := plans[0].Placements
	if len(selected) != len(input.Pods) {
		return nil, fmt.Errorf("选中方案的成员数量与本轮不一致")
	}
	for _, pod := range input.Pods {
		if pod == nil {
			return nil, fmt.Errorf("选定计划的本轮成员中存在空 Pod")
		}
		ref := workqueue.PodRef{Name: pod.Name, UID: pod.UID}
		node := selected[ref]
		if node == "" || input.NodeInfos[node] == nil {
			return nil, fmt.Errorf("选中方案的成员 %s 缺少位置或节点 %q 不在本轮快照中", pod.Name, node)
		}
	}
	return maps.Clone(selected), nil
}
