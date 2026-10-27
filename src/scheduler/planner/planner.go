// Package planner 组织组放置方案的生成、评价和最终选择。
// 所有试放都使用本轮资源副本，选定计划之后才交给后面的 Assume、Bind。
package planner

import (
	"context"
	"fmt"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// Planner 的插件在启动前设置；Plan 内部按固定顺序调用，不需要 Buffer 协调这些环节。
// 集中评价须同时有插件和用户开关；具体插件的算法仍逐项实现。
type Planner struct {
	Placement     LinkPlacementPlugin
	Concentration ConcentrationPlugin
}

// Plan 接收 Pod 评分后的输入，最后返回本轮唯一的 Pod → Node 计划。
func (p *Planner) Plan(ctx context.Context, input scheduler.GroupScoreInput) (map[workqueue.PodRef]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// 环节一：链路放置。插件内部组合链路，返回多套完整候选方案。
	if p.Placement == nil {
		return nil, fmt.Errorf("%w: 链路放置插件未配置", staging.ErrHandlerUnavailable)
	}
	plans, err := p.Placement.Place(ctx, input)
	if err != nil {
		return nil, err
	}

	// 环节二：可选的集中评分。各套方案增加一项资源整合分，位置保持原样。
	if p.Concentration != nil && input.Placement != nil && input.Placement.ResourceConsolidation {
		for i := range plans {
			score, err := p.Concentration.Score(ctx, input, plans[i])
			if err != nil {
				return nil, err
			}
			plans[i].Scores.Consolidation = &score
		}
	}

	// 环节三：自动优化暂留位置。如果以后调整了候选位置，需要重新检查并重算评分。
	// TODO：先明确它与资源整合开关的区别，再决定是否增加这个环节的接口和实现。

	// 环节四：首版沿用通信搜索的排序，选出第一套完整计划。
	return selectBest(input, plans)
}
