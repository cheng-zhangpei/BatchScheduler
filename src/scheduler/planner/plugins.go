package planner

import (
	"context"

	"batchscheduler/src/scheduler"
)

// LinkPlacementPlugin 接收本轮成员、按分数排序的 NodeList、NodeInfo 和通信声明。
// 实现负责组合出若干套完整 Pod → Node 方案，并带出基础分、通信分和逐条关系结果。
// 候选按实现自身的偏好由好到差排列；未启用额外评分时，Planner 直接沿用这个顺序。
// Planner 随后把这些方案交给集中插件和最终汇总；试放阶段只更新本地副本。
type LinkPlacementPlugin interface {
	Place(context.Context, scheduler.GroupScoreInput) ([]CandidatePlan, error)
}

// ConcentrationPlugin 评价整套方案放完之后的资源分布，例如是否保留了完整空闲节点。
// 只读取输入和候选位置，返回资源整合分；不修改 Placements，也不直接选定最终方案。
type ConcentrationPlugin interface {
	Score(context.Context, scheduler.GroupScoreInput, CandidatePlan) (Score, error)
}
