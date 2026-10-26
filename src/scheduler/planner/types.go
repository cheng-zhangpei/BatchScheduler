package planner

import (
	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/workqueue"
)

// CandidatePlan 把成员位置和评价放在一起，通信插件返回后，后面的插件继续评价同一套方案。
// Placements 保存每个成员选中的节点；Scores 保存三项分数；Communication 保存逐条关系结果。
// 它是本地规划结果，后续选出最终方案后才进入 Assume 和 Bind。
type CandidatePlan struct {
	Placements    map[workqueue.PodRef]string // 本轮每个 Pod → 候选节点名。
	Scores        PlanScores
	Communication []CommunicationResult // 每条关系实际放到了哪个靠近等级，供最终汇总继续权衡。
	Notes         []string              // 解释方案，例如哪条通信关系降级、为什么降级。
}

// CommunicationResult 同时保留用户期望和实际靠近等级，最终汇总可据此比较关系的满足程度。
type CommunicationResult struct {
	Rule        scheduling.CommunicationLink // 保留端点、权重、优先级和期望等级，不靠解析说明字符串取回。
	ActualLevel scheduling.TopologyLevel
}

// PlanScores 分开保留各项评价，最后再汇总，方便解释为什么选中这套方案。
type PlanScores struct {
	Pod           Score  // 当前通信插件把每个成员所选节点的基础分相加。
	Communication Score  // 通信关系在整套方案中的满足程度。
	Consolidation *Score // nil 表示还没有资源整合评价结果；后续集中插件填入。
}

// Score 保存一项评价和说明。分数范围、归一化及最终权重以后逐项确定。
type Score struct {
	Value   int64
	Message string
}
