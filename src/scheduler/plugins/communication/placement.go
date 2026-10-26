// Package communication 实现基于通信关系的有限候选组规划。
package communication

import (
	"context"
	"fmt"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/scheduler"
	"batchscheduler/src/scheduler/planner"
	corev1 "k8s.io/api/core/v1"
)

// Plugin 用固定的候选数量和拓扑标签来组织本轮通信放置。
type Plugin struct {
	MaxPlans  int    // 每处理完一条关系或一个剩余成员，最多保留多少套方案；默认 4。
	RackLabel string // 自定义机架标签，默认 scheduling.batchscheduler.io/rack。
	ZoneLabel string // 默认使用 Kubernetes 的 zone 标签。
}

var _ planner.LinkPlacementPlugin = (*Plugin)(nil)

func New() *Plugin {
	return &Plugin{MaxPlans: 4, RackLabel: scheduling.GroupName + "/rack", ZoneLabel: corev1.LabelTopologyZone}
}

// Place 把本轮 Pod 的候选节点列表组合成若干套完整的 Pod → Node 方案。
//
// 输入分为三部分：Pods 提供成员，PodScores 提供按基础分排好的节点列表，
// NodeInfos 提供本轮节点容量和已有占用；Placement.Communication 提供用户的链路声明。
//
// 主流程只有三段：
//  1. resolveLinks 把声明中的成员 ID 换成两个实际 Pod。
//  2. 从一套空方案出发，逐条 placeLink，让已有方案增加这条关系的两个端点。
//  3. 用 completeMember 补上未参与链路的成员，再取出完整 CandidatePlan。
//
// 每条链路都接着上一轮保留的 trials 继续放，所以资源是整套方案累计计算的。
// 当前 resolveLinks 沿用 YAML 声明顺序，所以下面的链路循环也按这个顺序执行。
// 没有通信声明时，直接通过第三段完成本轮成员的普通放置。
func (p *Plugin) Place(ctx context.Context, input scheduler.GroupScoreInput) ([]planner.CandidatePlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(input.Pods) == 0 {
		return nil, fmt.Errorf("链路放置缺少本轮成员")
	}
	// 第一步：读取用户声明，得到 link{Rule, From Pod, To Pod} 列表。
	var rules []scheduling.CommunicationLink
	if input.Placement != nil {
		rules = input.Placement.Communication
	}
	links, err := resolveLinks(input.Pods, rules)
	if err != nil {
		return nil, err
	}
	limit := p.MaxPlans
	if limit <= 0 {
		limit = 4
	}

	// 第二步：先建一套空方案。处理一条链路后，用返回的新方案列表替换旧列表，
	// 下一条链路就在这些已选位置和已占用资源之上继续展开。
	trials := []*trial{newTrial(input)}
	for _, l := range links {
		trials, err = p.placeLink(ctx, input, trials, l, limit)
		if err != nil {
			return nil, err
		}
		if len(trials) == 0 {
			return nil, fmt.Errorf("有限候选搜索未找到可行组方案，停止在链路 %s", l.Rule.Name)
		}
	}

	// 第三步：逐个确认全部成员都有位置。已经由链路放好的成员直接沿用，
	// 其余成员沿自己的 NodeList 试放；没有链路声明时，所有成员都在这里安排。
	for _, pod := range input.Pods {
		trials, err = completeMember(ctx, input, trials, pod, limit)
		if err != nil {
			return nil, err
		}
		if len(trials) == 0 {
			return nil, fmt.Errorf("有限候选搜索无法放入剩余成员 %s", pod.Name)
		}
	}
	plans := make([]planner.CandidatePlan, 0, len(trials))
	// nodes 只是搜索过程的资源账。交给后续评分的是位置、分数和关系结果，
	// 它们由每个 trial 的 plan 字段统一带出。
	for _, t := range trials {
		plans = append(plans, t.plan)
	}
	return plans, nil
}
