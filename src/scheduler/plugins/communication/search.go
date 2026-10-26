package communication

import (
	"context"
	"fmt"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/scheduler/planner"
	corev1 "k8s.io/api/core/v1"
)

// placeLink 把一条链路加入上一轮保留的 current 方案，返回扩展后的候选列表。
//
// 下面的循环从外到内依次是：旧方案 → 靠近等级 → From 候选节点 → To 候选节点。
// 两端 NodeList 已按各自 Pod 的基础分排序，这里沿这个顺序枚举节点对。
// 每个“旧方案＋靠近等级”最多收集 limit 个成功扩展，最后由 keepTrials 统一筛选。
//
// 同节点有可行选择时，仍会继续采样更远的方案，让后续资源整合评价有不同布局可选。
// 期望等级只用于计算是否降级；实际枚举顺序始终是 Node、Rack、Zone、Cluster。
func (p *Plugin) placeLink(ctx context.Context, input scheduler.GroupScoreInput, current []*trial, l link, limit int) ([]*trial, error) {
	var candidates []*trial
	for _, t := range current {
		// 如果某端已在前面的链路中放好，candidates 只返回它已经选中的那个位置。
		// 例如处理完 A—B 再处理 A—C，A 沿用旧位置，C 才继续搜索。
		fromNodes, toNodes := t.candidates(l.From, input), t.candidates(l.To, input)
		for _, level := range proximityLevels {
			// found 统计当前等级成功生成了几套方案，失败尝试不计数。
			found := 0
			// 达到数量后跳出这对 NodeList 的枚举，随后继续下一个靠近等级。
		searchLevel:
			for _, from := range fromNodes {
				for _, to := range toNodes {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					if input.NodeInfos[from.Node] == nil || input.NodeInfos[to.Node] == nil {
						return nil, fmt.Errorf("链路 %s 的候选节点缺少 NodeInfo", l.Rule.Name)
					}
					// 一对节点只在它实际所属的等级处理一次。
					// 例如相同节点归 Node，不再重复归入 Rack 或 Zone。
					if p.actualLevel(input.NodeInfos, from.Node, to.Node) != level {
						continue
					}
					candidate, err := tryLink(t, l, from, to)
					if err != nil {
						return nil, err
					}
					if candidate == nil {
						continue
					}
					// 两端已经成功加入资源副本，再记这条关系的结果。
					// 例：期望 Node(0)，实际 Zone(2)，降级步数就是 2；比期望更近记为 0。
					depth := max(0, levelRank(level)-levelRank(l.Rule.PreferredLevel))
					candidate.degradation = append(candidate.degradation, depth)
					candidate.plan.Communication = append(candidate.plan.Communication, planner.CommunicationResult{Rule: l.Rule, ActualLevel: level})
					// 靠近分是 3-rank：Node=3、Rack=2、Zone=1、Cluster=0。
					// 当前关系乘自己的 Weight，再加到整套方案的通信分中。
					candidate.plan.Scores.Communication.Value += int64(3-levelRank(level)) * int64(l.Rule.Weight)
					candidate.plan.Scores.Communication.Message = "各关系权重 × 靠近分之和：Node=3、Rack=2、Zone=1、Cluster=0"
					candidate.plan.Notes = append(candidate.plan.Notes, fmt.Sprintf("链路 %s：期望 %s，实际 %s，降级 %d 级", l.Rule.Name, l.Rule.PreferredLevel, level, depth))
					candidates = append(candidates, candidate)
					found++
					if found == limit {
						break searchLevel
					}
				}
			}
		}
	}
	// 当前链路的所有成功扩展放在一起，选出供下一条链路继续使用的少量方案。
	return keepTrials(candidates, limit), nil
}

// tryLink 复制一套旧方案，然后先放 From，再在同一份资源账上放 To。
// 两端都放下才返回副本；某端放不下返回 nil,nil，调用者继续试其他节点对。
// error 单独表示无法完成这次计算，例如传来的 NodeScore 找不到对应 NodeInfo。
func tryLink(t *trial, l link, from, to scheduler.NodeScore) (*trial, error) {
	candidate := t.clone()
	for _, endpoint := range []struct {
		pod  *corev1.Pod
		node scheduler.NodeScore
	}{{l.From, from}, {l.To, to}} {
		ok, err := candidate.place(endpoint.pod, endpoint.node)
		if err != nil || !ok {
			return nil, err
		}
	}
	return candidate, nil
}

// completeMember 逐套补齐一个成员：已有位置的方案直接保留，没有位置的沿 NodeList 扩展。
// Place 会对本轮所有 Pod 调用它，所以无链路的成员最终也会进入完整方案。
// 每套旧方案最多增加 limit 个成功选择，随后仍由 keepTrials 统一保留少量方案。
func completeMember(ctx context.Context, input scheduler.GroupScoreInput, current []*trial, pod *corev1.Pod, limit int) ([]*trial, error) {
	var candidates []*trial
	for _, t := range current {
		if _, exists := t.plan.Placements[podRef(pod)]; exists {
			candidates = append(candidates, t)
			continue
		}
		found := 0
		for _, node := range t.candidates(pod, input) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// 每个节点选择从同一套旧方案各复制一份，形成互相独立的备选。
			candidate := t.clone()
			ok, err := candidate.place(pod, node)
			if err != nil {
				return nil, err
			}
			if ok {
				candidates = append(candidates, candidate)
				found++
				if found == limit {
					break
				}
			}
		}
	}
	return keepTrials(candidates, limit), nil
}
