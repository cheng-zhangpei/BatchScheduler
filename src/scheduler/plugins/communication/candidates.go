package communication

import (
	"fmt"
	"sort"
	"strings"
)

// keepTrials 决定下一轮搜索继续带着哪些部分方案，返回数量最多为 limit。
//
// 做法是：先按当前评价排序，去掉完全相同的放置，再各取一种分布形状，
// 还有空位就按原排名补齐。最后将保留结果重新排序，交给下一条链路或剩余成员。
// 这里选择的是继续搜索的备选，最终组选点仍由 Planner 后面的汇总步骤完成。
func keepTrials(candidates []*trial, limit int) []*trial {
	sort.SliceStable(candidates, func(i, j int) bool { return betterTrial(candidates[i], candidates[j]) })
	// 排序后先去重：多个展开过程可能到达同一个 Pod → Node 映射，只留最靠前的一份。
	unique := make([]*trial, 0, len(candidates))
	seen := make(map[string]bool)
	for _, candidate := range candidates {
		key := placementKey(candidate)
		if !seen[key] {
			seen[key] = true
			unique = append(unique, candidate)
		}
	}
	kept := make([]*trial, 0, limit)
	chosen, shapes := make(map[*trial]bool), make(map[string]bool)
	// 接着保留布局差异。例如两个成员都在一台的 [2]，和各占一台的 [1 1]，
	// 各取当前排序中最靠前的一套，让后续集中评分还能比较不同分布。
	for _, candidate := range unique {
		shape := placementShape(candidate)
		if !shapes[shape] && len(kept) < limit {
			kept = append(kept, candidate)
			chosen[candidate], shapes[shape] = true, true
		}
	}
	// 再补空位：同一种形状也可能有不同节点选择，继续按排名保留这些候选。
	for _, candidate := range unique {
		if !chosen[candidate] && len(kept) < limit {
			kept = append(kept, candidate)
		}
	}
	// 最后重排保留结果，让下一轮仍从当前评价较好的方案开始尝试。
	sort.SliceStable(kept, func(i, j int) bool { return betterTrial(kept[i], kept[j]) })
	return kept
}

// betterTrial 先逐项比较降级记录；前面处理的关系先比较，少降级的排在前面。
// 全部降级步数一致时，再比较加权通信分，最后比较 Pod 基础分。
// 因此它使用的是链路实际处理顺序，是否按 Priority 处理取决于上游 resolveLinks。
func betterTrial(a, b *trial) bool {
	for i := range a.degradation {
		if a.degradation[i] != b.degradation[i] {
			return a.degradation[i] < b.degradation[i]
		}
	}
	if a.plan.Scores.Communication.Value != b.plan.Scores.Communication.Value {
		return a.plan.Scores.Communication.Value > b.plan.Scores.Communication.Value
	}
	return a.plan.Scores.Pod.Value > b.plan.Scores.Pod.Value
}

// placementKey 把成员名称、UID、节点名排成固定顺序，用于识别完全相同的放置。
// 排序后再拼接，使 map 的遍历顺序不影响去重。
func placementKey(t *trial) string {
	parts := make([]string, 0, len(t.plan.Placements))
	for ref, node := range t.plan.Placements {
		parts = append(parts, ref.Name+"/"+string(ref.UID)+"="+node)
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x00")
}

// placementShape 只记录各节点分到几个成员：A/B 同节点是 [2]，分开是 [1 1]。
// 三个成员分在两台节点上、数量分别为 2 和 1 时，排序后统一表示为 [1 2]。
// 形状不含节点名称，用来为不同的集中程度保留一个代表。
func placementShape(t *trial) string {
	counts := make(map[string]int)
	for _, node := range t.plan.Placements {
		counts[node]++
	}
	shape := make([]int, 0, len(counts))
	for _, count := range counts {
		shape = append(shape, count)
	}
	sort.Ints(shape)
	return fmt.Sprint(shape)
}
