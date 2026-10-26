package buffer

import (
	"context"
	"fmt"
	"sort"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
)

// GroupContext 同时查询活跃 Unit 和已落盘的成功记录，供跨 Unit 的组规划使用。
// 它只返回副本，不合并 Unit，不转移 Pod 归属，也不把旧位置当成现在的资源占用。
func (b *Buffer) GroupContext(ctx context.Context, key group.Key) (staging.GroupContext, error) {
	result := staging.GroupContext{Group: key}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if b.history == nil {
		return result, fmt.Errorf("组上下文缺少成功记录存储")
	}
	// 先复制活跃记录，再查磁盘。成功记录先落盘才会从活跃区删除，避免两边都查不到。
	b.mu.RLock()
	for unitKey := range b.groups[key] {
		r := b.records[unitKey]
		result.InFlight = append(result.InFlight, staging.InFlightUnit{RecordID: r.ID, Unit: unitKey,
			Mode: r.Unit.Mode(), Phase: r.ExecutionPhase, Members: r.Unit.Members(), Plan: copyPlan(r.Plan)})
	}
	b.mu.RUnlock()
	var err error
	result.Succeeded, err = b.history.List(ctx, key)
	if err != nil {
		return result, err
	}
	archived := make(map[string]bool, len(result.Succeeded))
	for _, record := range result.Succeeded {
		archived[record.ID] = true
	}
	// 归档与查询可能恰好交错；同一份成功记录只呈现一次，以已确认的落盘结果为准。
	active := result.InFlight[:0]
	for _, unit := range result.InFlight {
		if !archived[unit.RecordID] {
			active = append(active, unit)
		}
	}
	result.InFlight = active
	sort.Slice(result.InFlight, func(i, j int) bool { return result.InFlight[i].RecordID < result.InFlight[j].RecordID })
	return result, nil
}
