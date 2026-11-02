package buffer

import (
	"fmt"

	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// validateResourceResult 检查两份名单确实划分了本轮成员，再允许 Report 保存。
// 例如 A 已暂占、B 未放置：不能漏掉 B，也不能把 A 同时记到两份名单里。
func validateResourceResult(r *staging.Record, result *staging.ResourceResult) error {
	if result == nil {
		return nil
	}
	if len(result.Assumed)+len(result.Unplaced) != len(r.Members) {
		return fmt.Errorf("资源结果必须覆盖本轮全部成员")
	}
	if r.Unit.Mode() == workqueue.AllTogether && len(result.Assumed) != 0 && len(result.Unplaced) != 0 {
		return fmt.Errorf("Atomic 不能报告部分接纳")
	}
	seen := make(map[workqueue.PodRef]bool, len(r.Members))
	for _, ref := range result.Assumed {
		if _, exists := r.Members[ref]; !exists || seen[ref] {
			return fmt.Errorf("已暂占成员不属于本轮或重复: %s", ref.Name)
		}
		seen[ref] = true
	}
	for _, member := range result.Unplaced {
		if _, exists := r.Members[member.Pod]; !exists || seen[member.Pod] {
			return fmt.Errorf("未放置成员不属于本轮或重复: %s", member.Pod.Name)
		}
		if member.Failure.Reason == "" {
			return fmt.Errorf("未放置成员 %s 缺少内部错误分类", member.Pod.Name)
		}
		seen[member.Pod] = true
	}
	return nil
}
