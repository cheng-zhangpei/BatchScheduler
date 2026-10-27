package steps

import (
	"context"
	"fmt"

	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// Assume 把本轮计划的资源占用记入本地资源缓存。
func (r *Runner) Assume(ctx context.Context, a *Attempt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.resources == nil {
		return fmt.Errorf("%w: Assume 缺少共享资源账", staging.ErrHandlerUnavailable)
	}
	if len(a.members) == 0 || len(a.members) != len(a.Request.Members) || len(a.Plan) != len(a.members) {
		return fmt.Errorf("Assume 的本轮成员或计划不完整")
	}
	expected := make(map[workqueue.PodRef]bool, len(a.Request.Members))
	for _, ref := range a.Request.Members {
		expected[ref] = true
	}
	// 用 Filter 保存的完整成员和最终计划组装暂占记录，不修改原 Pod 对象。
	batch := make([]resourcecache.Assumption, 0, len(a.members))
	for _, pod := range a.members {
		ref := workqueue.PodRef{Name: pod.Name, UID: pod.UID}
		if !expected[ref] || a.Plan[ref] == "" {
			return fmt.Errorf("Assume 成员 %s 不属于本轮或没有目标节点", pod.Name)
		}
		copy := pod.DeepCopy()
		copy.Spec.NodeName = a.Plan[ref]
		batch = append(batch, resourcecache.Assumption{Unit: a.Request.Unit, ActionID: a.Request.ActionID, Pod: copy})
	}
	if err := r.resources.Assume(batch); err != nil {
		return err
	}
	// 批量登记成功才标记已暂占；后续 Report 失败也不能丢掉这份资源责任。
	a.Reserved = true
	return nil
}
