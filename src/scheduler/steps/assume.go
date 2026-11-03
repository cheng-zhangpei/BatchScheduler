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
	if len(a.members) == 0 || len(a.members) != len(a.Request.Members) || len(a.Plan) == 0 ||
		(a.Request.Mode != workqueue.AllowPartial && len(a.Plan) != len(a.members)) {
		return fmt.Errorf("Assume 的本轮成员或计划不完整")
	}
	expected := make(map[workqueue.PodRef]bool, len(a.Request.Members))
	for _, ref := range a.Request.Members {
		expected[ref] = true
	}
	// 只给计划中有位置的成员暂占资源。Atomic 的计划须覆盖所有成员。
	batch := make([]resourcecache.Assumption, 0, len(a.Plan))
	assumed := make([]workqueue.PodRef, 0, len(a.Plan))
	for _, pod := range a.members {
		ref := workqueue.PodRef{Name: pod.Name, UID: pod.UID}
		node, placed := a.Plan[ref]
		if !placed {
			continue
		}
		if !expected[ref] || node == "" {
			return fmt.Errorf("Assume 成员 %s 不属于本轮或没有目标节点", pod.Name)
		}
		copy := pod.DeepCopy()
		copy.Spec.NodeName = node
		batch = append(batch, resourcecache.Assumption{Unit: a.Request.Unit, ActionID: a.Request.ActionID, Pod: copy})
		assumed = append(assumed, ref)
	}
	if len(batch) != len(a.Plan) {
		return fmt.Errorf("Assume 计划含有不属于本轮的成员")
	}
	if err := r.resources.Assume(batch); err != nil {
		return err
	}
	// 缓存确认整批记账成功后，才保存实际暂占名单。
	// 计划里有位置不等于已经占到资源；失败时不能用计划冒充暂占结果。
	a.AssumedMembers = assumed
	a.Reserved = true
	return nil
}
