package steps

import (
	"context"
	"errors"
	"fmt"

	"batchscheduler/src/group"
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
	if a.Request.Mode == workqueue.AllowPartial {
		return r.assumePartial(a, batch)
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

// assumePartial 把最新资源账上放不下的成员摘出，绑定计划只保留真正暂占成功者。
// 这里的 Unplaced 只包含 Assume 新发现的失败，Executor 随后合并 Filter/Plan 的未放置成员。
func (r *Runner) assumePartial(a *Attempt, batch []resourcecache.Assumption) error {
	result, err := r.resources.AssumePartial(batch)
	if err != nil {
		return err
	}
	if len(result.Assumed) == 0 {
		// 没有成员可继续 Bind，没有新增占用；整轮交给原来的恢复反馈路径。
		return result.Rejected[0].Err
	}
	plan := make(map[workqueue.PodRef]string, len(result.Assumed))
	assumed := make([]workqueue.PodRef, 0, len(result.Assumed))
	for _, record := range result.Assumed {
		ref := workqueue.PodRef{Name: record.Pod.Name, UID: record.Pod.UID}
		plan[ref] = record.Pod.Spec.NodeName
		assumed = append(assumed, ref)
	}
	a.Plan, a.AssumedMembers, a.Reserved = plan, assumed, true
	if len(result.Rejected) > 0 {
		a.Resources = &staging.ResourceResult{Assumed: assumed}
		for _, rejected := range result.Rejected {
			reason := workqueue.ReasonUnschedulable // 累计资源、Pod 数量或 HostPort 放不下。
			if errors.Is(rejected.Err, resourcecache.ErrAssumeNodeMissing) {
				reason = workqueue.ReasonNodeUnavailable
			}
			pod := rejected.Assumption.Pod
			a.Resources.Unplaced = append(a.Resources.Unplaced, staging.UnplacedMember{
				Pod:     workqueue.PodRef{Name: pod.Name, UID: pod.UID},
				Failure: group.FailureInfo{Reason: reason, Message: rejected.Err.Error()},
			})
		}
	}
	return nil
}
