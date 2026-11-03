package executor

import (
	"context"

	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// schedule 串行完成选点和暂占。返回成功后，Executor 再把这一轮交给后台绑定。
func (e *Executor) schedule(ctx context.Context, a *executionAttempt) error {
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhaseFiltering, "", false); err != nil {
		return err
	}
	// Step 1：原生过滤，得到每个成员的候选节点和拒绝原因。
	candidates, err := e.steps.Filter(ctx, &a.Attempt)
	if err != nil {
		return err
	}
	allowPartial := a.Request.Mode == workqueue.AllowPartial
	if definition := a.Request.Definition; definition != nil && definition.Spec.Placement != nil && len(definition.Spec.Placement.Communication) > 0 {
		// 有通信链路时，保持整组通过过滤再规划，避免丢掉链路的一端。
		allowPartial = false
	}
	unplaced, err := unplacedAfterFilter(a.Request.Members, candidates, allowPartial)
	if err != nil {
		return err
	}
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhaseFiltered, "", false); err != nil {
		return err
	}
	// Step 2：原生 Pod 评分。每个成员保留完整排序列表，把 Mode 一起往后传。
	scored, err := e.steps.Score(ctx, &a.Attempt, candidates)
	if err != nil {
		return err
	}
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhaseScored, "", false); err != nil {
		return err
	}
	// Step 3：选出本轮的放置计划。Partial 还会给出暂时放不下的成员。
	result, err := e.steps.PlanGroup(ctx, &a.Attempt, scored)
	if err != nil {
		return err
	}
	a.Plan = result.Placements
	unplaced = append(unplaced, result.Unplaced...)
	if len(unplaced) == 0 {
		if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhasePlanned, "", false); err != nil {
			return err
		}
	}
	// Step 4：预占资源，直接继续，不等 Buffer 再派发 Assume。
	if err := e.steps.Assume(ctx, &a.Attempt); err != nil {
		if len(unplaced) > 0 {
			// 部分计划还没有报告。暂占整批失败后，按原成员范围收尾。
			a.Plan = nil
		}
		return err
	}
	if len(unplaced) > 0 {
		// A、B 确实占到资源后，才通知 Buffer 可以交回 C。
		a.Resources = &staging.ResourceResult{Assumed: a.AssumedMembers}
		for _, member := range unplaced {
			a.Resources.Unplaced = append(a.Resources.Unplaced, staging.UnplacedMember{Pod: member.Pod, Failure: member.Failure})
		}
	}
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhaseAssumed, "", false); err != nil {
		return err
	}
	return nil
}
