package executor

import (
	"context"
	"errors"

	"batchscheduler/src/staging"
)

// schedule 按步骤组织本轮调度，不要求外部拼接执行函数。
func (e *Executor) schedule(ctx context.Context, a *executionAttempt) error {
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhaseFiltering, "", false); err != nil {
		return err
	}
	// Step 1：原生过滤，得到每个成员的候选节点和拒绝原因。
	candidates, err := e.steps.Filter(ctx, &a.Attempt)
	if err != nil {
		return err
	}
	if err := noCandidateFailure(a.Request.Members, candidates); err != nil {
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
	// Step 3：组评分、组内选点。只留入口，具体算法下一步讨论。
	a.Plan, err = e.steps.PlanGroup(ctx, &a.Attempt, scored)
	if err != nil {
		return err
	}
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhasePlanned, "", false); err != nil {
		return err
	}
	// Step 4：预占资源，直接继续，不等 Buffer 再派发 Assume。
	if err := e.steps.Assume(ctx, &a.Attempt); err != nil {
		return err
	}
	a.Reserved = true
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhaseAssumed, "", false); err != nil {
		return err
	}
	// Step 5：绑定和结清本轮资源，直接继续，不等 Buffer 再派发 Bind。
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhaseBinding, "", false); err != nil {
		return err
	}
	a.Binding = true
	if err := e.steps.Bind(ctx, &a.Attempt); err != nil {
		if errors.Is(err, staging.ErrExecutionNotStarted) {
			a.Binding = false
		}
		return err
	}
	a.Binding, a.Reserved = false, false
	return nil
}
