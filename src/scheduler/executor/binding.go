package executor

import (
	"context"

	"batchscheduler/src/staging"
)

// runBinding 在后台完成这一轮的绑定和结果回报，不再占用串行调度线程。
// 此时 Assume 已经写入共享资源账，后面的调度会看见本轮占用。
func (e *Executor) runBinding(ctx context.Context, a *executionAttempt) error {
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhaseBinding, "", false); err != nil {
		return err
	}
	if err := e.steps.Bind(ctx, &a.Attempt); err != nil {
		return e.handleExecutionError(ctx, a, err)
	}
	a.Binding, a.Reserved = false, false
	return e.report(ctx, a, staging.FeedbackFinished, staging.PhaseBound, "", true)
}
