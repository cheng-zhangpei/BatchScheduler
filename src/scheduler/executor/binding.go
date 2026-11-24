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
		// TODO：Atomic 的已绑定成员回滚、Partial 的失败成员单独回送，在 Recover 中实现。
		// 目前已有成员绑定成功时，Recover 仍报告阻塞，不假装已完成回滚。
		return e.handleExecutionError(ctx, a, err)
	}
	a.Binding, a.Reserved = false, false
	return e.report(ctx, a, staging.FeedbackFinished, staging.PhaseBound, "", true)
}
