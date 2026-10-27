package executor

import (
	"context"
	"fmt"

	"batchscheduler/src/staging"
)

// blockUntilImplemented 停在待实现步骤。若本轮已经暂占而绑定还没开始，先退账再等待开发。
// 这不是业务调度失败，不回 Fail 重试，也不能一边等待开发一边长期占住节点资源。
func (e *Executor) blockUntilImplemented(ctx context.Context, a *executionAttempt, cause error) error {
	if !a.Reserved {
		return e.report(ctx, a, staging.FeedbackBlocked, a.phase, cause.Error(), true)
	}
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhaseRecovering, "", false); err != nil {
		return err
	}
	if err := e.steps.Recover(ctx, &a.Attempt); err != nil {
		return e.report(ctx, a, staging.FeedbackBlocked, staging.PhaseRecovering,
			fmt.Sprintf("%v；暂占清理未完成: %v", cause, err), false)
	}
	return e.report(ctx, a, staging.FeedbackBlocked, staging.PhaseRecovered, cause.Error(), true)
}
