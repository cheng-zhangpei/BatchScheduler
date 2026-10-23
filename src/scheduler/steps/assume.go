package steps

import (
	"context"
	"fmt"

	"batchscheduler/src/staging"
)

// Assume 把本轮计划的资源占用记入本地资源缓存。
func (r *Runner) Assume(context.Context, *Attempt) error {
	// TODO：实现预留；即使只完成部分，也要及时设 Attempt.Reserved，失败时才能清理。
	return fmt.Errorf("%w: %w: Assume 尚未实现", staging.ErrExecutionNotStarted, staging.ErrHandlerUnavailable)
}
