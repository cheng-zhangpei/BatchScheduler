package steps

import (
	"context"
	"fmt"

	"batchscheduler/src/staging"
)

// Bind 提交绑定并确认结果，返回 nil 才表示绑定和本轮资源交接都完成。
func (r *Runner) Bind(context.Context, *Attempt) error {
	// TODO：提交并确认绑定。已确认拒绝时清 Attempt.Binding；未知结果时保留标记。
	return fmt.Errorf("%w: %w: Bind 尚未实现", staging.ErrExecutionNotStarted, staging.ErrHandlerUnavailable)
}
