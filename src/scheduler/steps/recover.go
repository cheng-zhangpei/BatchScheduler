package steps

import (
	"context"
	"fmt"

	"batchscheduler/src/staging"
)

// Recover 结清失败轮次留下的预留和绑定责任。
func (r *Runner) Recover(_ context.Context, a *Attempt) error {
	// 过滤或选点就失败时，没有预留，也没发绑定，无需撤销资源。
	if !a.Reserved && !a.Binding {
		return nil
	}
	// TODO：实际处理预留与绑定；不能只清标记就宣称资源已释放。
	return fmt.Errorf("%w: 真实资源恢复尚未实现", staging.ErrHandlerUnavailable)
}
