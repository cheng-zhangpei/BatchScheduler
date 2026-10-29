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
	if a.Binding {
		return ErrBindingResultUnknown
	}
	// 已有成员真正绑定时，不能只退整轮暂占就宣称回滚完成。
	// TODO：分别实现 Atomic 的已绑定成员恢复，以及 Partial 的成功/失败成员交接。
	if len(a.BoundMembers) != 0 {
		return fmt.Errorf("%w: 已有 %d 个成员绑定成功，部分绑定的恢复尚未实现", staging.ErrHandlerUnavailable, len(a.BoundMembers))
	}
	if a.Request.Operation != staging.OperationRun {
		return fmt.Errorf("%w: 旧执行的暂占归属尚未确认，不能直接 Forget", staging.ErrHandlerUnavailable)
	}
	if r.resources == nil {
		return fmt.Errorf("%w: 恢复暂占缺少共享资源账", staging.ErrHandlerUnavailable)
	}
	if err := r.resources.Forget(a.Request.Unit, a.Request.ActionID, a.Request.Members); err != nil {
		return err
	}
	a.Reserved = false
	return nil
}
