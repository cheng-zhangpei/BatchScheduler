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
	// 本轮没有提交绑定或已明确未开始绑定时，可以撤销本轮自己的暂占。
	// 已发出的绑定、以及新的 Recover 请求接手旧执行，都还需要单独确认结果与原执行编号。
	if a.Binding || a.Request.Operation != staging.OperationRun {
		return fmt.Errorf("%w: 绑定结果或旧执行的暂占归属尚未确认，不能直接 Forget", staging.ErrHandlerUnavailable)
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
