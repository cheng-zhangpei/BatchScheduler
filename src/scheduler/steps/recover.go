package steps

import (
	"context"
	"fmt"

	"batchscheduler/src/staging"
)

// Recover 检查失败轮次留下了什么：没有暂占就直接结束；有可撤销的暂占才 Forget。
// Atomic/Partial 决定下一轮处理哪些成员，不改变“只退实际暂占”的记账规则。
// 本函数不调用 Buffer。返回 nil 表示资源收尾完成，返回 error 表示尚未完成；
// Executor.recoverAttempt 接住返回值，再统一通过 Report 把结果交给 Buffer。
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
	if len(a.AssumedMembers) == 0 {
		return fmt.Errorf("%w: 尚未确认实际暂占成员，不能宣称恢复完成", staging.ErrHandlerUnavailable)
	}
	// 原请求以后可能同时包含“已暂占”和“没放下”的成员，只按成功记账的名单退。
	if err := r.resources.Forget(a.Request.Unit, a.Request.ActionID, a.AssumedMembers); err != nil {
		return err
	}
	a.Reserved = false
	a.AssumedMembers = nil
	return nil
}
