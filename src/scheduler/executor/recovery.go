package executor

import (
	"context"
	"errors"
	"time"

	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/scheduler/steps"
	"batchscheduler/src/staging"
)

// recoverAttempt 处理整轮失败后的收尾，分三步：
// 1. 通知 Buffer 正在恢复，保留 Pending。
// 2. Recover 检查实际占用；未 Assume 时直接结束，有可撤销的暂占才 Forget。
// 3. 把恢复结果交回 Buffer。这里不创建新 Unit，也不调用 Select 或 Transition。
// Partial 中仍有成员要继续绑定的情况，不应把整轮交给这个结束流程。
func (e *Executor) recoverAttempt(ctx context.Context, a *executionAttempt) error {
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhaseRecovering, "", false); err != nil {
		return err
	}
	recoveryErr := e.steps.Recover(ctx, &a.Attempt)
	return e.reportRecoveryResult(ctx, a, recoveryErr)
}

// reportRecoveryResult 是恢复后的反馈出口，最终通过 e.Report 调用 Buffer.Report。
// recoveryErr 说明“恢复有没有完成”；a.failure 仍保存“原来为什么调度失败”。
// 恢复成功不代表调度成功：Buffer 收到原始 Failure 后，后续评估才决定回送哪些成员。
func (e *Executor) reportRecoveryResult(ctx context.Context, a *executionAttempt, recoveryErr error) error {
	switch {
	case recoveryErr == nil:
		// 没有占用要撤销，或 Forget 已成功：本次执行可以结束。
		// Finished/Recovered 让 Buffer 取消 Pending，并记录恢复完成；原始 Failure 一起带回。
		a.Reserved, a.Binding = false, false
		return e.report(ctx, a, staging.FeedbackFinished, staging.PhaseRecovered, "", true)
	case errors.Is(recoveryErr, steps.ErrBindingResultUnknown):
		// 已收到成功响应的成员先转为真实占用；到期只释放其余暂占。
		if err := e.resources.ConfirmBoundMembers(a.Request.Unit, a.Request.ActionID, a.BoundMembers); err != nil {
			return err
		}
		if err := e.report(ctx, a, staging.FeedbackUnknown, staging.PhaseRecovering, recoveryErr.Error(), false); err != nil {
			return err
		}
		if err := e.resources.TrackBindingChecks(a.Request.Unit, a.Request.ActionID, a.Plan, a.BindingDeadline); err != nil {
			return err
		}
		if !time.Now().Before(a.BindingDeadline) {
			// 预算已用完就立即退账，不再增加一轮 GET 或等待后台计时。
			checks := make([]resourcecache.BindingCheck, 0, len(a.Plan))
			for ref, node := range a.Plan {
				checks = append(checks, resourcecache.BindingCheck{Key: resourcecache.BindingCheckKey{
					Unit: a.Request.Unit, ActionID: a.Request.ActionID, Pod: ref}, NodeName: node, Deadline: a.BindingDeadline})
			}
			return e.checkBindingAttempt(ctx, checks)
		}
		return nil // 次数先耗尽时，只等待 informer 确认或原预算到期，不再额外发 GET。
	case errors.Is(recoveryErr, staging.ErrHandlerUnavailable):
		// 例如已有成员绑定，但对应恢复尚未实现。报告阻塞，保留 Record 和资源责任。
		return e.report(ctx, a, staging.FeedbackBlocked, staging.PhaseRecovering, recoveryErr.Error(), false)
	default:
		// Forget 等步骤出错，也不能宣称清理完成。原始失败不被这个恢复错误覆盖。
		return e.report(ctx, a, staging.FeedbackUnknown, staging.PhaseRecovering, recoveryErr.Error(), false)
	}
}
