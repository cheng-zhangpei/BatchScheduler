package executor

import (
	"context"
	"errors"

	"batchscheduler/src/group"
	"batchscheduler/src/scheduler/steps"
	"batchscheduler/src/staging"
)

// executionAttempt 保存正在执行的一轮。串行调度完成后，由该轮的后台绑定协程接着使用。
type executionAttempt struct {
	steps.Attempt
	phase   staging.ExecutionPhase
	failure *group.FailureInfo
}

// prepareExecution 先完成选点和暂占，成功时交出 Attempt，供后台绑定继续。
// 返回 nil Attempt 表示这一轮没有进入绑定：已经处理失败，或这是单独的恢复请求。
func (e *Executor) prepareExecution(ctx context.Context, request staging.Execution) (*executionAttempt, error) {
	a := &executionAttempt{Attempt: steps.Attempt{Request: request, Plan: request.Plan}, failure: request.Failure}
	if request.Operation == staging.OperationRecover {
		// 这是执行之外发现旧轮次异常后的恢复入口，不是正常流水线每步都回到 Buffer。
		a.Reserved = len(request.Plan) != 0 // 有旧计划时不能直接假定没有遗留资源。
		return nil, e.recoverAttempt(ctx, a)
	}
	if err := e.schedule(ctx, a); err != nil {
		return nil, e.handleExecutionError(ctx, a, err)
	}
	return a, nil
}

// handleExecutionError 供调度和绑定两条路径共用，仍通过原来的 Report 报告结果。
func (e *Executor) handleExecutionError(ctx context.Context, a *executionAttempt, err error) error {
	var feedbackErr *reportError
	if errors.As(err, &feedbackErr) {
		// Buffer 没接住进展时停在原处，不能把“汇报失败”当成“调度失败”继续恢复。
		return err
	}
	var bindingErr *steps.BindingError
	if errors.As(err, &bindingErr) {
		// Bind 的明确拒绝或未知结果都进入 Recover，由它判断能不能撤销暂占。
		a.failure = executionFailure(err)
		return e.recoverAttempt(ctx, a)
	}
	if a.Binding {
		// 绑定请求已经发出但结果未确认，不能一边盲目撤销，一边又安排新的执行。
		return e.report(ctx, a, staging.FeedbackUnknown, staging.PhaseBinding, err.Error(), false)
	}
	if errors.Is(err, staging.ErrHandlerUnavailable) {
		return e.blockUntilImplemented(ctx, a, err)
	}
	a.failure = executionFailure(err)
	return e.recoverAttempt(ctx, a)
}

// recoverAttempt 由执行器自己收尾，Buffer 不需要再为本次失败派发一次 Recover。
func (e *Executor) recoverAttempt(ctx context.Context, a *executionAttempt) error {
	if err := e.report(ctx, a, staging.FeedbackProgress, staging.PhaseRecovering, "", false); err != nil {
		return err
	}
	if err := e.steps.Recover(ctx, &a.Attempt); err != nil {
		kind := staging.FeedbackUnknown
		if !errors.Is(err, steps.ErrBindingResultUnknown) && errors.Is(err, staging.ErrHandlerUnavailable) {
			kind = staging.FeedbackBlocked
		}
		return e.report(ctx, a, kind, staging.PhaseRecovering, err.Error(), false)
	}
	a.Reserved, a.Binding = false, false
	return e.report(ctx, a, staging.FeedbackFinished, staging.PhaseRecovered, "", true)
}

// report 是执行器唯一的回报出口；同一次请求始终使用同一个 ActionID。
func (e *Executor) report(ctx context.Context, a *executionAttempt, kind staging.FeedbackKind, phase staging.ExecutionPhase, message string, settled bool) error {
	a.phase = phase
	failure := a.failure
	if a.Request.Operation == staging.OperationRecover {
		// Buffer 已保存外部观测错误及其成员范围，不把 Partial 成员错误扩成整轮错误。
		failure = nil
	}
	err := e.Report(ctx, staging.Feedback{Unit: a.Request.Unit, ActionID: a.Request.ActionID, Operation: a.Request.Operation,
		Kind: kind, Phase: phase, Message: message, Plan: a.Plan, Failure: failure, ResponsibilitiesSettled: settled})
	if err != nil {
		return &reportError{err}
	}
	return nil
}

type reportError struct{ error }

func (e *reportError) Unwrap() error { return e.error }
