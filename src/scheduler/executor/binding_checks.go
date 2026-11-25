package executor

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"batchscheduler/src/group"
	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// runBindingChecks 只检查原截止时间和缓存中的真实绑定，不再增加一轮 GET。
// 取消 Context 后停止计时，由 Executor.Run 等它退出。
func (e *Executor) runBindingChecks(ctx context.Context) {
	if e.resources == nil {
		return
	}
	ticker := time.NewTicker(e.config.BindingCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.checkBindings(ctx); err != nil && ctx.Err() == nil {
				slog.Debug("绑定核查未完成，下一轮继续", "error", err)
			}
		}
	}
}

// checkBindings 先按执行编号分组，再核查每组；只能在整轮接纳成员都确认后补报成功。
func (e *Executor) checkBindings(ctx context.Context) error {
	groups := make(map[executionKey][]resourcecache.BindingCheck)
	for _, check := range e.resources.BindingChecks() {
		key := executionKey{Unit: check.Key.Unit, ActionID: check.Key.ActionID}
		groups[key] = append(groups[key], check)
	}
	var failures []error
	for _, checks := range groups {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := e.checkBindingAttempt(ctx, checks); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// checkBindingAttempt 原预算内接住 informer 确认，到期直接回收；不再发 GET 或 Bind。
func (e *Executor) checkBindingAttempt(ctx context.Context, checks []resourcecache.BindingCheck) error {
	plan := make(map[workqueue.PodRef]string, len(checks))
	keys := make([]resourcecache.BindingCheckKey, 0, len(checks))
	for _, check := range checks {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		keys = append(keys, check.Key)
		plan[check.Key.Pod] = check.NodeName
	}
	result, err := e.resources.ReclaimBindingChecks(keys, time.Now())
	if err != nil || !result.Ready {
		return err
	}
	first := checks[0].Key
	f := staging.Feedback{Unit: first.Unit, ActionID: first.ActionID, Operation: staging.OperationRun,
		Kind: staging.FeedbackFinished, Phase: staging.PhaseBound, Plan: plan, ResponsibilitiesSettled: true,
		ReleasedBindings: result.Released}
	switch {
	case result.Mismatched:
		// TODO：实际绑定偏离原计划时的处理；本轮不把真实位置当成原计划成功。
		f.Kind, f.Phase, f.ResponsibilitiesSettled = staging.FeedbackBlocked, staging.PhaseRecovering, false
		f.Message = "实际绑定不能只靠 Forget 撤销，等待已绑定成员恢复策略"
		f.Failure = &group.FailureInfo{Reason: workqueue.ReasonBindingConfirmationExpired, Message: f.Message}
	case len(result.Bound) == 0:
		f.Phase = staging.PhaseRecovered
		f.Failure = &group.FailureInfo{Reason: workqueue.ReasonBindingConfirmationExpired, Message: "已按核查策略释放本轮暂占，等待退避重试"}
	}
	err = e.Report(ctx, f)
	if err == nil || errors.Is(err, staging.ErrFeedbackObsolete) {
		// Buffer 接住确认，或已被 informer 先一步结束了本轮，都不用继续核查。
		e.resources.CompleteBindingChecks(keys)
		return nil
	}
	return err // 回报失败时保留记录，下个周期重试确认。
}
