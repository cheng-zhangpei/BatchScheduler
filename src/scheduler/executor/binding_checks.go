package executor

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// runBindingChecks 使用一个独立循环核查未知结果，不继续占用绑定 Worker 的名额。
// 取消 Context 后停止计时、取消正在发出的 GET，由 Executor.Run 等它退出。
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

// checkBindingAttempt 只发 GET，不再补发 Bind。
// 查到绑定就更新真实占用；未绑定就记一次有效核查；全部确认后仍走唯一的 Report 入口。
func (e *Executor) checkBindingAttempt(ctx context.Context, checks []resourcecache.BindingCheck) error {
	allBound := true
	plan := make(map[workqueue.PodRef]string, len(checks))
	keys := make([]resourcecache.BindingCheckKey, 0, len(checks))
	var failures []error
	for _, check := range checks {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		keys = append(keys, check.Key)
		plan[check.Key.Pod] = check.NodeName
		queryCtx, cancel := context.WithTimeout(ctx, e.config.BindRetry.RequestTimeout)
		pod, err := e.steps.Client.CoreV1().Pods(check.Key.Unit.Group.Namespace).Get(queryCtx, check.Key.Pod.Name, metav1.GetOptions{})
		cancel()
		if err != nil {
			allBound = false
			failures = append(failures, err) // 查询失败或 NotFound 都不计作“原 Pod 仍未绑定”。
			continue
		}
		confirmed, err := e.resources.ObserveBindingCheck(check.Key, pod, time.Now(), e.config.BindingCheckInterval)
		allBound = allBound && confirmed
		if err != nil {
			failures = append(failures, err)
		}
	}
	if !allBound {
		// TODO：达到 ReadyToRelease 阈值后，接入 Forget、转入 LRU 和 Buffer 的失败回送。
		// Pod 删除、换 UID、绑定到其他节点的收尾也在下一步分别处理。
		return errors.Join(failures...)
	}
	first := checks[0].Key
	err := e.Report(ctx, staging.Feedback{Unit: first.Unit, ActionID: first.ActionID, Operation: staging.OperationRun,
		Kind: staging.FeedbackFinished, Phase: staging.PhaseBound, Plan: plan, ResponsibilitiesSettled: true})
	if err == nil || errors.Is(err, staging.ErrFeedbackObsolete) {
		// Buffer 接住确认，或已被 informer 先一步结束了本轮，都不用继续核查。
		e.resources.CompleteBindingChecks(keys)
		return nil
	}
	return err // 回报失败时保留记录，下个周期重试确认。
}
