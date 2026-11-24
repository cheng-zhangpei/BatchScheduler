package executor

import (
	"context"
	"fmt"
	"time"

	"batchscheduler/src/scheduler/steps"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// BindRetryPolicy 控制本轮绑定中的短请求和指数退避，总期限仍由 BindTimeout 决定。
type BindRetryPolicy struct {
	MaxAttempts    int           // 包含首次发送；查询失败也消耗一次处理机会，不无限查询。
	RequestTimeout time.Duration // 每次 Bind 或 GET 的最长等待时间。
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// bindFailHandler 属于 Executor，只处理当前成员的查询和补发。
// 它不重新选节点、不再次 Assume、不搬 WorkQueue，也不直接调用 Report。
// 返回 nil 后 steps.Bind 记录这个成员成功；返回错误后 Executor 统一 Recover/Report。
type bindFailHandler struct {
	runner *steps.Runner
	policy BindRetryPolicy
}

// Handle 是绑定出错后的唯一入口：明确的永久拒绝直接结束，其余进入有限重试。
func (h *bindFailHandler) Handle(ctx context.Context, binding *corev1.Binding, err error) error {
	failure := steps.NewBindingError(binding, err)
	switch failure.Reason {
	case workqueue.ReasonForbidden, workqueue.ReasonInvalidSpec, workqueue.ReasonObjectDeleted:
		return h.handlePermanentFailure(failure, false)
	case workqueue.ReasonBindingRejected, workqueue.ReasonBindingUnknown:
		return h.retry(ctx, binding, failure)
	default:
		return failure
	}
}

// retry 每轮先查实际 Pod，再等待退避并向原节点补发；查到成功立即结束。
// Query 失败只重试查询，不盲目追加 Bind。整轮 Context 到期会结束查询、退避和发送。
func (h *bindFailHandler) retry(ctx context.Context, binding *corev1.Binding, failure *steps.BindingError) error {
	unknown := failure.Reason == workqueue.ReasonBindingUnknown
	backoff := h.policy.InitialBackoff
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return finishBindFailure(failure, unknown)
		}
		observation, queryErr := h.lookup(ctx, binding)
		switch observation {
		case bindingConfirmed:
			return nil
		case bindingPodGone:
			return &steps.BindingError{Reason: workqueue.ReasonObjectDeleted, Cause: queryErr}
		case bindingElsewhere:
			return finishBindFailure(&steps.BindingError{Cause: queryErr}, true)
		}
		if attempt >= h.policy.MaxAttempts {
			return finishBindFailure(failure, unknown)
		}
		if err := waitBindBackoff(ctx, backoff); err != nil {
			return finishBindFailure(failure, unknown)
		}
		if backoff < h.policy.MaxBackoff {
			// 每次最多翻倍，到最大间隔后保持不变。
			backoff += min(backoff, h.policy.MaxBackoff-backoff)
		}
		if queryErr != nil {
			continue
		}
		err := h.runner.BindPod(ctx, binding)
		if err == nil {
			return nil
		}
		failure = steps.NewBindingError(binding, err)
		unknown = unknown || failure.Reason == workqueue.ReasonBindingUnknown
		switch failure.Reason {
		case workqueue.ReasonForbidden, workqueue.ReasonInvalidSpec, workqueue.ReasonObjectDeleted:
			return h.handlePermanentFailure(failure, unknown)
		}
	}
}

// handlePermanentFailure 停止补发，交给现有 Recover 决定资源如何收尾。
// 如果更早有一次请求结果未知，本次明确拒绝也不能证明旧请求失败。
func (h *bindFailHandler) handlePermanentFailure(failure *steps.BindingError, unknown bool) error {
	return finishBindFailure(failure, unknown)
}

// finishBindFailure 保留“曾有请求结果未知”的事实，Recover 据此保留暂占。
// 已知只有明确拒绝时，仍返回原错误，允许现有恢复流程 Forget。
func finishBindFailure(failure *steps.BindingError, unknown bool) error {
	if unknown {
		return &steps.BindingError{Reason: workqueue.ReasonBindingUnknown,
			Cause: fmt.Errorf("绑定仍需确认: %w", failure)}
	}
	return failure
}

func waitBindBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
