package steps

import (
	"context"
	"fmt"
	"time"

	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Bind 在 Inflight 的 worker 中运行，按 Plan 逐个提交 Pod 的 binding 子资源。
// API 返回成功表示已经完成节点分配，不等容器启动；真实资源信息随后由 informer 接回。
// 一轮内顺序提交。错误交给 Executor 的 Handler；它确认成功后继续下一个成员，
// 否则结束本轮绑定，交给原来的 Recover/Report。各轮之间的并发由 Inflight 管理。
func (r *Runner) Bind(ctx context.Context, a *Attempt) error {
	if r.Client == nil {
		return fmt.Errorf("%w: %w: Bind 缺少 Kubernetes 客户端", staging.ErrExecutionNotStarted, staging.ErrHandlerUnavailable)
	}
	bindings, err := bindingRequests(a)
	if err != nil {
		return fmt.Errorf("%w: %w", staging.ErrExecutionNotStarted, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", staging.ErrExecutionNotStarted, err)
	}
	if a.BindingDeadline.IsZero() {
		a.BindingDeadline = time.Now().Add(r.BindTimeout)
	}
	ctx, cancel := context.WithDeadline(ctx, a.BindingDeadline)
	defer cancel()
	for _, binding := range bindings {
		a.Binding = true
		err := r.BindPod(ctx, binding)
		if err != nil && r.HandleBindFailure != nil {
			err = r.HandleBindFailure(ctx, binding, err)
		}
		if err != nil {
			failure := NewBindingError(binding, err)
			// 明确拒绝说明本次请求没有绑定成功；超时等不明确的结果保留标记，交给 Recover。
			a.Binding = failure.Reason == workqueue.ReasonBindingUnknown
			return failure
		}
		a.Binding = false
		a.BoundMembers = append(a.BoundMembers, workqueue.PodRef{Name: binding.Name, UID: binding.UID})
	}
	return nil
}

// BindPod 只发送一次请求。初次绑定和 Handler 补发都经过这里，使用相同的短超时。
// ctx 同时带着整轮期限，所以重试不会重新获得一轮 30 秒。
func (r *Runner) BindPod(ctx context.Context, binding *corev1.Binding) error {
	requestCtx, cancel := context.WithTimeout(ctx, r.BindRequestTimeout)
	defer cancel()
	return r.Client.CoreV1().Pods(binding.Namespace).Bind(requestCtx, binding, metav1.CreateOptions{})
}

// bindingRequests 先把整轮请求组装好，再发网络请求；每个请求都带原 Pod UID。
// 请求里只有 Pod 身份和目标节点，不更新 informer 中的 Pod，也不重新选点。
func bindingRequests(a *Attempt) ([]*corev1.Binding, error) {
	if !a.Reserved || a.Request.Unit.Group.Namespace == "" || len(a.AssumedMembers) == 0 || len(a.Plan) != len(a.AssumedMembers) {
		return nil, fmt.Errorf("Bind 需要与实际暂占成员一致的计划")
	}
	bindings := make([]*corev1.Binding, 0, len(a.AssumedMembers))
	seen := make(map[workqueue.PodRef]bool)
	for _, ref := range a.AssumedMembers {
		if ref.Name == "" || ref.UID == "" || a.Plan[ref] == "" || seen[ref] {
			return nil, fmt.Errorf("Bind 成员 %s 缺少身份、目标节点或重复出现", ref.Name)
		}
		seen[ref] = true
		bindings = append(bindings, &corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{Namespace: a.Request.Unit.Group.Namespace, Name: ref.Name, UID: ref.UID},
			Target:     corev1.ObjectReference{Kind: "Node", Name: a.Plan[ref]},
		})
	}
	return bindings, nil
}
