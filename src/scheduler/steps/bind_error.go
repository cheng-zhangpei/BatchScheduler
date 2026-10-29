package steps

import (
	"errors"
	"fmt"

	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

var ErrBindingResultUnknown = errors.New("绑定结果还未确认")

// BindingError 保留原 API 错误，并给出调度器内部分类，Executor 再交给 Recover。
type BindingError struct {
	Reason string
	Cause  error
}

func (e *BindingError) Error() string { return e.Cause.Error() }
func (e *BindingError) Unwrap() error { return e.Cause }

// 这里只认明确的拒绝类型，不根据错误文字猜结果。
// Conflict 也先算未知：Pod 可能已经被其他请求绑定，不能直接当成仍未绑定。
// 网络错误、超时和 5xx 同样需要后续查 API 确认，不自动重发或 Forget。
func newBindingError(binding *corev1.Binding, err error) *BindingError {
	reason := workqueue.ReasonBindingUnknown
	switch {
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		reason = workqueue.ReasonForbidden
	case apierrors.IsInvalid(err), apierrors.IsBadRequest(err), apierrors.IsMethodNotSupported(err):
		reason = workqueue.ReasonInvalidSpec
	case apierrors.IsNotFound(err):
		reason = workqueue.ReasonObjectDeleted
	case apierrors.IsTooManyRequests(err):
		reason = workqueue.ReasonBindingRejected
	}
	return &BindingError{Reason: reason,
		Cause: fmt.Errorf("Bind Pod %s/%s → %s: %w", binding.Namespace, binding.Name, binding.Target.Name, err)}
}
