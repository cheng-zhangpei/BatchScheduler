package executor

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type bindingObservation int

const (
	bindingUnbound bindingObservation = iota
	bindingConfirmed
	bindingPodGone
	bindingElsewhere
	bindingLookupFailed
)

// lookup 直接查询 API Server，不用 informer 的旧对象判断这次 Bind 是否成功。
// 同一个 UID 落在原计划节点才算成功；其他结果返回给 retry 统一处理。
func (h *bindFailHandler) lookup(ctx context.Context, binding *corev1.Binding) (bindingObservation, error) {
	queryCtx, cancel := context.WithTimeout(ctx, h.policy.RequestTimeout)
	defer cancel()
	pod, err := h.runner.Client.CoreV1().Pods(binding.Namespace).Get(queryCtx, binding.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return bindingPodGone, err
	case err != nil:
		return bindingLookupFailed, err
	case pod.UID != binding.UID:
		return bindingPodGone, fmt.Errorf("原 Pod %s/%s（UID=%s）已被替换", binding.Namespace, binding.Name, binding.UID)
	case pod.Spec.NodeName == binding.Target.Name:
		return bindingConfirmed, nil
	case pod.Spec.NodeName != "":
		return bindingElsewhere, fmt.Errorf("Pod %s/%s 已绑定到 %s，原计划为 %s", pod.Namespace, pod.Name, pod.Spec.NodeName, binding.Target.Name)
	case pod.DeletionTimestamp != nil:
		return bindingPodGone, fmt.Errorf("Pod %s/%s 正在删除，停止补发绑定", pod.Namespace, pod.Name)
	default:
		return bindingUnbound, nil
	}
}
