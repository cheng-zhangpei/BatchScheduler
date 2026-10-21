package errorengine

import (
	"fmt"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// MapPodSchedulingError 把 informer 读到的 Pod 调度异常转成内部错误。
// 只匹配 PodScheduled=False；容器退出、镜像拉取失败等不是这次调度失败。
// 返回 nil 表示没有匹配到当前支持的调度异常，不表示整个 Pod 一切正常。
func MapPodSchedulingError(pod *corev1.Pod) *group.FailureInfo {
	if pod == nil || pod.Spec.NodeName != "" || pod.DeletionTimestamp != nil ||
		pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return nil
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type != corev1.PodScheduled || condition.Status != corev1.ConditionFalse {
			continue
		}
		var reason string
		switch condition.Reason {
		case corev1.PodReasonUnschedulable:
			// 可能是资源、污点或拓扑等原因，原生 Reason 本身没有给出细分结论。
			reason = workqueue.ReasonUnschedulable
		case corev1.PodReasonSchedulerError:
			// 原因可能是内部临时故障，也可能是声明解析错误，不能直接许诺自动重试。
			reason = workqueue.ReasonSchedulingError
		case corev1.PodReasonSchedulingGated:
			reason = workqueue.ReasonDependencyNotReady
		default:
			// 暂未定义的原生原因仍留在 Observed.Status 中，之后逐条增加规则。
			return nil
		}
		return &group.FailureInfo{Reason: reason,
			Message: fmt.Sprintf("Pod %s: PodScheduled=False, %s: %s", pod.Name, condition.Reason, condition.Message)}
	}
	return nil
}
