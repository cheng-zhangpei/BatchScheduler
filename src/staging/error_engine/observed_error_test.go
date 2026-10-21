package errorengine

import (
	"testing"

	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMapPodSchedulingError(t *testing.T) {
	for _, tc := range []struct{ native, internal string }{
		{corev1.PodReasonUnschedulable, workqueue.ReasonUnschedulable},
		{corev1.PodReasonSchedulerError, workqueue.ReasonSchedulingError},
		{corev1.PodReasonSchedulingGated, workqueue.ReasonDependencyNotReady},
		{"UnknownReason", ""},
	} {
		t.Run(tc.native, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker"}, Status: corev1.PodStatus{
				Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: tc.native}},
			}}
			failure := MapPodSchedulingError(pod)
			if tc.internal == "" {
				if failure != nil {
					t.Fatalf("未知原生原因不应猜分类: %+v", failure)
				}
				return
			}
			if failure == nil || failure.Reason != tc.internal {
				t.Fatalf("映射不匹配: %+v", failure)
			}
			pod.Spec.NodeName = "node-a"
			if MapPodSchedulingError(pod) != nil {
				t.Fatal("已绑定后不能继续当成未调度失败")
			}
		})
	}
}

func TestContainerFailureIsNotSchedulingFailure(t *testing.T) {
	pod := &corev1.Pod{Status: corev1.PodStatus{
		Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady"}},
		ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}},
	}}
	if MapPodSchedulingError(pod) != nil {
		t.Fatal("容器启动失败不能直接映射成调度失败")
	}
}
