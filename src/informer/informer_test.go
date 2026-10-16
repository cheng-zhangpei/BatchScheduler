package informer

import (
	"context"
	"testing"
	"time"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func TestInitialCacheIncludesOtherSchedulersAndStops(t *testing.T) {
	client := fake.NewClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "ours", Namespace: "work"},
			Spec:       corev1.PodSpec{SchedulerName: "batchscheduler"},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "kube-system"},
			Spec:       corev1.PodSpec{SchedulerName: "default-scheduler", NodeName: "node-1"},
		},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	groupClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{scheduling.PodGroupGVR: "PodGroupList"})
	observed, err := New(client, groupClient)
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- observed.Run(ctx, ready) }()
	// 即使断言失败，也等待 informer 停止，避免后台协程遗留。
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("informer 未按时停止")
		}
	}()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("初始缓存未同步")
	}
	nodes, err := observed.Nodes.List(labels.Everything())
	if err != nil || len(nodes) != 1 {
		t.Fatalf("节点缓存: nodes=%v, err=%v", nodes, err)
	}
	pods, err := observed.Pods.List(labels.Everything())
	if err != nil || len(pods) != 2 {
		t.Fatalf("Pod 缓存应跨 namespace 包含其他调度器的成员: pods=%v, err=%v", pods, err)
	}
}
