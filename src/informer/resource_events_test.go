package informer

import (
	"context"
	"sync"
	"testing"
	"time"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/scheduler/resourcecache"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

type blockedResourceObserver struct {
	*resourcecache.Cache
	entered chan struct{}
	release chan struct{}
}

func (b *blockedResourceObserver) UpdatePod(pod *corev1.Pod) error {
	if pod.UID == "bound" {
		close(b.entered)
		<-b.release
	}
	return b.Cache.UpdatePod(pod)
}

func TestReadyWaitsForResourceAccounting(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "node"},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")}}}
	bound := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "bound", Namespace: "other", UID: "bound"},
		Spec: corev1.PodSpec{NodeName: "node-a", SchedulerName: "default-scheduler", Containers: []corev1.Container{{
			Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}},
		}}}}
	pending := bound.DeepCopy()
	pending.Name, pending.UID, pending.Spec.NodeName = "pending", "pending", ""
	client := fake.NewClientset(node, bound, pending)
	groups := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{scheduling.PodGroupGVR: "PodGroupList"})
	observed, err := New(client, groups)
	if err != nil {
		t.Fatal(err)
	}
	resources := &blockedResourceObserver{Cache: resourcecache.New(), entered: make(chan struct{}), release: make(chan struct{})}
	if err := observed.OnResourceChange(resources); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ready, done := make(chan struct{}), make(chan error, 1)
	var release sync.Once
	go func() { done <- observed.Run(ctx, ready) }()
	t.Cleanup(func() {
		release.Do(func() { close(resources.release) })
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	select {
	case <-resources.entered:
	case <-ctx.Done():
		t.Fatal("未收到初始资源事件")
	}
	if !cache.WaitForCacheSync(ctx.Done(), observed.podInformer.HasSynced, observed.groupInformer.HasSynced,
		observed.factory.Core().V1().Nodes().Informer().HasSynced) {
		t.Fatal("informer 索引没有完成初始同步")
	}
	// 故意卡住资源回调：索引虽已齐全，调度入口仍不能收到 ready。
	select {
	case <-ready:
		t.Fatal("资源尚未记完就允许启动调度")
	case <-time.After(150 * time.Millisecond):
	}
	release.Do(func() { close(resources.release) })
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("资源处理完成后没有 ready")
	}
	info := resources.SnapshotNodeInfos()["node-a"]
	if info == nil || info.GetRequested().GetMilliCPU() != 2000 || len(info.GetPods()) != 1 {
		t.Fatal("初始资源账没有计入其他调度器的已绑定 Pod，或错误计入 Pending Pod")
	}
	// ready 后仍由相同回调持续维护占用。
	if err := client.CoreV1().Pods(bound.Namespace).Delete(ctx, bound.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for resources.SnapshotNodeInfos()["node-a"].GetRequested().GetMilliCPU() != 0 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("Watch 删除事件没有释放资源占用")
		}
	}
}

func TestInvalidInitialResourcesDoNotOpenReady(t *testing.T) {
	client := fake.NewClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "missing-uid", Namespace: "work"},
		Spec: corev1.PodSpec{NodeName: "node-a"}})
	groups := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{scheduling.PodGroupGVR: "PodGroupList"})
	observed, err := New(client, groups)
	if err != nil {
		t.Fatal(err)
	}
	if err := observed.OnResourceChange(resourcecache.New()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ready := make(chan struct{})
	if err := observed.Run(ctx, ready); err == nil {
		t.Fatal("资源初始化错误没有返回给入口")
	}
	if ctx.Err() != nil {
		t.Fatal("资源初始化失败后没有及时停止 informer")
	}
	select {
	case <-ready:
		t.Fatal("资源初始化失败仍然允许调度启动")
	default:
	}
}
