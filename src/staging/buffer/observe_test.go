package buffer

import (
	"context"
	"testing"
	"time"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/group"
	"batchscheduler/src/informer"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func TestObserveRefreshesFactsWithoutChangingOwnership(t *testing.T) {
	b, view := fixture(t, workqueue.AllowPartial)
	u := queuedUnit(t, view, "unit", workqueue.AllowPartial, 0)
	if err := b.Accept(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	ref := u.Members()[0]
	view.Members[0].Pod.Spec.NodeName = "node-a"
	view.Members[0].Pod.Status.Phase = corev1.PodRunning
	view.Members[0].Pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady"}}
	if err := b.Observe(context.Background(), staging.Observation{Group: u.Key().Group}); err != nil {
		t.Fatal(err)
	}
	fact := b.records[u.Key()].Observed.Members[ref]
	if !fact.Known || !fact.Present || fact.NodeName != "node-a" || fact.Status.Phase != corev1.PodRunning {
		t.Fatalf("事实未刷新: %+v", fact)
	}
	view.Members[0].Pod.Status.Conditions[0].Reason = "changed"
	if fact.Status.Conditions[0].Reason != "ContainersNotReady" {
		t.Fatal("观察记录共享了缓存的可变字段")
	}
	if b.records[u.Key()].Members[ref].Phase != group.PodUnscheduled || !b.ContainsPod(u.Key().Group, ref.UID) {
		t.Fatal("观察不应擅自覆盖执行状态或释放归属")
	}
	// 同名新 Pod 不应继承旧成员；记录成员已离开，不等同于确认旧绑定失败。
	view.Members[0].Pod.UID = "replacement"
	if err := b.Observe(context.Background(), staging.Observation{Group: u.Key().Group}); err != nil {
		t.Fatal(err)
	}
	if fact := b.records[u.Key()].Observed.Members[ref]; !fact.Known || fact.Present {
		t.Fatalf("同名重建未区分 UID: %+v", fact)
	}
	// 旧事件附带 View 也不能覆盖当前 Store；同名新组不冒充原组。
	view.Group.UID = "replacement-group"
	if err := b.Observe(context.Background(), staging.Observation{Group: u.Key().Group, View: &group.View{}}); err != nil {
		t.Fatal(err)
	}
	if facts := b.records[u.Key()].Observed; facts.Exists || facts.Members[ref].Known {
		t.Fatal("组已换 UID 时不能确认原成员状态")
	}
}

func TestObserveWithoutAcceptedUnitDoesNotAccumulateRecords(t *testing.T) {
	b, view := fixture(t, workqueue.AllowPartial)
	if err := b.Observe(context.Background(), staging.Observation{Group: view.Members[0].Group}); err != nil {
		t.Fatal(err)
	}
	if len(b.records) != 0 || len(b.groups) != 0 || len(b.owners) != 0 {
		t.Fatal("纯观察不应创建执行记录或持有成员")
	}
}

func TestInformerCallbackUpdatesBufferOnPodChangeAndDeletion(t *testing.T) {
	_, view := fixture(t, workqueue.AllowPartial)
	view.Group.APIVersion = scheduling.GroupVersion.String()
	view.Group.Kind = "PodGroup"
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(view.Group)
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientset(view.Members[0].Pod.DeepCopy(), view.Members[1].Pod.DeepCopy())
	groupClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{scheduling.PodGroupGVR: "PodGroupList"}, &unstructured.Unstructured{Object: raw})
	i, err := informer.New(client, groupClient)
	if err != nil {
		t.Fatal(err)
	}
	b := NewBuffer(i.GroupViews, testHistory(t))
	u := queuedUnit(t, view, "unit", workqueue.AllowPartial, 0)
	ref := u.Members()[0]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	changes := make(chan staging.ObservedPod, 16)
	errors := make(chan error, 16)
	if err := i.OnGroupChange(func(key group.Key) {
		if err := b.Observe(ctx, staging.Observation{Group: key}); err != nil {
			select {
			case errors <- err:
			default:
			}
			return
		}
		b.mu.RLock()
		defer b.mu.RUnlock()
		if record := b.records[u.Key()]; record != nil && record.Observed != nil {
			select {
			case changes <- record.Observed.Members[ref]:
			default:
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	ready, done := make(chan struct{}), make(chan error, 1)
	go func() { done <- i.Run(ctx, ready) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("缓存未同步")
	}
	if err := b.Accept(ctx, u); err != nil {
		t.Fatal(err)
	}
	pod := view.Members[0].Pod.DeepCopy()
	waitFor := func(match func(staging.ObservedPod) bool) {
		t.Helper()
		for {
			select {
			case err := <-errors:
				t.Fatal(err)
			case fact := <-changes:
				if match(fact) {
					return
				}
			case <-ctx.Done():
				t.Fatal("Informer → Observe 未收到预期事实")
			}
		}
	}
	// 从 fake API 写入原生调度失败，让 informer 真正经 Observe 完成错误映射。
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}
	if _, err := client.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(func(f staging.ObservedPod) bool {
		return f.Failure != nil && f.Failure.Reason == workqueue.ReasonUnschedulable
	})
	ds, err := b.Evaluate(ctx, u.Key().Group)
	if err != nil {
		t.Fatal(err)
	}
	matched := false
	for _, d := range ds {
		if d.Unit != nil && *d.Unit == u.Key() {
			matched = d.Action == staging.ActionExecute && d.Operation == staging.OperationRecover && d.Failure != nil && d.Failure.Reason == workqueue.ReasonUnschedulable
		}
	}
	if !matched {
		t.Fatalf("informer 异常没有传到 Evaluate/error_engine: %+v", ds)
	}
	pod.Spec.NodeName = "node-a"
	if _, err := client.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(func(f staging.ObservedPod) bool { return f.Present && f.NodeName == "node-a" && f.Failure == nil })
	if err := client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(func(f staging.ObservedPod) bool { return f.Known && !f.Present })
	if !b.ContainsPod(u.Key().Group, ref.UID) {
		t.Fatal("删除观察不能直接释放未知执行责任")
	}
}
