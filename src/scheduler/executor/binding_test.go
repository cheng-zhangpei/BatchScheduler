package executor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/group"
	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// 暂停第一轮的后台绑定，第二轮仍应调度，并看见第一轮 Assume 留下的占用。
func TestAsyncBindingAllowsNextScheduleAndKeepsAssumedResources(t *testing.T) {
	e, resources, requests := asyncBindingFixture(2, "1")
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBinding := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseBinding()
	finished := make(chan staging.Feedback, 4)
	client := fake.NewClientset()
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "binding" {
			return false, nil, nil
		}
		entered <- struct{}{}
		<-release
		return true, nil, nil
	})
	e.steps.Client = client
	e.Report = func(_ context.Context, f staging.Feedback) error {
		if f.Kind == staging.FeedbackFinished || f.Kind == staging.FeedbackBlocked {
			finished <- f
		}
		return nil
	}
	startAsyncExecutor(t, e)
	if err := e.Execute(context.Background(), requests[0]); err != nil {
		t.Fatal(err)
	}
	awaitBindingEvent(t, entered)
	// 同一请求还在绑定时再次提交，只接收一次，不重复启动或重复暂占。
	if err := e.Execute(context.Background(), requests[0]); err != nil {
		t.Fatal(err)
	}
	if err := e.Execute(context.Background(), requests[1]); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-finished:
		if f.Unit != requests[1].Unit || f.Failure == nil || f.Failure.Reason != workqueue.ReasonInsufficientResources {
			t.Fatalf("第二轮没有看见仍在绑定的第一轮占用: %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("第一轮绑定阻塞了后续调度")
	}
	if resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 1000 {
		t.Fatal("第二轮失败清理误删了第一轮的暂占")
	}
	releaseBinding()
	select {
	case f := <-finished:
		if f.Unit != requests[0].Unit || f.Kind != staging.FeedbackFinished || f.Phase != staging.PhaseBound || !f.ResponsibilitiesSettled {
			t.Fatalf("Bind 成功没有沿 Report 回报: %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("第一轮没有完成后台反馈")
	}
	if resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 1000 {
		t.Fatal("成功绑定在 informer 回来前丢失了资源占用")
	}
	select {
	case <-entered:
		t.Fatal("同一执行在后台绑定期间被重复启动")
	default:
	}
}

// 把后台全部暂停：达到上限后，下一轮必须在 Assume 前等待。
func TestAsyncBindingLimitsInFlightAndRunWaitsForBackground(t *testing.T) {
	const limit = 2
	e, resources, requests := asyncBindingFixture(limit+1, "16")
	e.config.MaxInFlight, e.config.BindWorkers = limit, limit
	entered := make(chan struct{}, limit+1)
	release := make(chan struct{})
	e.Report = func(_ context.Context, f staging.Feedback) error {
		if f.Phase == staging.PhaseBinding {
			entered <- struct{}{}
			<-release // 测试控制退出时机，验证 Run 也会等后台结束。
		}
		return nil
	}
	stop, done := startAsyncExecutor(t, e)
	// 即使测试提前失败，也先放开后台，再由启动辅助函数等待 Run 退出。
	defer close(release)
	for _, request := range requests {
		if err := e.Execute(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	for range limit {
		awaitBindingEvent(t, entered)
	}
	select {
	case <-entered:
		t.Fatal("后台绑定超过了并发上限")
	case <-time.After(100 * time.Millisecond):
	}
	if resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != int64(limit)*1000 {
		t.Fatal("没有绑定名额时仍提前暂占了下一轮资源")
	}
	stop()
	select {
	case <-done:
		t.Fatal("Run 在后台绑定退出前就返回了")
	case <-time.After(100 * time.Millisecond):
	}
}

func asyncBindingFixture(count int, cpu string) (*Executor, *resourcecache.Cache, []staging.Execution) {
	key := group.Key{Namespace: "work", Name: "job", UID: "group"}
	view := &group.View{Group: &scheduling.PodGroup{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: key.UID}}}
	requests := make([]staging.Execution, 0, count)
	for i := range count {
		name := fmt.Sprintf("pod-%d", i)
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: name, UID: types.UID(name)},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			}}}}}
		view.Members = append(view.Members, group.PodView{Group: key, Pod: pod})
		requests = append(requests, staging.Execution{Unit: workqueue.UnitKey{Group: key, ID: name}, ActionID: name,
			Operation: staging.OperationRun, Mode: workqueue.AllowPartial, Members: []workqueue.PodRef{{Name: name, UID: pod.UID}}})
	}
	resources := resourcecache.New()
	resources.UpdateNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourcePods: resource.MustParse("32"),
	}}})
	return NewExecutor(executionViews{view}, resources, DefaultConfig()), resources, requests
}

func startAsyncExecutor(t *testing.T, e *Executor) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := e.Run(ctx); err != nil {
			t.Error(err)
		}
	}()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Executor 没有退出")
		}
	})
	return stop, done
}

func awaitBindingEvent(t *testing.T, events <-chan struct{}) {
	t.Helper()
	select {
	case <-events:
	case <-time.After(5 * time.Second):
		t.Fatal("没有进入后台绑定")
	}
}
