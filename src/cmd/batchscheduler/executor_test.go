package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/group"
	execution "batchscheduler/src/scheduler/executor"
	"batchscheduler/src/staging"
	"batchscheduler/src/staging/buffer"
	"batchscheduler/src/staging/history"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type executorTestViews struct{ view *group.View }

func (v executorTestViews) Get(string, string) (*group.View, error) { return v.view, nil }

// 使用和 main 相同的接线、真实执行器和原生过滤插件，只用本地缓存替代集群。
func executorFixture(t *testing.T) (*buffer.Buffer, workqueue.Unit, *execution.Executor, *group.View) {
	t.Helper()
	key := group.Key{Namespace: "work", Name: "job", UID: "group"}
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: "pod", UID: "pod",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: scheduling.GroupVersion.String(), Kind: "PodGroup", Name: key.Name, UID: key.UID, Controller: &controller}},
	}, Status: corev1.PodStatus{Phase: corev1.PodPending}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
	}}}}}
	view := &group.View{Group: &scheduling.PodGroup{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: key.UID},
		Spec: scheduling.PodGroupSpec{Members: 1, SubmitMode: string(workqueue.AllTogether)}}, Members: []group.PodView{{Group: key, Pod: pod}}}
	reader := executorTestViews{view}
	nodes := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourcePods: resource.MustParse("32"),
	}}}
	if err := nodes.Add(node); err != nil {
		t.Fatal(err)
	}
	pods := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	if err := pods.Add(pod); err != nil {
		t.Fatal(err)
	}
	nodeLister := corelisters.NewNodeLister(nodes)
	executor := execution.NewExecutor(reader, nodeLister, corelisters.NewPodLister(pods))
	w := workqueue.New()
	w.Views = reader
	store, err := history.Open(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	b := buffer.NewBuffer(reader, store, newStagingHandlers(w, nodeLister, executor))
	executor.SetGroupContextReader(b)
	executor.Report, w.Staging = b.Report, b
	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- executor.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	if err := w.Select(workqueue.SelectRequest{View: view}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	u, err := w.Active.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Accept(ctx, u); err != nil {
		t.Fatal(err)
	}
	return b, u, executor, view
}

func executionDecision(t *testing.T, b *buffer.Buffer, unit workqueue.Unit) staging.Decision {
	t.Helper()
	ctx := context.Background()
	if err := b.Observe(ctx, staging.Observation{Group: unit.Key().Group}); err != nil {
		t.Fatal(err)
	}
	decisions, err := b.Evaluate(ctx, unit.Key().Group)
	if err != nil {
		t.Fatal(err)
	}
	for _, decision := range decisions {
		if decision.Unit != nil && *decision.Unit == unit.Key() {
			return decision
		}
	}
	t.Fatal("没有本轮决策")
	return staging.Decision{}
}

func TestBufferExecuteRunsInternalFilterAndStopsBeforePlanning(t *testing.T) {
	b, unit, executor, _ := executorFixture(t)
	reports := make(chan staging.Feedback, 16)
	executor.Report = func(ctx context.Context, feedback staging.Feedback) error {
		if err := b.Report(ctx, feedback); err != nil {
			return err
		}
		reports <- feedback
		return nil
	}
	if err := b.Apply(context.Background(), executionDecision(t, b, unit)); err != nil {
		t.Fatal(err)
	}
	var actionID string
	phases := []staging.ExecutionPhase{staging.PhaseFiltering, staging.PhaseFiltered, staging.PhaseScored, staging.PhaseScored}
	kinds := []staging.FeedbackKind{staging.FeedbackProgress, staging.FeedbackProgress, staging.FeedbackProgress, staging.FeedbackBlocked}
	for i, phase := range phases {
		select {
		case feedback := <-reports:
			if feedback.Phase != phase || feedback.Kind != kinds[i] {
				t.Fatalf("进度不符合实际流程: %+v", feedback)
			}
			if actionID == "" {
				actionID = feedback.ActionID
			}
			if feedback.ActionID != actionID || feedback.Operation != staging.OperationRun {
				t.Fatal("同一轮被拆成多次执行")
			}
			if feedback.Kind == staging.FeedbackBlocked && !feedback.ResponsibilitiesSettled {
				t.Fatal("过滤和评分没有资源操作，应该明确结清")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("执行器没有报告进度")
		}
	}
	if next := executionDecision(t, b, unit); next.Action != staging.ActionWait || next.Reason != "ExecutionBlocked" {
		t.Fatalf("待实现步骤不能反复重新调度: %+v", next)
	}
	if unit.State().Queue != workqueue.QueueNone || unit.State().Phase != workqueue.UnitProcessing {
		t.Fatal("进度报告不应移动 workqueue")
	}
}

func TestBufferExecuteFilterFailureRecoversInternally(t *testing.T) {
	b, unit, executor, view := executorFixture(t)
	view.Members[0].Pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "ready"}}
	finished := make(chan staging.Feedback, 1)
	executor.Report = func(ctx context.Context, f staging.Feedback) error {
		if err := b.Report(ctx, f); err != nil {
			return err
		}
		if f.Kind == staging.FeedbackFinished {
			finished <- f
		}
		return nil
	}
	if err := b.Apply(context.Background(), executionDecision(t, b, unit)); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-finished:
		if f.Phase != staging.PhaseRecovered || f.Failure == nil || !f.ResponsibilitiesSettled {
			t.Fatalf("执行器没有自行恢复并结清: %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("未收到整轮结果")
	}
	if next := executionDecision(t, b, unit); next.Action != staging.ActionReturn || next.To != workqueue.QueueFail {
		t.Fatalf("内部恢复后应直接回 Fail，不再由 Buffer 派发 Recover: %+v", next)
	}
}

func TestExecutorDoesNotAcceptExternalAssumeOrBind(t *testing.T) {
	_, unit, executor, _ := executorFixture(t)
	for _, operation := range []staging.Operation{"Assume", "Bind"} {
		err := executor.Execute(context.Background(), staging.Execution{Unit: unit.Key(), ActionID: "action", Operation: operation, Members: unit.Members()})
		if !errors.Is(err, staging.ErrExecutionNotStarted) {
			t.Fatalf("%s 不应再由外部单独派发: %v", operation, err)
		}
	}
}
