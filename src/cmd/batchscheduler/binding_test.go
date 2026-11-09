package main

import (
	"context"
	"testing"
	"time"

	execution "batchscheduler/src/scheduler/executor"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestGroupPlanRetriesAfterCapacityIsReleased(t *testing.T) {
	config := execution.DefaultConfig()
	client := fake.NewClientset()
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return action.GetSubresource() == "binding", nil, nil
	})
	config.Client = client
	b, queues, executor, _, resources := queuedExecutorFixtureWithMembers(t, 2, config)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 节点有 4 核，已有 Pod 占 3 核。两个成员各需 1 核，单独通过 Filter，整组放不下。
	occupier := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "occupier", UID: "occupier"},
		Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")},
		}}}}}
	if err := resources.UpdatePod(occupier); err != nil {
		t.Fatal(err)
	}
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
	startAttempt := func() workqueue.Unit {
		t.Helper()
		u, err := queues.Active.Pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Accept(ctx, u); err != nil {
			t.Fatal(err)
		}
		if err := b.Apply(ctx, executionDecision(t, b, u)); err != nil {
			t.Fatal(err)
		}
		return u
	}
	old := startAttempt()
	failure := waitBindingFeedback(t, finished)
	if failure.Phase != staging.PhaseRecovered || failure.Failure == nil || failure.Failure.Reason != workqueue.ReasonUnschedulable || len(client.Actions()) != 0 {
		t.Fatalf("整组规划失败应可重试，且不提交绑定: %+v", failure)
	}
	if err := b.Apply(ctx, executionDecision(t, b, old)); err != nil {
		t.Fatal(err)
	}
	queued := queues.GroupSnapshot(old.Key().Group)
	if len(queued) != 1 || queued[0].State.Queue != workqueue.QueueFail || len(queued[0].Unit.Members()) != 2 {
		t.Fatalf("应将整组成员一起回送 Fail: %+v", queued)
	}
	next := queued[0].Unit
	if err := b.Apply(ctx, executionDecision(t, b, next)); err != nil {
		t.Fatal(err)
	}
	if next.State().Queue != workqueue.QueueRetry {
		t.Fatal("未找到组方案的失败应进入 Retry 等待资源变化")
	}
	if err := resources.RemovePod(occupier); err != nil {
		t.Fatal(err)
	}
	b.NotifyRetry(workqueue.RetryOnResources)
	if err := b.Apply(ctx, executionDecision(t, b, next)); err != nil {
		t.Fatal(err)
	}
	if ready := startAttempt(); ready.Key() != next.Key() {
		t.Fatalf("应从 Active 取到重新就绪的组单元: %+v", ready.Key())
	}
	if f := waitBindingFeedback(t, finished); f.Phase != staging.PhaseBound || len(client.Actions()) != 2 {
		t.Fatalf("释放资源后应成功绑定两个成员: %+v", f)
	}
}

// 走真实 Buffer → Executor → Inflight → steps.Bind → Report，再模拟 informer 确认后清理归档。
func TestBindingSuccessClosesBufferAttemptAfterObservation(t *testing.T) {
	config := execution.DefaultConfig()
	client := fake.NewClientset()
	bindings := make(chan *corev1.Binding, 2)
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "binding" {
			return false, nil, nil
		}
		bindings <- action.(clienttesting.CreateAction).GetObject().(*corev1.Binding).DeepCopy()
		return true, nil, nil
	})
	config.Client = client
	b, unit, executor, view, resources := executorFixture(t, config)
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
	f := waitBindingFeedback(t, finished)
	if f.Phase != staging.PhaseBound || !f.ResponsibilitiesSettled || f.Failure != nil {
		t.Fatalf("绑定成功没有完整回报: %+v", f)
	}
	binding := <-bindings
	ref := unit.Members()[0]
	if binding.Name != ref.Name || binding.UID != ref.UID || binding.Namespace != unit.Key().Group.Namespace || binding.Target.Name != "node-a" {
		t.Fatalf("实际绑定请求不符合计划: %+v", binding)
	}
	if next := executionDecision(t, b, unit); next.Action == staging.ActionRelease {
		t.Fatal("informer 尚未确认绑定就删除了 Record")
	}
	// 模拟 informer 的真实更新：资源账接住实际绑定，GroupView 也变为已绑定。
	bound := view.Members[0].Pod.DeepCopy()
	bound.Spec.NodeName = "node-a"
	if err := resources.UpdatePod(bound); err != nil {
		t.Fatal(err)
	}
	view.Members[0].Pod = bound
	decision := executionDecision(t, b, unit)
	if decision.Action != staging.ActionRelease || decision.Reason != "AttemptComplete" {
		t.Fatalf("观察到真实绑定后没有结束本轮: %+v", decision)
	}
	if err := b.Apply(context.Background(), decision); err != nil {
		t.Fatal(err)
	}
	groupContext, err := b.GroupContext(context.Background(), unit.Key().Group)
	if err != nil || len(groupContext.InFlight) != 0 || len(groupContext.Succeeded) != 1 {
		t.Fatalf("成功记录没有清理并归档: %+v, err=%v", groupContext, err)
	}
	snapshot := resources.Snapshot()
	if snapshot.Nodes["node-a"].GetRequested().GetMilliCPU() != 1000 || len(snapshot.BoundPods) != 1 {
		t.Fatal("确认绑定后资源被重复累计或误清理")
	}
}

func TestBindingRejectionRecoversAndTransitionsToRetry(t *testing.T) {
	config := execution.DefaultConfig()
	client := fake.NewClientset()
	client.PrependReactor("create", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewTooManyRequests("busy", 1)
	})
	config.Client = client
	b, unit, executor, _, resources := executorFixture(t, config)
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
	f := waitBindingFeedback(t, finished)
	if f.Phase != staging.PhaseRecovered || !f.ResponsibilitiesSettled || f.Failure == nil || f.Failure.Reason != workqueue.ReasonBindingRejected {
		t.Fatalf("拒绝后没有经 Recover 归类并结清: %+v", f)
	}
	if resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 0 {
		t.Fatal("绑定拒绝后没有 Forget")
	}
	decision := executionDecision(t, b, unit)
	if decision.Action != staging.ActionReturn || decision.To != workqueue.QueueFail {
		t.Fatalf("没有回 Fail: %+v", decision)
	}
	if err := b.Apply(context.Background(), decision); err != nil {
		t.Fatal(err)
	}
	decisions, err := b.Evaluate(context.Background(), unit.Key().Group)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range decisions {
		if d.Action == staging.ActionTransition && d.Transition.To == workqueue.QueueRetry {
			if d.Transition.RetryAfter.IsZero() {
				t.Fatal("临时绑定拒绝缺少退避时间")
			}
			if err := b.Apply(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatal("临时绑定拒绝没有进入带退避条件的 Retry")
}

func TestBindingUnknownReleasesAfterMatchingObservation(t *testing.T) {
	config := execution.DefaultConfig()
	client := fake.NewClientset()
	client.PrependReactor("create", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, context.DeadlineExceeded
	})
	config.Client = client
	b, unit, executor, view, resources := executorFixture(t, config)
	unknown := make(chan staging.Feedback, 1)
	executor.Report = func(ctx context.Context, f staging.Feedback) error {
		if err := b.Report(ctx, f); err != nil {
			return err
		}
		if f.Kind == staging.FeedbackUnknown {
			unknown <- f
		}
		return nil
	}
	if err := b.Apply(context.Background(), executionDecision(t, b, unit)); err != nil {
		t.Fatal(err)
	}
	f := waitBindingFeedback(t, unknown)
	if f.Phase != staging.PhaseRecovering || f.ResponsibilitiesSettled || f.Failure == nil || f.Failure.Reason != workqueue.ReasonBindingUnknown {
		t.Fatalf("未知结果被误报为已恢复: %+v", f)
	}
	if resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 1000 {
		t.Fatal("未知绑定被擅自 Forget")
	}
	if decision := executionDecision(t, b, unit); decision.Action != staging.ActionWait || decision.Reason != "OperationInFlight" {
		t.Fatalf("未知结果没有保留 Pending: %+v", decision)
	}

	// 同名新 Pod，或原 Pod 被绑到了别处，都不能确认这次 Bind 成功。
	bound := view.Members[0].Pod.DeepCopy()
	replacement := bound.DeepCopy()
	replacement.UID, replacement.Spec.NodeName = "replacement", "node-a"
	wrongNode := bound.DeepCopy()
	wrongNode.Spec.NodeName = "node-other"
	for _, pod := range []*corev1.Pod{replacement, wrongNode} {
		view.Members[0].Pod = pod
		if d := executionDecision(t, b, unit); d.Action != staging.ActionWait {
			t.Fatalf("不匹配的绑定观察提前结束了本轮: %+v", d)
		}
	}

	// 模拟 Informer 最终看到原 Pod 绑定到了计划节点。
	bound.Spec.NodeName = "node-a"
	if err := resources.UpdatePod(bound); err != nil {
		t.Fatal(err)
	}
	view.Members[0].Pod = bound
	d := executionDecision(t, b, unit)
	if d.Action != staging.ActionRelease || d.Reason != "AttemptComplete" {
		t.Fatalf("匹配的绑定观察没有生成 Release: %+v", d)
	}
	ctx := context.Background()
	if err := b.Apply(ctx, d); err != nil {
		t.Fatal(err)
	}
	groupContext, err := b.GroupContext(ctx, unit.Key().Group)
	if err != nil || len(groupContext.InFlight) != 0 || len(groupContext.Succeeded) != 1 || unit.State().Phase != workqueue.UnitDone {
		t.Fatalf("超时轮次没有清理并归档: %+v, err=%v", groupContext, err)
	}
	snapshot := resources.Snapshot()
	if snapshot.Nodes["node-a"].GetRequested().GetMilliCPU() != 1000 || len(snapshot.BoundPods) != 1 {
		t.Fatal("Release 清理了已确认绑定的占用或重复计账")
	}
}

func waitBindingFeedback(t *testing.T, reports <-chan staging.Feedback) staging.Feedback {
	t.Helper()
	select {
	case f := <-reports:
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("没有收到绑定结果反馈")
		return staging.Feedback{}
	}
}
