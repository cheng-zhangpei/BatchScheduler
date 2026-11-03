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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// 模拟执行器进展，使用 main 的真实 Handoff 接线、Select、Transition 和 Buffer。
// A、B 在执行，C、D 一起回送；新轮次可重试，旧轮次仍能接收反馈并单独结束。
func TestPartialReturnKeepsOriginalExecutionAndBatchesWaitingMembers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := group.Key{Namespace: "work", Name: "job", UID: "group"}
	view := &group.View{Group: &scheduling.PodGroup{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: key.UID},
		Spec: scheduling.PodGroupSpec{Members: 4, SubmitMode: string(workqueue.AllowPartial)}}}
	controller := true
	for _, name := range []string{"a", "b", "c", "d"} {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: name, UID: types.UID(name),
			OwnerReferences: []metav1.OwnerReference{{APIVersion: scheduling.GroupVersion.String(), Kind: "PodGroup", Name: key.Name, UID: key.UID, Controller: &controller}},
		}, Status: corev1.PodStatus{Phase: corev1.PodPending}}
		view.Members = append(view.Members, group.PodView{Pod: pod, Group: key})
	}
	views := executorTestViews{view}
	w := workqueue.New()
	w.Views = views
	store, err := history.Open(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	nodes := corelisters.NewNodeLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}))
	handlers := newStagingHandlers(w, nodes, &execution.Executor{})
	var request staging.Execution
	handlers.Execute = func(_ context.Context, r staging.Execution) error { request = r; return nil }
	realHandoff := handlers.Handoff
	var b *buffer.Buffer
	var progress staging.Feedback
	handoffs := 0
	handlers.Handoff = func(ctx context.Context, h staging.Handoff) error {
		if h.Decision.Action == staging.ActionReturnMembers {
			handoffs++
			// 交接期间 A、B 的反馈仍能到达；不能用交接编号替换执行的 ActionID。
			progress.Phase = staging.PhaseBinding
			if err := b.Report(ctx, progress); err != nil {
				return err
			}
			if d := executionDecision(t, b, h.Unit); d.Action != staging.ActionWait || d.Reason != "ReturningMembers" {
				t.Fatalf("交接未完成时不应结束或重复交接: %+v", d)
			}
		}
		return realHandoff(ctx, h)
	}
	b = buffer.NewBuffer(views, store, handlers)
	w.Staging = b
	if err := w.Select(workqueue.SelectRequest{View: view}); err != nil {
		t.Fatal(err)
	}
	old, err := w.Active.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Accept(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := b.Apply(ctx, executionDecision(t, b, old)); err != nil {
		t.Fatal(err)
	}
	refs := old.Members()
	plan := make(map[workqueue.PodRef]string)
	for _, ref := range refs {
		plan[ref] = "node-a"
	}
	progress = staging.Feedback{Unit: old.Key(), ActionID: request.ActionID, Operation: request.Operation,
		Kind: staging.FeedbackProgress, Phase: staging.PhaseAssumed, Plan: plan,
		Resources: &staging.ResourceResult{Assumed: refs[:2]}}
	for _, ref := range refs[2:] {
		progress.Resources.Unplaced = append(progress.Resources.Unplaced, staging.UnplacedMember{Pod: ref,
			Failure: group.FailureInfo{Reason: workqueue.ReasonInsufficientResources, Message: "本轮容量不足"}})
	}
	if err := b.Report(ctx, progress); err != nil {
		t.Fatal(err)
	}
	d := executionDecision(t, b, old)
	if d.Action != staging.ActionReturnMembers || d.To != workqueue.QueueRetry || len(d.Members) != 2 {
		t.Fatalf("C、D 应一起回送 Retry: %+v", d)
	}
	if err := b.Apply(ctx, d); err != nil {
		t.Fatal(err)
	}
	queued := w.GroupSnapshot(key)
	if len(queued) != 1 || queued[0].State.Queue != workqueue.QueueRetry || len(queued[0].Unit.Members()) != 2 ||
		queued[0].Retry.Plan.Condition != workqueue.RetryOnResources {
		t.Fatalf("C、D 应直接进入 Retry 并等待资源变化: %+v", queued)
	}
	next := queued[0].Unit
	for i, ref := range refs {
		if b.ContainsPod(key, ref.UID) != (i < 2) {
			t.Fatalf("交接后的成员归属不正确: %s", ref.Name)
		}
	}
	if old.State().Phase != workqueue.UnitProcessing || len(old.Members()) != 4 {
		t.Fatal("部分交接结束了旧 Unit 或修改了冻结名单")
	}
	if err := b.Apply(ctx, d); !errors.Is(err, staging.ErrStaleDecision) || handoffs != 1 || len(w.GroupSnapshot(key)) != 1 {
		t.Fatalf("旧决定重放重复创建了新 Unit: %v", err)
	}
	// 相同执行继续报告原始名单，也不能把 C、D 再次回送。
	if err := b.Report(ctx, progress); err != nil {
		t.Fatal(err)
	}
	if got := executionDecision(t, b, old); got.Action != staging.ActionWait {
		t.Fatalf("A、B 应继续等待绑定，C、D 不能重复回送: %+v", got)
	}
	// 直接进入 Retry 也需要等待相关事件，不能在下一次评估中立即重新调度。
	decisions, err := b.Evaluate(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, decision := range decisions {
		if decision.Action == staging.ActionTransition && decision.Unit != nil && *decision.Unit == next.Key() {
			t.Fatal("资源没有变化时不应唤醒 C、D")
		}
	}
	b.NotifyRetry(workqueue.RetryOnResources)
	move := executionDecision(t, b, next)
	if move.Action != staging.ActionTransition || move.Transition.To != workqueue.QueueActive {
		t.Fatalf("资源变化后应重新进入 Active: %+v", move)
	}
	if err := b.Apply(ctx, move); err != nil {
		t.Fatal(err)
	}
	ready, err := w.Active.Pop(ctx)
	if err != nil || ready.Key() != next.Key() {
		t.Fatalf("没有取到 C、D 的重试单元: %v", err)
	}
	if err := b.Accept(ctx, ready); err != nil {
		t.Fatalf("旧 Record 不应阻挡已交出成员的新轮次: %v", err)
	}
	// A、B 独立完成，归档和清理都只处理旧轮次仍负责的成员。
	for _, member := range view.Members[:2] {
		member.Pod.Spec.NodeName = "node-a"
	}
	progress.Kind, progress.Phase, progress.ResponsibilitiesSettled = staging.FeedbackFinished, staging.PhaseBound, true
	if err := b.Report(ctx, progress); err != nil {
		t.Fatal(err)
	}
	finish := executionDecision(t, b, old)
	if finish.Action != staging.ActionRelease {
		t.Fatalf("C、D 已交出，不应阻挡 A、B 完成: %+v", finish)
	}
	if err := b.Apply(ctx, finish); err != nil {
		t.Fatal(err)
	}
	groupContext, err := b.GroupContext(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if old.State().Phase != workqueue.UnitDone || len(groupContext.Succeeded) != 1 || len(groupContext.Succeeded[0].Members) != 2 ||
		len(groupContext.InFlight) != 1 || groupContext.InFlight[0].Unit != next.Key() || len(groupContext.InFlight[0].Members) != 2 {
		t.Fatalf("旧轮次结束影响了新轮次，或把 C、D 错归档为成功: %+v", groupContext)
	}
	for _, ref := range refs[2:] {
		if !b.ContainsPod(key, ref.UID) {
			t.Fatal("旧轮次清理误删了新轮次的成员归属")
		}
	}
}
