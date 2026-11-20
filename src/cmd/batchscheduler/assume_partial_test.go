package main

import (
	"context"
	"sync"
	"testing"
	"time"

	execution "batchscheduler/src/scheduler/executor"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// 让 Filter/Plan 看到充足资源，Assume 前再加入其他 Pod 占用。
// Partial 要边绑定成功成员边回送其余成员；Atomic 仍须整批失败且没有新增暂占。
func TestAssumeCapacityChangeFlowsThroughBindingAndRetry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    workqueue.SubmitMode
		members int
		shrink  staging.ExecutionPhase
	}{
		{"partial-after-full-plan", workqueue.AllowPartial, 3, staging.PhasePlanned},
		{"partial-merge-earlier-unplaced", workqueue.AllowPartial, 5, staging.PhaseScored},
		{"atomic-after-full-plan", workqueue.AllTogether, 3, staging.PhasePlanned},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			client := fake.NewClientset()
			bindings := make(chan *corev1.Binding, 8)
			bindGate := make(chan struct{})
			releaseBind := sync.OnceFunc(func() { close(bindGate) })
			defer releaseBind()
			client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "binding" {
					return false, nil, nil
				}
				bindings <- action.(clienttesting.CreateAction).GetObject().(*corev1.Binding).DeepCopy()
				select {
				case <-bindGate:
					return true, nil, nil
				case <-ctx.Done():
					return true, nil, ctx.Err()
				}
			})
			config := execution.DefaultConfig()
			config.Client = client
			b, queues, executor, _, resources := queuedExecutorFixtureWithMode(t, tc.members, tc.mode, config)
			occupier := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "occupier", UID: "occupier"},
				Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")},
				}}}}}
			changed := false
			finished := make(chan staging.Feedback, 4)
			executor.Report = func(ctx context.Context, f staging.Feedback) error {
				if err := b.Report(ctx, f); err != nil {
					return err
				}
				if f.Phase == tc.shrink && !changed {
					changed = true
					if err := resources.UpdatePod(occupier); err != nil {
						return err
					}
				}
				if f.Kind == staging.FeedbackFinished {
					finished <- f
				}
				return nil
			}
			old := acceptQueuedUnit(t, ctx, queues, b)
			if err := b.Apply(ctx, executionDecision(t, b, old)); err != nil {
				t.Fatal(err)
			}
			if tc.mode == workqueue.AllTogether {
				f := waitBindingFeedback(t, finished)
				if f.Phase != staging.PhaseRecovered || f.Failure == nil || len(client.Actions()) != 0 || resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 3000 {
					t.Fatalf("Atomic 应整批拒绝且不绑定、不残留暂占: %+v", f)
				}
				return
			}

			// 真正 Bind 已经开始，但故意不让它返回。未放置成员此时就应能独立回队列。
			select {
			case binding := <-bindings:
				if binding.UID != old.Members()[0].UID {
					t.Fatal("绑定请求不是实际接纳的第一个成员")
				}
			case <-ctx.Done():
				t.Fatal("Partial 没有把实际接纳成员提交到绑定 Worker")
			}
			d := executionDecision(t, b, old)
			if d.Action != staging.ActionReturnMembers || d.To != workqueue.QueueRetry || len(d.Members) != tc.members-1 {
				t.Fatalf("没有合并并回送本轮全部未接纳成员: %+v", d)
			}
			if err := b.Apply(ctx, d); err != nil {
				t.Fatal(err)
			}
			queued := queues.GroupSnapshot(old.Key().Group)
			if len(queued) != 1 || queued[0].State.Queue != workqueue.QueueRetry || len(queued[0].Unit.Members()) != tc.members-1 {
				t.Fatal("未放置成员没有组成新的 Retry Unit")
			}
			for i, ref := range old.Members() {
				if b.ContainsPod(old.Key().Group, ref.UID) != (i == 0) {
					t.Fatal("回送影响了成功成员，或未解除失败成员的旧归属")
				}
			}
			releaseBind()
			f := waitBindingFeedback(t, finished)
			if f.Phase != staging.PhaseBound || len(f.Plan) != 1 || f.Resources == nil || len(f.Resources.Assumed) != 1 || len(f.Resources.Unplaced) != tc.members-1 || len(client.Actions()) != 1 {
				t.Fatalf("成功部分未独立完成，或错误绑定了未接纳成员: %+v", f)
			}
			if tc.members != 3 {
				return
			}
			// 资源恢复后，剩下两个成员仍通过真实的 Retry → Active → Assume → Bind 完成。
			if err := resources.RemovePod(occupier); err != nil {
				t.Fatal(err)
			}
			next := queued[0].Unit
			b.NotifyRetry(workqueue.RetryOnResources)
			if err := b.Apply(ctx, executionDecision(t, b, next)); err != nil {
				t.Fatal(err)
			}
			applyAfterRetryDeadline(t, ctx, b, queues, next)
			ready := acceptQueuedUnit(t, ctx, queues, b)
			if err := b.Apply(ctx, executionDecision(t, b, ready)); err != nil {
				t.Fatal(err)
			}
			if f := waitBindingFeedback(t, finished); f.Phase != staging.PhaseBound || len(f.Plan) != 2 || len(client.Actions()) != 3 {
				t.Fatalf("剩余成员资源恢复后没有完成重试绑定: %+v", f)
			}
		})
	}
}
