package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	execution "batchscheduler/src/scheduler/executor"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// 使用实际 Executor/后台核查/Buffer/WorkQueue 接线：未知绑定释放后退避，再成功绑定。
// mixed 用例保留 A，只重试 B；包含用户选择的 Atomic 临时恢复语义。
func TestBindingReclamationReturnsAndRetriesThroughRealQueues(t *testing.T) {
	for _, mode := range []workqueue.SubmitMode{workqueue.AllTogether, workqueue.AllowPartial} {
		for _, mixed := range []bool{false, true} {
			name := string(mode) + "/all-unbound"
			if mixed {
				name = string(mode) + "/keep-bound-peer"
			}
			t.Run(name, func(t *testing.T) {
				config := execution.DefaultConfig()
				config.BindRetry.MaxAttempts = 1
				config.BindTimeout = 100 * time.Millisecond
				config.BindingCheckInterval = 10 * time.Millisecond
				client := fake.NewClientset()
				config.Client = client
				b, queues, executor, view, resources := queuedExecutorFixtureWithMode(t, 2, mode, config)
				for _, member := range view.Members {
					if err := client.Tracker().Add(member.Pod.DeepCopy()); err != nil {
						t.Fatal(err)
					}
				}
				var allowBinding atomic.Bool
				var firstCalls atomic.Int32
				firstName := view.Members[0].Pod.Name
				pods := corev1.SchemeGroupVersion.WithResource("pods")
				client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
					binding := action.(clienttesting.CreateAction).GetObject().(*corev1.Binding)
					if binding.Name == firstName {
						firstCalls.Add(1)
					}
					if !allowBinding.Load() && !(mixed && binding.Name == firstName) {
						return true, nil, context.DeadlineExceeded
					}
					obj, err := client.Tracker().Get(pods, binding.Namespace, binding.Name)
					if err != nil {
						return true, nil, err
					}
					pod := obj.(*corev1.Pod).DeepCopy()
					pod.Spec.NodeName = binding.Target.Name
					return true, nil, client.Tracker().Update(pods, pod, pod.Namespace)
				})
				unknown, finished := make(chan staging.Feedback, 1), make(chan staging.Feedback, 2)
				executor.Report = func(ctx context.Context, f staging.Feedback) error {
					if err := b.Report(ctx, f); err != nil {
						return err
					}
					if f.Kind == staging.FeedbackUnknown {
						unknown <- f
					} else if f.Kind == staging.FeedbackFinished {
						finished <- f
					}
					return nil
				}
				ctx := context.Background()
				old := acceptQueuedUnit(t, ctx, queues, b)
				if err := b.Apply(ctx, executionDecision(t, b, old)); err != nil {
					t.Fatal(err)
				}
				waitBindingFeedback(t, unknown)
				if mixed {
					// 模拟 informer 的组视图接住已成功的 A；资源账已经接到 Bind 成功回执。
					view.Members[0].Pod.Spec.NodeName = "node-a"
				}
				f := waitBindingFeedback(t, finished)
				wantRetry, wantCPU := 2, int64(0)
				if mixed {
					wantRetry, wantCPU = 1, 1000
				}
				if len(f.ReleasedBindings) != wantRetry || resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != wantCPU {
					t.Fatalf("释放范围或剩余资源错误: %+v", f)
				}
				d := executionDecision(t, b, old)
				if len(d.Members) != wantRetry || (d.Action != staging.ActionReturn && d.Action != staging.ActionReturnMembers) {
					t.Fatalf("没有组织需要重试的成员: %+v", d)
				}
				if err := b.Apply(ctx, d); err != nil {
					t.Fatal(err)
				}
				if !mixed && mode == workqueue.AllowPartial {
					if err := b.Apply(ctx, executionDecision(t, b, old)); err != nil {
						t.Fatal(err)
					}
				}
				queued := queues.GroupSnapshot(old.Key().Group)
				if len(queued) != 1 || queued[0].State.Queue != workqueue.QueueFail || len(queued[0].Unit.Members()) != wantRetry {
					t.Fatalf("Fail 队列没有接住正确成员: %+v", queued)
				}
				next := queued[0].Unit
				if err := b.Apply(ctx, executionDecision(t, b, next)); err != nil {
					t.Fatal(err)
				}
				if next.State().Queue != workqueue.QueueRetry {
					t.Fatal("确认超时没有转入带退避的 Retry")
				}
				applyAfterRetryDeadline(t, ctx, b, queues, next)
				allowBinding.Store(true)
				ready := acceptQueuedUnit(t, ctx, queues, b)
				// mixed Atomic 的旧 Record 故意不提前 Release，验证它不会挡住重试轮次。
				d = executionDecision(t, b, ready)
				if d.Action != staging.ActionExecute {
					t.Fatalf("重试被等待组成员或旧记录卡住: %+v", d)
				}
				if err := b.Apply(ctx, d); err != nil {
					t.Fatal(err)
				}
				f = waitBindingFeedback(t, finished)
				if f.Phase != staging.PhaseBound || len(f.Plan) != wantRetry {
					t.Fatalf("重试没有成功绑定: %+v", f)
				}
				if mixed && firstCalls.Load() != 1 {
					t.Fatal("已经绑定的 A 被重新提交了 Bind")
				}
				for _, member := range view.Members {
					member.Pod.Spec.NodeName = "node-a"
					if err := resources.UpdatePod(member.Pod); err != nil {
						t.Fatal(err)
					}
				}
				if mixed {
					if err := b.Apply(ctx, executionDecision(t, b, old)); err != nil {
						t.Fatal(err)
					}
				}
				if err := b.Apply(ctx, executionDecision(t, b, ready)); err != nil {
					t.Fatal(err)
				}
				if snapshot := resources.Snapshot(); snapshot.Nodes["node-a"].GetRequested().GetMilliCPU() != 2000 || len(snapshot.BoundPods) != 2 {
					t.Fatal("闭环后实际资源账不正确")
				}
			})
		}
	}
}
