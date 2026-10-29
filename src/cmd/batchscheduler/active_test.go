package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"batchscheduler/src/group"
	execution "batchscheduler/src/scheduler/executor"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// 只入队并启动消费、评估线程，不手动 Pop、Accept、Observe 或 Apply 单个任务。
// 验证 Active 自动进入 Buffer，再经已有执行线发送 Binding 并回报成功。
func TestActiveConsumerAutomaticallyDrivesBinding(t *testing.T) {
	config := execution.DefaultConfig()
	client := fake.NewClientset()
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "binding" {
			return true, nil, nil
		}
		return false, nil, nil
	})
	config.Client = client
	b, queues, executor, view, resources := queuedExecutorFixture(t, config)
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
	ctx, stop := context.WithCancel(context.Background())
	activeDone, evaluationDone := make(chan error, 1), make(chan error, 1)
	go func() {
		evaluationDone <- b.RunEvaluations(ctx, 32, func(ctx context.Context, decisions []staging.Decision) error {
			for _, decision := range decisions {
				if err := b.Apply(ctx, decision); err != nil && ctx.Err() == nil && !errors.Is(err, staging.ErrStaleDecision) {
					t.Errorf("自动评估后的 Apply 失败: %v", err)
					return err
				}
			}
			return nil
		})
	}()
	go func() { activeDone <- runActive(ctx, queues, b) }()
	t.Cleanup(func() {
		stop()
		awaitActiveWorker(t, activeDone)
		awaitActiveWorker(t, evaluationDone)
	})
	f := waitBindingFeedback(t, finished)
	if f.Phase != staging.PhaseBound || f.Failure != nil || !f.ResponsibilitiesSettled {
		t.Fatalf("Active 没有自动走完执行线: %+v", f)
	}
	key := group.Key{Namespace: view.Group.Namespace, Name: view.Group.Name, UID: view.Group.UID}
	if !b.ContainsPod(key, view.Members[0].Pod.UID) {
		t.Fatal("Buffer 没有接住成员归属")
	}
	if resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 1000 {
		t.Fatal("绑定确认通知到来前丢失了占用")
	}
	if len(client.Actions()) != 1 || client.Actions()[0].GetSubresource() != "binding" {
		t.Fatal("没有准确发送一次绑定请求")
	}
}

func TestActiveConsumerReturnsRejectedUnitToQueue(t *testing.T) {
	b, queues, _, view, _ := queuedExecutorFixture(t)
	// 排队之后声明发生变化，旧 Unit 应被 Accept 拒绝，不能继续带旧配置执行。
	view.Group.Generation++
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runActive(ctx, queues, b); err == nil {
		t.Fatal("Accept 拒绝后应该上报错误，而不是丢掉任务或无限重试")
	}
	unit, err := queues.Active.Pop(ctx)
	if err != nil {
		t.Fatalf("被拒绝的 Unit 没有放回 Active: %v", err)
	}
	if state := unit.State(); state.Phase != workqueue.UnitQueued || state.Queue != workqueue.QueueActive {
		t.Fatalf("接收失败却改变了执行归属: %+v", state)
	}
	if b.ContainsPod(unit.Key().Group, unit.Members()[0].UID) {
		t.Fatal("Accept 拒绝后仍留下了 Buffer 成员归属")
	}
}

type cancelAfterPop struct {
	workqueue.ActiveQueue
	stop context.CancelFunc
}

func (q cancelAfterPop) Pop(ctx context.Context) (workqueue.Unit, error) {
	unit, err := q.ActiveQueue.Pop(ctx)
	if err == nil {
		q.stop() // 模拟刚出队、尚未进入 Accept 时程序退出。
	}
	return unit, err
}

func TestActiveConsumerCancellationAfterPopDoesNotLoseUnit(t *testing.T) {
	b, queues, _, _, _ := queuedExecutorFixture(t)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	original := queues.Active
	queues.Active = cancelAfterPop{ActiveQueue: original, stop: stop}
	if err := runActive(ctx, queues, b); err != nil {
		t.Fatalf("取消消费不应当作业务错误: %v", err)
	}
	checkCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	unit, err := original.Pop(checkCtx)
	if err != nil || unit.State().Phase != workqueue.UnitQueued {
		t.Fatalf("退出发生在 Pop 和 Accept 之间时丢失任务: %v", err)
	}
	if b.ContainsPod(unit.Key().Group, unit.Members()[0].UID) {
		t.Fatal("取消后仍登记了执行归属")
	}
}

func awaitActiveWorker(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Active 消费或评估线程没有退出")
	}
}
