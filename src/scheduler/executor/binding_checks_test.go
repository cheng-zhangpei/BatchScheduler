package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"batchscheduler/src/staging"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestUnknownBindingEntersChecksAndReportsConfirmation(t *testing.T) {
	for _, reportErr := range []error{nil, errors.New("report unavailable"), staging.ErrFeedbackObsolete} {
		t.Run(stringOrSuccess(reportErr), func(t *testing.T) {
			e, a, resources, pod := bindRetryFixture(t)
			e.bindFailures.policy.MaxAttempts = 1
			client := fake.NewClientset(pod)
			client.PrependReactor("create", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, context.DeadlineExceeded
			})
			e.steps.Client = client
			var last staging.Feedback
			e.Report = func(_ context.Context, f staging.Feedback) error { last = f; return nil }
			if err := e.runBinding(context.Background(), a); err != nil {
				t.Fatal(err)
			}
			if last.Kind != staging.FeedbackUnknown || len(resources.BindingChecks()) != 1 {
				t.Fatal("未知反馈后没有登记核查")
			}
			if !resources.BindingChecks()[0].Deadline.Equal(a.BindingDeadline) || resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 1000 {
				t.Fatal("转后台重置了原始截止时间，或次数耗尽就提前释放了资源")
			}
			pod.Spec.NodeName = "node-a"
			if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace); err != nil {
				t.Fatal(err)
			}
			if err := resources.UpdatePod(pod); err != nil {
				t.Fatal(err)
			}
			e.Report = func(_ context.Context, f staging.Feedback) error { last = f; return reportErr }
			err := e.checkBindings(context.Background())
			if reportErr != nil && !errors.Is(reportErr, staging.ErrFeedbackObsolete) {
				if !errors.Is(err, reportErr) || len(resources.BindingChecks()) != 1 {
					t.Fatal("回报失败丢掉了待确认记录")
				}
				e.Report = func(_ context.Context, f staging.Feedback) error { last = f; return nil }
				if err := e.checkBindings(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if last.Kind != staging.FeedbackFinished || last.Phase != staging.PhaseBound || last.ActionID != a.Request.ActionID || len(resources.BindingChecks()) != 0 {
				t.Fatalf("最终确认没有完成: %+v", last)
			}
			snapshot := resources.Snapshot()
			if len(snapshot.BoundPods) != 1 || snapshot.Nodes["node-a"].GetRequested().GetMilliCPU() != 1000 {
				t.Fatal("主动确认未更新实际占用，或重复记账")
			}
		})
	}
}

func TestExpiredBindingForgetsWithoutWaitingForAnotherQuery(t *testing.T) {
	e, a, resources, pod := bindRetryFixture(t)
	if err := resources.TrackBindingChecks(a.Request.Unit, a.Request.ActionID, a.Plan, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientset(pod)
	queries := 0
	client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		queries++
		return true, nil, errors.New("API unavailable")
	})
	e.steps.Client = client
	var last staging.Feedback
	e.Report = func(_ context.Context, f staging.Feedback) error { last = f; return nil }
	if err := e.checkBindings(context.Background()); err != nil {
		t.Fatal(err)
	}
	if queries != 0 || resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 0 || last.Phase != staging.PhaseRecovered {
		t.Fatalf("到期后仍等待查询或没有回收: queries=%d, feedback=%+v", queries, last)
	}
}

func TestWaitingForOriginalDeadlineDoesNotSendMoreQueries(t *testing.T) {
	e, a, resources, pod := bindRetryFixture(t)
	deadline := time.Now().Add(time.Hour)
	if err := resources.TrackBindingChecks(a.Request.Unit, a.Request.ActionID, a.Plan, deadline); err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientset(pod)
	client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		t.Error("后台增加了额外 GET 流程")
		return true, nil, errors.New("connection lost")
	})
	e.steps.Client = client
	if err := e.checkBindings(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !resources.BindingChecks()[0].Deadline.Equal(deadline) || resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 1000 {
		t.Fatal("查询失败延长了截止时间，或提前释放了暂占")
	}
}

func TestExecutorStopsBindingDeadlineMonitorOnExit(t *testing.T) {
	e, a, resources, _ := bindRetryFixture(t)
	e.config.BindingCheckInterval = time.Millisecond
	if err := resources.TrackBindingChecks(a.Request.Unit, a.Request.ActionID, a.Plan, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	stop, done := startAsyncExecutor(t, e)
	stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("截止时间监控没有随 Executor 取消退出")
	}
}

func stringOrSuccess(err error) string {
	if err == nil {
		return "success"
	}
	return err.Error()
}

func TestReclaimedBindingReportRetriesWithoutRepeatingForget(t *testing.T) {
	for _, lateBinding := range []bool{false, true} {
		e, a, resources, pod := bindRetryFixture(t)
		e.config.BindingCheckInterval = time.Nanosecond
		if err := resources.TrackBindingChecks(a.Request.Unit, a.Request.ActionID, a.Plan, time.Now().Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		client := fake.NewClientset(pod)
		e.steps.Client = client
		failure := errors.New("report temporarily unavailable")
		e.Report = func(context.Context, staging.Feedback) error { return failure }
		if err := e.checkBindings(context.Background()); !errors.Is(err, failure) {
			t.Fatalf("没有执行回收后的反馈: %v", err)
		}
		if resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 0 || len(resources.BindingChecks()) != 1 {
			t.Fatal("回报失败时没有保留待交付记录，或仍占用资源")
		}
		if lateBinding {
			pod.Spec.NodeName = "node-a"
			if err := resources.UpdatePod(pod); err != nil {
				t.Fatal(err)
			}
		}
		var last staging.Feedback
		e.Report = func(_ context.Context, f staging.Feedback) error { last = f; return nil }
		if err := e.checkBindings(context.Background()); err != nil {
			t.Fatal(err)
		}
		wantCPU, wantPhase := int64(0), staging.PhaseRecovered
		if lateBinding {
			wantCPU, wantPhase = 1000, staging.PhaseBound
		}
		if len(resources.BindingChecks()) != 0 || last.Phase != wantPhase || resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != wantCPU {
			t.Fatalf("反馈重试覆盖了晚到绑定，或重复扣账: %+v", last)
		}
	}
}
