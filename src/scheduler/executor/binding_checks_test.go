package executor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"batchscheduler/src/staging"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
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
			pod.Spec.NodeName = "node-a"
			if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace); err != nil {
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

func TestBindingChecksDoNotCountFailedQueries(t *testing.T) {
	e, a, resources, pod := bindRetryFixture(t)
	if err := resources.TrackBindingChecks(a.Request.Unit, a.Request.ActionID, a.Plan, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientset(pod)
	client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection lost")
	})
	e.steps.Client = client
	if err := e.checkBindings(context.Background()); err == nil {
		t.Fatal("测试没有制造查询失败")
	}
	if resources.BindingChecks()[0].UnboundChecks != 0 {
		t.Fatal("查询失败被算成未绑定")
	}
}

func TestExecutorCancelsBackgroundBindingQueryOnExit(t *testing.T) {
	e, a, resources, _ := bindRetryFixture(t)
	e.config.BindingCheckInterval = time.Millisecond
	if err := resources.TrackBindingChecks(a.Request.Unit, a.Request.ActionID, a.Plan, time.Now()); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	e.steps.Client = client
	stop, done := startAsyncExecutor(t, e)
	awaitBindingEvent(t, entered)
	stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("后台核查的 GET 没随 Executor 取消退出")
	}
}

func stringOrSuccess(err error) string {
	if err == nil {
		return "success"
	}
	return err.Error()
}
