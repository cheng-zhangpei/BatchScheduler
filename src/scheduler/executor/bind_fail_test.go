package executor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/scheduler/steps"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
)

func TestBindFailHandlerClassifiesQueriesAndRetries(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "pod-0", errors.New("denied"))
	busy := apierrors.NewTooManyRequests("busy", 0)
	cases := []struct {
		name       string
		errors     []error
		node       string
		queryError error
		replaced   bool
		binds      int
		gets       int
		reason     string
		settled    bool
	}{
		{name: "throttle-then-success", errors: []error{busy, nil}, binds: 2, gets: 1, settled: true},
		{name: "lost-response-already-bound", errors: []error{context.DeadlineExceeded}, node: "node-a", binds: 1, gets: 1, settled: true},
		{name: "timeout-then-resend-success", errors: []error{context.DeadlineExceeded, nil}, binds: 2, gets: 1, settled: true},
		{name: "permanent-no-retry", errors: []error{forbidden}, binds: 1, reason: workqueue.ReasonForbidden, settled: true},
		{name: "later-rejection-cannot-clear-old-unknown", errors: []error{context.DeadlineExceeded, forbidden}, binds: 2, gets: 1, reason: workqueue.ReasonBindingUnknown},
		{name: "unknown-limit", errors: []error{context.DeadlineExceeded}, binds: 5, gets: 5, reason: workqueue.ReasonBindingUnknown},
		{name: "throttle-limit-can-recover", errors: []error{busy}, binds: 5, gets: 5, reason: workqueue.ReasonBindingRejected, settled: true},
		{name: "query-fails-no-blind-resend", errors: []error{context.DeadlineExceeded}, queryError: errors.New("connection lost"), binds: 1, gets: 5, reason: workqueue.ReasonBindingUnknown},
		{name: "pod-deleted", errors: []error{context.DeadlineExceeded}, queryError: apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "pod-0"), binds: 1, gets: 1, reason: workqueue.ReasonObjectDeleted, settled: true},
		{name: "pod-replaced", errors: []error{context.DeadlineExceeded}, replaced: true, binds: 1, gets: 1, reason: workqueue.ReasonObjectDeleted, settled: true},
		{name: "bound-elsewhere", errors: []error{context.DeadlineExceeded}, node: "node-other", binds: 1, gets: 1, reason: workqueue.ReasonBindingUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, a, resources, pod := bindRetryFixture(t)
			pod.Spec.NodeName = tc.node
			if tc.replaced {
				pod.UID = "replacement"
			}
			client := fake.NewClientset(pod)
			binds, gets := 0, 0
			client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
				gets++
				return tc.queryError != nil, nil, tc.queryError
			})
			client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				binding := action.(clienttesting.CreateAction).GetObject().(*corev1.Binding)
				ref := a.AssumedMembers[0]
				if binding.UID != ref.UID || binding.Target.Name != a.Plan[ref] {
					t.Fatal("补发更换了原 UID 或计划节点")
				}
				index := min(binds, len(tc.errors)-1)
				binds++
				return true, nil, tc.errors[index]
			})
			e.steps.Client = client
			var last staging.Feedback
			e.Report = func(_ context.Context, f staging.Feedback) error { last = f; return nil }
			if err := e.runBinding(context.Background(), a); err != nil {
				t.Fatal(err)
			}
			if binds != tc.binds || gets != tc.gets || last.ResponsibilitiesSettled != tc.settled {
				t.Fatalf("calls bind/get=%d/%d, feedback=%+v", binds, gets, last)
			}
			if tc.reason == "" {
				if last.Phase != staging.PhaseBound || last.Failure != nil || len(a.BoundMembers) != 1 {
					t.Fatalf("成功未交回正常绑定路径: %+v", last)
				}
			} else if last.Failure == nil || last.Failure.Reason != tc.reason {
				t.Fatalf("错误分类不符: %+v", last)
			}
			wantCPU := int64(1000)
			if tc.reason != "" && tc.settled {
				wantCPU = 0
			}
			if cpu := resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU(); cpu != wantCPU {
				t.Fatalf("暂占错误: got=%d, want=%d", cpu, wantCPU)
			}
		})
	}
}

// 使用真实 HTTP 客户端验证短请求超时、整轮总预算和取消，不依赖 fake client 响应 Context。
func TestBindRetryHonorsRequestAndRoundDeadlines(t *testing.T) {
	e, a, resources, pod := bindRetryFixture(t)
	e.steps.BindTimeout = 120 * time.Millisecond
	e.steps.BindRequestTimeout = 35 * time.Millisecond
	e.bindFailures.policy.RequestTimeout = 35 * time.Millisecond
	var posts atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(pod)
			return
		}
		posts.Add(1)
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
	var last staging.Feedback
	e.Report = func(ctx context.Context, f staging.Feedback) error {
		if ctx.Err() != nil {
			t.Fatal("局部绑定超时取消了 Report 使用的外层 Context")
		}
		last = f
		return nil
	}
	started := time.Now()
	if err := e.runBinding(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("重试刷新了整轮预算: %s", elapsed)
	}
	// 期限优先于次数；预算很短或机器忙时，允许第一轮之后就结束，不能强求发满。
	if posts.Load() < 1 || posts.Load() > 5 || last.Kind != staging.FeedbackUnknown || last.ResponsibilitiesSettled {
		t.Fatalf("超时处理不符: posts=%d, feedback=%+v", posts.Load(), last)
	}
	if resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 1000 {
		t.Fatal("总预算耗尽后擅自释放未知暂占")
	}
}

func TestBindBackoffStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitBindBackoff(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消没有结束退避: %v", err)
	}
}

// A 已绑定；B 的响应丢失但查询确认成功。A 不能重绑，整轮应正常报告成功。
func TestBindLookupConfirmationContinuesGroupWithoutRebindingPeers(t *testing.T) {
	e, a, resources, pod := bindRetryFixture(t)
	peer := pod.DeepCopy()
	peer.Name, peer.UID = "peer", "peer"
	ref := workqueue.PodRef{Name: peer.Name, UID: peer.UID}
	a.Request.Members = append(a.Request.Members, ref)
	a.AssumedMembers = append(a.AssumedMembers, ref)
	a.Plan[ref] = "node-a"
	peer.Spec.NodeName = "node-a"
	if err := resources.Assume([]resourcecache.Assumption{{Unit: a.Request.Unit, ActionID: a.Request.ActionID, Pod: peer}}); err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientset(peer)
	calls := make(map[string]int)
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		binding := action.(clienttesting.CreateAction).GetObject().(*corev1.Binding)
		calls[binding.Name]++
		if binding.Name == peer.Name {
			return true, nil, context.DeadlineExceeded
		}
		return true, nil, nil
	})
	e.steps.Client = client
	var last staging.Feedback
	e.Report = func(_ context.Context, f staging.Feedback) error { last = f; return nil }
	if err := e.runBinding(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if last.Phase != staging.PhaseBound || len(a.BoundMembers) != 2 || calls[pod.Name] != 1 || calls[peer.Name] != 1 {
		t.Fatalf("查询补确认后没有保持组进度: calls=%v, feedback=%+v", calls, last)
	}
}

func bindRetryFixture(t *testing.T) (*Executor, *executionAttempt, *resourcecache.Cache, *corev1.Pod) {
	t.Helper()
	e, resources, requests := asyncBindingFixture(1, "4")
	e.bindFailures.policy.InitialBackoff = time.Millisecond
	e.bindFailures.policy.MaxBackoff = 4 * time.Millisecond
	request := requests[0]
	ref := request.Members[0]
	pod := &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Namespace: request.Unit.Group.Namespace, Name: ref.Name, UID: ref.UID},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
		}}}}}
	assumed := pod.DeepCopy()
	assumed.Spec.NodeName = "node-a"
	if err := resources.Assume([]resourcecache.Assumption{{Unit: request.Unit, ActionID: request.ActionID, Pod: assumed}}); err != nil {
		t.Fatal(err)
	}
	a := &executionAttempt{Attempt: steps.Attempt{Request: request, Plan: map[workqueue.PodRef]string{ref: "node-a"},
		Reserved: true, AssumedMembers: []workqueue.PodRef{ref}}}
	return e, a, resources, pod
}
