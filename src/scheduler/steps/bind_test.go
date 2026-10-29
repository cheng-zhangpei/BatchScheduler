package steps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
)

// 经过真实 client-go HTTP 客户端，检查 namespace/name/UID/Node 都沿 Plan 正确提交。
func TestBindPostsPodBindingSubresource(t *testing.T) {
	r, a := assumeStepFixture()
	if err := r.Assume(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	received := make(chan *corev1.Binding, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var binding corev1.Binding
		if err := json.NewDecoder(req.Body).Decode(&binding); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Method != http.MethodPost || req.URL.Path != fmt.Sprintf("/api/v1/namespaces/work/pods/%s/binding", binding.Name) {
			http.Error(w, "wrong endpoint", http.StatusBadRequest)
			return
		}
		received <- &binding
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Success","code":201}`))
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL, ContentConfig: rest.ContentConfig{ContentType: "application/json"}})
	if err != nil {
		t.Fatal(err)
	}
	r.Client = client
	if err := r.Bind(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	for _, ref := range a.Request.Members {
		binding := <-received
		if binding.Namespace != "work" || binding.Name != ref.Name || binding.UID != ref.UID || binding.Target.Kind != "Node" || binding.Target.Name != a.Plan[ref] {
			t.Fatalf("绑定请求与原计划不一致: %+v", binding)
		}
	}
	if a.Binding || len(a.BoundMembers) != 2 || r.resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 4000 {
		t.Fatal("成功绑定后未记录成员，或在 informer 确认前扣掉了占用")
	}
	if a.members[0].Spec.NodeName != "" {
		t.Fatal("Bind 修改了原观察对象")
	}
}

func TestBindErrorsMapToRecoveryRules(t *testing.T) {
	pods := schema.GroupResource{Resource: "pods"}
	cases := []struct {
		name    string
		err     error
		reason  string
		unknown bool
	}{
		{"forbidden", apierrors.NewForbidden(pods, "a", errors.New("denied")), workqueue.ReasonForbidden, false},
		{"invalid", apierrors.NewBadRequest("invalid binding"), workqueue.ReasonInvalidSpec, false},
		{"deleted", apierrors.NewNotFound(pods, "a"), workqueue.ReasonObjectDeleted, false},
		{"throttled", apierrors.NewTooManyRequests("busy", 1), workqueue.ReasonBindingRejected, false},
		{"conflict", apierrors.NewConflict(pods, "a", errors.New("already assigned")), workqueue.ReasonBindingUnknown, true},
		{"timeout", context.DeadlineExceeded, workqueue.ReasonBindingUnknown, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, a := assumeStepFixture()
			if err := r.Assume(context.Background(), a); err != nil {
				t.Fatal(err)
			}
			client := fake.NewClientset()
			calls := 0
			client.PrependReactor("create", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
				calls++
				return true, nil, tc.err
			})
			r.Client = client
			err := r.Bind(context.Background(), a)
			var failure *BindingError
			if !errors.As(err, &failure) || failure.Reason != tc.reason || !errors.Is(err, tc.err) || a.Binding != tc.unknown || calls != 1 {
				t.Fatalf("错误分类或提交次数不正确: err=%v, binding=%v, calls=%d", err, a.Binding, calls)
			}
			err = r.Recover(context.Background(), a)
			if tc.unknown {
				if !errors.Is(err, ErrBindingResultUnknown) || !a.Reserved {
					t.Fatalf("未知结果被退账: %v", err)
				}
			} else if err != nil || a.Reserved || r.resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 0 {
				t.Fatalf("明确拒绝后没有退账: %v", err)
			}
		})
	}
}

func TestPartialBindFailureDoesNotPretendWholeAttemptWasRolledBack(t *testing.T) {
	r, a := assumeStepFixture()
	if err := r.Assume(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientset()
	calls := 0
	client.PrependReactor("create", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 1 {
			return true, nil, nil
		}
		return true, nil, apierrors.NewTooManyRequests("busy", 1)
	})
	r.Client = client
	if err := r.Bind(context.Background(), a); err == nil || len(a.BoundMembers) != 1 || a.Binding {
		t.Fatalf("部分成功没有记录: %v", err)
	}
	if err := r.Recover(context.Background(), a); !errors.Is(err, staging.ErrHandlerUnavailable) || !a.Reserved {
		t.Fatalf("部分绑定不能仅 Forget 就报告回滚完成: %v", err)
	}
}

func TestBindTimeoutKeepsUnknownReservation(t *testing.T) {
	r, a := assumeStepFixture()
	if err := r.Assume(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		select {
		case <-req.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	r.Client, r.BindTimeout = client, 50*time.Millisecond
	err = r.Bind(context.Background(), a)
	var failure *BindingError
	if !errors.As(err, &failure) || failure.Reason != workqueue.ReasonBindingUnknown || !a.Binding {
		t.Fatalf("超时没有保留未知结果: %v", err)
	}
	if err := r.Recover(context.Background(), a); !errors.Is(err, ErrBindingResultUnknown) || !a.Reserved {
		t.Fatalf("超时后擅自退账: %v", err)
	}
}
