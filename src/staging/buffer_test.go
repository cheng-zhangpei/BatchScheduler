package staging

import (
	"context"
	"fmt"
	"sync"
	"testing"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type testViews struct{ view *group.View }

func (v testViews) Get(string, string) (*group.View, error) { return v.view, nil }

func fixture(mode workqueue.SubmitMode) (*Buffer, *group.View) {
	key := group.Key{Namespace: "work", Name: "job", UID: "group-uid"}
	view := &group.View{Group: &scheduling.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: key.UID},
		Spec:       scheduling.PodGroupSpec{Members: 2, SubmitMode: string(mode)},
	}}
	controller := true
	for i := 0; i < 2; i++ {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: key.Namespace, Name: fmt.Sprintf("pod-%d", i), UID: types.UID(fmt.Sprintf("uid-%d", i)),
			OwnerReferences: []metav1.OwnerReference{{APIVersion: scheduling.GroupVersion.String(), Kind: "PodGroup", Name: key.Name, UID: key.UID, Controller: &controller}},
		}, Status: corev1.PodStatus{Phase: corev1.PodPending}}
		view.Members = append(view.Members, group.PodView{Group: key, Pod: pod})
	}
	return NewBuffer(testViews{view}), view
}

func queuedUnit(t *testing.T, view *group.View, id string, mode workqueue.SubmitMode, indices ...int) workqueue.Unit {
	t.Helper()
	u := workqueue.NewUnit(workqueue.UnitKey{Group: view.Members[0].Group, ID: id}, mode)
	for _, i := range indices {
		pod := view.Members[i].Pod
		if err := u.AddMember(workqueue.PodRef{Name: pod.Name, UID: pod.UID}); err != nil {
			t.Fatal(err)
		}
	}
	if err := u.Freeze(); err != nil {
		t.Fatal(err)
	}
	if err := u.MarkQueued(workqueue.QueueActive, nil, ""); err != nil {
		t.Fatal(err)
	}
	return u
}

func TestAcceptPartialLinksSeparateUnits(t *testing.T) {
	b, view := fixture(workqueue.AllowPartial)
	first := queuedUnit(t, view, "first", workqueue.AllowPartial, 0)
	second := queuedUnit(t, view, "second", workqueue.AllowPartial, 1)
	for _, u := range []workqueue.Unit{first, second, first} {
		if err := b.Accept(context.Background(), u); err != nil {
			t.Fatal(err)
		}
	}
	if len(b.records) != 2 || len(b.groups[first.Key().Group]) != 2 {
		t.Fatal("同组不同 Unit 应独立登记，重复提交不应创建记录")
	}
	for _, member := range view.Members {
		if !b.ContainsPod(member.Group, member.Pod.UID) {
			t.Fatal("缺少成员归属")
		}
	}
	// 重放接收只确认已发生的交接，不覆盖后来写入的执行记录。
	b.records[first.Key()].PendingAction = "binding-1"
	view.Members[0].Pod.Spec.NodeName = "node-a"
	if err := b.Accept(context.Background(), first); err != nil || b.records[first.Key()].PendingAction != "binding-1" {
		t.Fatalf("重复接收覆盖记录或被后续状态错误拒绝: %v", err)
	}
}

func TestAcceptRejectsConflictsWithoutPartialWrites(t *testing.T) {
	for _, kind := range []string{"atomic-second", "partial-overlap", "key-reused", "mode-changed-with-old-atomic"} {
		t.Run(kind, func(t *testing.T) {
			mode := workqueue.AllowPartial
			indices := []int{0}
			if kind == "atomic-second" || kind == "mode-changed-with-old-atomic" {
				mode, indices = workqueue.AllTogether, []int{0, 1}
			}
			b, view := fixture(mode)
			first := queuedUnit(t, view, "first", mode, indices...)
			if err := b.Accept(context.Background(), first); err != nil {
				t.Fatal(err)
			}
			id := "second"
			if kind == "key-reused" {
				id, indices = "first", []int{1}
			}
			if kind == "mode-changed-with-old-atomic" {
				mode, indices = workqueue.AllowPartial, []int{1}
				view.Group.Spec.SubmitMode = string(mode)
			}
			second := queuedUnit(t, view, id, mode, indices...)
			if err := b.Accept(context.Background(), second); err == nil {
				t.Fatal("冲突应拒绝")
			}
			if len(b.records) != 1 || len(b.groups[first.Key().Group]) != 1 || len(b.owners[first.Key().Group]) != len(first.Members()) {
				t.Fatal("拒绝时留下部分登记或破坏旧记录")
			}
		})
	}
}

func TestAcceptValidatesCurrentFacts(t *testing.T) {
	for _, kind := range []string{"atomic-incomplete", "mode-mismatch", "group-recreated", "group-deleting", "pod-recreated", "bound", "terminal", "wrong-owner", "unfrozen", "retry-source", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			mode := workqueue.AllowPartial
			if kind == "atomic-incomplete" {
				mode = workqueue.AllTogether
			}
			b, view := fixture(mode)
			u := queuedUnit(t, view, "first", mode, 0)
			ctx := context.Background()
			switch kind {
			case "mode-mismatch":
				view.Group.Spec.SubmitMode = string(workqueue.AllTogether)
			case "group-recreated":
				view.Group.UID = "replacement"
			case "group-deleting":
				now := metav1.Now()
				view.Group.DeletionTimestamp = &now
			case "pod-recreated":
				view.Members[0].Pod.UID = "replacement"
			case "bound":
				view.Members[0].Pod.Spec.NodeName = "node-a"
			case "terminal":
				view.Members[0].Pod.Status.Phase = corev1.PodFailed
			case "wrong-owner":
				view.Members[0].Pod.OwnerReferences[0].UID = "another-group"
			case "unfrozen":
				u = workqueue.NewUnit(u.Key(), mode)
			case "retry-source":
				u = workqueue.NewUnit(u.Key(), mode)
				_ = u.AddMember(workqueue.PodRef{Name: view.Members[0].Pod.Name, UID: view.Members[0].Pod.UID})
				_ = u.Freeze()
				_ = u.MarkQueued(workqueue.QueueRetry, nil, "")
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err := b.Accept(ctx, u); err == nil {
				t.Fatal("非法接收应拒绝")
			}
			if len(b.records) != 0 || len(b.groups) != 0 || len(b.owners) != 0 {
				t.Fatal("校验失败仍产生缓冲记录")
			}
		})
	}
}

func TestAcceptConcurrentMemberOwnership(t *testing.T) {
	b, view := fixture(workqueue.AllowPartial)
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for i := 0; i < 16; i++ {
		u := queuedUnit(t, view, fmt.Sprintf("unit-%d", i), workqueue.AllowPartial, 0)
		wg.Add(1)
		go func() { defer wg.Done(); results <- b.Accept(context.Background(), u) }()
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		}
	}
	if success != 1 || len(b.records) != 1 || len(b.owners[view.Members[0].Group]) != 1 {
		t.Fatalf("同一成员并发接收应只成功一次，实际 %d", success)
	}
}
