package workqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"batchscheduler/src/group"
	"k8s.io/apimachinery/pkg/types"
)

// takeActive 模拟 Buffer 接收；使用和 runActive 相同的交接入口。
func takeActive(t *testing.T, w *WorkQueue) Unit {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var received Unit
	if err := w.DispatchActive(ctx, func(_ context.Context, unit Unit) error {
		received = unit
		return unit.MarkProcessing()
	}); err != nil {
		t.Fatal(err)
	}
	return received
}

type testStagingSet map[types.UID]bool

func (s testStagingSet) ContainsPod(_ group.Key, uid types.UID) bool { return s[uid] }

func TestDispatchActiveDropsOnlyTheDeliveredUnit(t *testing.T) {
	first, second := testView("work", "one", AllowPartial, 1, 1), testView("work", "two", AllowPartial, 1, 1)
	w := queueFor(first, second)
	selectOK(t, w, SelectRequest{View: first})
	selectOK(t, w, SelectRequest{View: second})
	u := takeActive(t, w)
	if w.groups[u.Key().Group] != nil || len(w.groups) != 1 {
		t.Fatal("交出后应删除该组队列记录，并保留另一组")
	}
	if !u.Frozen() || u.State().Phase != UnitProcessing {
		t.Fatal("交给 Buffer 后名单必须固定，执行状态由 Buffer 管理")
	}
	takeActive(t, w)
	if len(w.groups) != 0 {
		t.Fatal("队列仍在保留已经执行的 Unit")
	}
}

func TestDispatchFailureRestoresEditableActiveUnit(t *testing.T) {
	full := testView("work", "restore", AllowPartial, 2, 2)
	first := *full
	first.Members = full.Members[:1]
	w := queueFor(full)
	selectOK(t, w, SelectRequest{View: &first})
	want := errors.New("Buffer 未接收")
	err := w.DispatchActive(context.Background(), func(context.Context, Unit) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("没有返回接收错误: %v", err)
	}
	r := w.groups[full.Members[0].Group]
	if len(r.units) != 1 || len(r.pods) != 1 {
		t.Fatal("接收失败丢掉了原关联")
	}
	selectOK(t, w, SelectRequest{View: full})
	u := takeActive(t, w)
	if len(u.Members()) != 2 {
		t.Fatal("放回 Active 后没有重新开放成员追加")
	}
}

func TestDispatchSkipsEmptyEntryAfterPodWasBoundElsewhere(t *testing.T) {
	old, next := testView("work", "old", AllowPartial, 1, 1), testView("work", "next", AllowPartial, 1, 1)
	w := queueFor(old, next)
	selectOK(t, w, SelectRequest{View: old})
	old.Members[0].Pod.Spec.NodeName = "node-a"
	selectOK(t, w, SelectRequest{View: old})
	selectOK(t, w, SelectRequest{View: next})
	u := takeActive(t, w)
	if u.Key().Group != next.Members[0].Group || len(w.groups) != 0 {
		t.Fatal("空条目阻塞了后面的任务，或遗留了旧目录")
	}
}

func TestConcurrentSelectAndDispatchDoNotCreateAnotherUnit(t *testing.T) {
	view := testView("work", "concurrent-transfer", AllowPartial, 4, 4)
	w := queueFor(view)
	held := testStagingSet{}
	w.Staging = held
	selectOK(t, w, SelectRequest{View: view})
	var wg sync.WaitGroup
	errs := make(chan error, 33)
	start := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		errs <- w.DispatchActive(context.Background(), func(_ context.Context, unit Unit) error {
			if err := unit.MarkProcessing(); err != nil {
				return err
			}
			for _, ref := range unit.Members() {
				held[ref.UID] = true
			}
			return nil
		})
	}()
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- w.Select(SelectRequest{View: view})
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(w.groups) != 0 || len(held) != 4 {
		t.Fatal("并发事件使交出的成员重新出现在队列，或丢失了接收归属")
	}
	drainActive(t, w, 0)
}
