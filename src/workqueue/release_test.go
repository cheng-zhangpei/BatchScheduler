package workqueue

import (
	"testing"

	"batchscheduler/src/group"
)

func TestReleaseUnitPreservesOtherUnitsAndAllowsReplay(t *testing.T) {
	full := testView("work", "release", AllowPartial, 2, 2)
	first := *full
	first.Members = full.Members[:1]
	w := queueFor(full)
	selectOK(t, w, SelectRequest{View: &first})
	u := drainActive(t, w, 1)[0]
	if err := u.MarkProcessing(); err != nil {
		t.Fatal(err)
	}
	selectOK(t, w, SelectRequest{View: full})
	other := drainActive(t, w, 1)[0]
	key := u.Key()
	if err := w.ReleaseUnit(key); err != nil {
		t.Fatal(err)
	}
	record := w.groups[key.Group]
	if record == nil || record.units[other.Key().ID] != other || len(record.units) != 1 {
		t.Fatal("清理了同组其他 Unit")
	}
	if _, exists := record.group.UnitIDs[key.ID]; exists {
		t.Fatal("组目录仍有旧 Unit")
	}
	if record.pods[u.Members()[0].UID] != nil || record.pods[other.Members()[0].UID].UnitID != other.Key().ID {
		t.Fatal("成员清理范围错误")
	}
	if u.State().Phase != UnitProcessing {
		t.Fatal("WorkQueue 不应代替 Buffer 标记 Done")
	}
	if err := w.ReleaseUnit(key); err != nil {
		t.Fatal("重复清理应成功", err)
	}
	if err := other.MarkProcessing(); err != nil {
		t.Fatal(err)
	}
	if err := w.ReleaseUnit(other.Key()); err != nil {
		t.Fatal(err)
	}
	if w.groups[key.Group] != nil {
		t.Fatal("最后一个 Unit 结束后仍有空组记录")
	}
	if err := w.ReleaseUnit(key); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseUnitRejectsQueuedTasksWithoutChanges(t *testing.T) {
	for _, queue := range []QueueName{QueueActive, QueueFail, QueueRetry, QueueFastRetry} {
		t.Run(string(queue), func(t *testing.T) {
			view := testView("work", "queued", AllTogether, 1, 1)
			w := queueFor(view)
			w.Policy = func(SelectionInput, Unit) (QueueDecision, error) {
				return QueueDecision{To: queue, Reason: "failure"}, nil
			}
			selectOK(t, w, SelectRequest{View: view})
			key := view.Members[0].Group
			record := w.groups[key]
			id := record.pods[view.Members[0].Pod.UID].UnitID
			if err := w.ReleaseUnit(UnitKey{Group: key, ID: id}); err == nil {
				t.Fatal("队列中的任务不允许直接清理")
			}
			if len(record.units) != 1 || len(record.pods) != 1 || record.units[id].State().Queue != queue {
				t.Fatal("拒绝请求时修改了记录")
			}
		})
	}
	if err := New().ReleaseUnit(UnitKey{Group: group.Key{}}); err == nil {
		t.Fatal("空编号应报错")
	}
}
