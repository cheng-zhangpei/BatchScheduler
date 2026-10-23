package workqueue

import (
	"testing"

	"batchscheduler/src/group"
)

func TestSelectReturnBatchesPartialMembersAndPreservesOtherUnits(t *testing.T) {
	view := testView("work", "return", AllowPartial, 4, 4)
	first := *view
	first.Members = view.Members[:3]
	w := queueFor(view)
	selectOK(t, w, SelectRequest{View: &first})
	old := drainActive(t, w, 1)[0]
	if err := old.MarkProcessing(); err != nil {
		t.Fatal(err)
	}
	selectOK(t, w, SelectRequest{View: view})
	other := drainActive(t, w, 1)[0]
	// 两个失败成员一起回送；第三个已经绑定，不再调度。
	view.Members[2].Pod.Spec.NodeName = "node-a"
	w.Staging = testStaging{key: old.Key().Group, uid: old.Members()[0].UID}
	request := SelectRequest{Return: &ReturnRequest{Previous: old.Key(), Members: old.Members()[:2],
		Failure: &group.FailureInfo{Reason: ReasonInsufficientResources, Message: "cpu"}, RecoveryComplete: true}}
	selectOK(t, w, request)
	queued := w.GroupSnapshot(old.Key().Group)
	if len(queued) != 1 || queued[0].State.Queue != QueueFail || len(queued[0].Unit.Members()) != 2 || !queued[0].Failure.RecoveryComplete || queued[0].Failure.Message != "cpu" {
		t.Fatalf("回送没有接住整批失败成员和错误: %+v", queued)
	}
	next := queued[0].Unit
	record := w.groups[old.Key().Group]
	if record.units[old.Key().ID] != nil || record.units[other.Key().ID] != other || len(record.units) != 2 {
		t.Fatal("没有替换旧 Unit，或误删同组其他 Unit")
	}
	for _, ref := range next.Members() {
		if record.pods[ref.UID].UnitID != next.Key().ID || record.pods[ref.UID].Phase != group.PodUnscheduled {
			t.Fatal("新轮次成员关联错误")
		}
	}
	if record.pods[old.Members()[2].UID] != nil || old.State().Phase != UnitProcessing {
		t.Fatal("已完成成员残留，或提前代替 Buffer 标记 Done")
	}
	if err := w.Select(request); err == nil {
		t.Fatal("已经替换的旧轮次不能再次创建新任务")
	}
	if len(w.GroupSnapshot(old.Key().Group)) != 1 {
		t.Fatal("重复请求产生了额外 Unit")
	}
}

func TestSelectReturnRejectsInvalidRequestsWithoutChangingQueues(t *testing.T) {
	for _, name := range []string{"atomic-subset", "not-recovered", "no-error", "foreign-member", "duplicate-member", "bound", "recreated-group", "queued", "mode-changed", "factory-error"} {
		t.Run(name, func(t *testing.T) {
			mode := AllowPartial
			if name == "atomic-subset" {
				mode = AllTogether
			}
			view := testView("work", name, mode, 2, 2)
			w := queueFor(view)
			selectOK(t, w, SelectRequest{View: view})
			old := drainActive(t, w, 1)[0]
			if name != "queued" {
				if err := old.MarkProcessing(); err != nil {
					t.Fatal(err)
				}
			}
			request := &ReturnRequest{Previous: old.Key(), Members: old.Members(),
				Failure: &group.FailureInfo{Reason: ReasonInsufficientResources}, RecoveryComplete: true}
			switch name {
			case "atomic-subset":
				request.Members = request.Members[:1]
			case "not-recovered":
				request.RecoveryComplete = false
			case "no-error":
				request.Failure = nil
			case "foreign-member":
				request.Members = []PodRef{{Name: "other", UID: "other"}}
			case "duplicate-member":
				request.Members = []PodRef{old.Members()[0], old.Members()[0]}
			case "bound":
				view.Members[0].Pod.Spec.NodeName = "node-a"
			case "recreated-group":
				view.Group.UID = "new-group"
			case "mode-changed":
				view.Group.Spec.SubmitMode = string(AllTogether)
			case "factory-error":
				w.MakeUnit = func(UnitKey, SubmitMode) Unit { return nil }
			}
			before := old.State()
			if err := w.Select(SelectRequest{Return: request}); err == nil {
				t.Fatal("无效回送应被拒绝")
			}
			record := w.groups[old.Key().Group]
			if len(record.units) != 1 || record.units[old.Key().ID] != old || len(w.GroupSnapshot(old.Key().Group)) != 0 || old.State().Revision != before.Revision {
				t.Fatal("失败请求修改了旧轮次或实际队列")
			}
			for _, ref := range old.Members() {
				if record.pods[ref.UID].UnitID != old.Key().ID {
					t.Fatal("失败请求修改了成员关联")
				}
			}
		})
	}
}
