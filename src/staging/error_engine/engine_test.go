package errorengine

import (
	"testing"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
)

func TestDecideFailureSettlementAndIsolation(t *testing.T) {
	for _, reason := range []string{
		workqueue.ReasonInsufficientResources, workqueue.ReasonBindingRejected,
		workqueue.ReasonForbidden, workqueue.ReasonInvalidSpec, "UnclassifiedFailure",
	} {
		t.Run(reason, func(t *testing.T) {
			input := Input{Failure: &group.FailureInfo{Reason: reason}, Members: []workqueue.PodRef{{Name: "pod", UID: "uid"}}}
			d, ok := Decide(input)
			if !ok || d.Action != Recover || d.Reason != reason {
				t.Fatalf("尚未结清时必须先恢复: %+v", d)
			}
			d.Failure.Reason = "changed"
			d.Members[0].Name = "changed"
			if input.Failure.Reason != reason || input.Members[0].Name != "pod" {
				t.Fatal("决策修改了输入状态")
			}
			input.RecoveryComplete, input.ResponsibilitiesSettled = true, true
			d, _ = Decide(input)
			if d.Action != ReturnFail || d.Failure.Reason != reason {
				t.Fatalf("已结清应回 Fail 并保留错误分类: %+v", d)
			}
		})
	}
}

func TestInvalidationTakesPriorityOverFailure(t *testing.T) {
	input := Input{Invalidated: true, Failure: &group.FailureInfo{Reason: workqueue.ReasonInsufficientResources}}
	d, _ := Decide(input)
	if d.Action != Recover || d.Reason != "AttemptInvalidated" {
		t.Fatalf("旧轮次失效应先恢复: %+v", d)
	}
	input.ResponsibilitiesSettled = true
	d, _ = Decide(input)
	if d.Action != Release {
		t.Fatalf("失效且结清后不能回送旧对象重试: %+v", d)
	}
	if _, ok := Decide(Input{}); ok {
		t.Fatal("没有异常不应生成错误决策")
	}
}
