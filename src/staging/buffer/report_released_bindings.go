package buffer

import (
	"fmt"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// validateReleasedBindings 检查后台回收只涉及这次绑定计划中的成员。
// 普通进展不能顺手释放成员；这里允许 Atomic 恢复阶段暂时按成员处理。
func validateReleasedBindings(r *staging.Record, f staging.Feedback) error {
	if len(f.ReleasedBindings) == 0 {
		return nil
	}
	previous := r.LastFeedback
	if f.Operation != staging.OperationRun || previous == nil || previous.ActionID != f.ActionID || previous.Kind != staging.FeedbackUnknown ||
		previous.Failure == nil || previous.Failure.Reason != workqueue.ReasonBindingUnknown {
		return fmt.Errorf("绑定回收只能结清当前未知绑定")
	}
	if f.Kind != staging.FeedbackFinished && f.Kind != staging.FeedbackBlocked {
		return fmt.Errorf("绑定回收必须报告本轮结束或明确阻塞")
	}
	seen := make(map[workqueue.PodRef]bool)
	for _, ref := range f.ReleasedBindings {
		if r.Plan[ref] == "" || seen[ref] {
			return fmt.Errorf("回收成员不在原绑定计划或重复: %s", ref.Name)
		}
		seen[ref] = true
	}
	if f.Kind == staging.FeedbackFinished && ((len(seen) == len(r.Plan) && f.Phase != staging.PhaseRecovered) ||
		(len(seen) < len(r.Plan) && f.Phase != staging.PhaseBound)) {
		return fmt.Errorf("全部释放应报告 Recovered，保留已绑定成员时应报告 Bound")
	}
	return nil
}

// applyReleasedBindings 把已释放成员转入 Unplaced，复用原来的部分回送路径。
// 原 Unit 和 Plan 仍保存本轮原始名单；实际还占用资源的成员留在 Assumed。
func applyReleasedBindings(r *staging.Record, f staging.Feedback) {
	if len(f.ReleasedBindings) == 0 {
		return
	}
	for _, ref := range f.ReleasedBindings {
		state := r.Members[ref]
		state.Phase, state.PlannedNode = group.PodUnscheduled, ""
		r.Members[ref] = state
	}
	if f.Kind != staging.FeedbackFinished {
		return
	}
	if r.Unit.Mode() == workqueue.AllTogether {
		if len(f.ReleasedBindings) == len(r.Plan) {
			return // 没有成员已绑定时，Atomic 仍整轮回送。
		}
		r.BindingRecovery = true // 恢复阶段暂时保留已绑定成员，只重排失败成员。
	}
	resources := r.Resources.DeepCopy()
	if resources == nil {
		resources = &staging.ResourceResult{Assumed: r.Unit.Members()}
	}
	released := make(map[workqueue.PodRef]bool, len(f.ReleasedBindings))
	for _, ref := range f.ReleasedBindings {
		released[ref] = true
	}
	kept := resources.Assumed[:0]
	for _, ref := range resources.Assumed {
		if released[ref] {
			resources.Unplaced = append(resources.Unplaced, staging.UnplacedMember{Pod: ref,
				Failure: group.FailureInfo{Reason: workqueue.ReasonBindingConfirmationExpired, Message: "已按核查策略释放暂占，等待退避重试"}})
		} else {
			kept = append(kept, ref)
		}
	}
	resources.Assumed = kept
	r.Resources = resources
	// 失败已分到成员上，不能再用一个 Unit 级错误覆盖原来的其他失败原因。
	r.Failure = nil
}
