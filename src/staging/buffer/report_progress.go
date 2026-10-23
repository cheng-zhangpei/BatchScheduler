package buffer

import (
	"fmt"
	"reflect"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
)

// validateFeedback 检查报告的含义与顺序，不要求 Buffer 批准执行器的下一步。
func validateFeedback(r *staging.Record, f staging.Feedback) error {
	rank := phaseOrder(f.Phase)
	if rank == 0 || rank < phaseOrder(r.ExecutionPhase) {
		return fmt.Errorf("执行进度未知或倒退: %s → %s", r.ExecutionPhase, f.Phase)
	}
	switch f.Kind {
	case staging.FeedbackProgress:
		if f.ResponsibilitiesSettled {
			return fmt.Errorf("进展报告不能代替整轮结束报告")
		}
	case staging.FeedbackUnknown:
		if (f.Phase != staging.PhaseBinding && f.Phase != staging.PhaseRecovering) || f.ResponsibilitiesSettled {
			return fmt.Errorf("未知结果只能等待绑定或恢复确认，不能声明资源已结清")
		}
	case staging.FeedbackFinished:
		if !f.ResponsibilitiesSettled || (f.Phase != staging.PhaseBound && f.Phase != staging.PhaseRecovered) {
			return fmt.Errorf("整轮结束须已绑定或已恢复，并明确结清资源")
		}
		if f.Phase == staging.PhaseRecovered && f.Operation == staging.OperationRun && f.Failure == nil {
			return fmt.Errorf("Run 恢复结束需要保留本轮失败原因")
		}
	case staging.FeedbackBlocked:
		if f.Message == "" {
			return fmt.Errorf("停在待实现步骤时需要说明原因")
		}
	default:
		return fmt.Errorf("未知反馈类型: %q", f.Kind)
	}
	if f.Failure != nil && f.Phase != staging.PhaseRecovering && f.Phase != staging.PhaseRecovered {
		return fmt.Errorf("确认失败后应由执行器进入恢复流程再报告")
	}
	if f.Operation == staging.OperationRecover && f.Phase != staging.PhaseRecovering && f.Phase != staging.PhaseRecovered {
		return fmt.Errorf("恢复请求不能报告正常调度进度")
	}

	plan := f.Plan
	if plan == nil {
		plan = r.Plan
	}
	if f.Plan != nil || (rank >= phaseOrder(staging.PhasePlanned) && rank <= phaseOrder(staging.PhaseBound)) {
		if len(plan) != len(r.Members) {
			return fmt.Errorf("反馈缺少完整成员计划")
		}
		for ref := range r.Members {
			if plan[ref] == "" {
				return fmt.Errorf("成员 %s 没有目标节点", ref.Name)
			}
		}
	}
	// 已预占后再换计划，会让进度记录与真正占用的节点不一致。
	if f.Plan != nil && phaseOrder(r.ExecutionPhase) >= phaseOrder(staging.PhaseAssumed) && !reflect.DeepEqual(f.Plan, r.Plan) {
		return fmt.Errorf("预占后不能用另一份计划覆盖原计划")
	}
	return nil
}

// updateReportedProgress 记录执行器已经做到哪，不负责做预占、绑定或资源清理。
func updateReportedProgress(r *staging.Record, f staging.Feedback) {
	r.ExecutionPhase = f.Phase
	if f.Plan != nil {
		r.Plan = copyPlan(f.Plan)
	}
	if f.Failure != nil {
		r.Failure = copyFailure(f.Failure)
	}
	for ref, state := range r.Members {
		switch f.Phase {
		case staging.PhaseAssumed:
			state.Phase, state.PlannedNode = group.PodReserved, r.Plan[ref]
		case staging.PhaseBinding:
			state.Phase, state.PlannedNode = group.PodBinding, r.Plan[ref]
			if f.Kind == staging.FeedbackUnknown {
				state.Phase = group.PodBindingUnknown
			}
		case staging.PhaseBound:
			state.Phase = group.PodBound
		case staging.PhaseRecovered:
			state.Phase, state.PlannedNode = group.PodUnscheduled, ""
		}
		r.Members[ref] = state
	}
}

func phaseOrder(phase staging.ExecutionPhase) int {
	switch phase {
	case staging.PhaseFiltering:
		return 1
	case staging.PhaseFiltered:
		return 2
	case staging.PhaseScored:
		return 3
	case staging.PhasePlanned:
		return 4
	case staging.PhaseAssumed:
		return 5
	case staging.PhaseBinding:
		return 6
	case staging.PhaseBound:
		return 7
	case staging.PhaseRecovering:
		return 8
	case staging.PhaseRecovered:
		return 9
	default:
		return 0
	}
}
