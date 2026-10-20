package staging

import (
	"context"
	"fmt"
	"reflect"

	"batchscheduler/src/group"
)

// Report 当前只接受执行器汇总后的整操作反馈。逐 Pod 回调的聚合后续单独设计，
// 不能把一个成员完成冒充整个操作完成。未知结果保留原 ActionID，等待同操作确认。
func (b *Buffer) Report(ctx context.Context, feedback Feedback) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if feedback.Pod != nil {
		return fmt.Errorf("当前需要整操作汇总反馈，不接受单成员完成")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	r := b.records[feedback.Unit]
	if r == nil {
		return fmt.Errorf("反馈对应的本轮记录不存在")
	}
	if r.LastFeedback != nil && reflect.DeepEqual(*r.LastFeedback, feedback) {
		return nil
	}
	if feedback.ActionID == "" || r.PendingAction != feedback.ActionID || r.PendingDecision != ActionExecute || r.PendingOperation != feedback.Operation {
		return fmt.Errorf("反馈动作身份不匹配或已经过期")
	}
	if feedback.Unknown {
		// 不把诊断错误写成确认失败，也不清掉在途动作，以免重复绑定。
		return nil
	}
	if feedback.ResponsibilitiesSettled && (feedback.Failure != nil || (feedback.Operation != OperationBind && feedback.Operation != OperationRecover)) {
		return fmt.Errorf("只有成功的 Bind/Recover 可以确认资源责任结清")
	}
	if feedback.Failure == nil && feedback.Operation == OperationSchedule {
		if len(feedback.Plan) != len(r.Members) {
			return fmt.Errorf("调度成功反馈缺少完整成员计划")
		}
		for ref := range r.Members {
			if feedback.Plan[ref] == "" {
				return fmt.Errorf("成员 %s 缺少目标节点", ref.Name)
			}
		}
	}
	if feedback.Failure != nil {
		r.Failure = copyFailure(feedback.Failure)
		r.RecoveryComplete, r.ResponsibilitiesSettled, r.NextOperation = false, false, ""
	} else {
		switch feedback.Operation {
		case OperationSchedule:
			r.Plan, r.NextOperation = copyPlan(feedback.Plan), OperationAssume
		case OperationAssume:
			r.NextOperation = OperationBind
			for ref, state := range r.Members {
				state.Phase, state.PlannedNode = group.PodReserved, r.Plan[ref]
				r.Members[ref] = state
			}
		case OperationBind:
			r.NextOperation = ""
			r.ResponsibilitiesSettled = feedback.ResponsibilitiesSettled
			for ref, state := range r.Members {
				state.Phase = group.PodBound
				r.Members[ref] = state
			}
		case OperationRecover:
			r.NextOperation = ""
			r.RecoveryComplete, r.ResponsibilitiesSettled = true, feedback.ResponsibilitiesSettled
		default:
			return fmt.Errorf("未知执行阶段: %q", feedback.Operation)
		}
	}
	copyFeedback := feedback
	copyFeedback.Failure, copyFeedback.Plan = copyFailure(feedback.Failure), copyPlan(feedback.Plan)
	r.LastFeedback = &copyFeedback
	// Bind/Recover 已返回成功但账目尚未结清时，仍保留同一 ActionID 等待最终确认。
	// 不能清掉身份后让“结清”反馈无处对应，也不能提前开始下一轮。
	awaitingSettlement := feedback.Failure == nil && (feedback.Operation == OperationBind || feedback.Operation == OperationRecover) && !feedback.ResponsibilitiesSettled
	if !awaitingSettlement {
		r.PendingAction, r.PendingOperation, r.PendingDecision = "", "", ""
	}
	r.Revision++
	b.evaluations.Add(feedback.Unit.Group)
	return nil
}
