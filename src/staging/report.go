package staging

import (
	"context"
	"fmt"
	"reflect"

	"batchscheduler/src/group"
)

// Report 接收执行器的结果，修改执行进度，再通知 Evaluate 安排下一步。
// 当前要求执行器把整个操作的成员结果汇总好，再调用一次；逐 Pod 上报尚未实现。
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
		// 同一份结果再次发来，不再修改进度或增加版本。
		return nil
	}
	if feedback.ActionID == "" || r.PendingAction != feedback.ActionID || r.PendingDecision != ActionExecute || r.PendingOperation != feedback.Operation {
		// 例如当前正在等 Bind，不能用旧 Schedule 请求的结果推进它。
		return fmt.Errorf("反馈动作身份不匹配或已经过期")
	}
	if feedback.Unknown {
		// 不知道成败就继续等。现在清 Pending 再绑定，可能把已经成功的请求重复执行。
		return nil
	}
	if feedback.ResponsibilitiesSettled && (feedback.Failure != nil || (feedback.Operation != OperationBind && feedback.Operation != OperationRecover)) {
		return fmt.Errorf("只有成功的 Bind/Recover 可以确认资源责任结清")
	}
	if feedback.Failure == nil && feedback.Operation == OperationSchedule {
		// 算节点成功必须给每个成员一个节点，不能拿部分计划继续预占和绑定。
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
		// 记下本轮明确失败，取消下一步正常执行；之后由 Evaluate 安排 Recover。
		r.Failure = copyFailure(feedback.Failure)
		r.RecoveryComplete, r.ResponsibilitiesSettled, r.NextOperation = false, false, ""
	} else {
		switch feedback.Operation {
		case OperationSchedule:
			// 节点算好了：存计划，下次评估可以安排资源预占。
			r.Plan, r.NextOperation = copyPlan(feedback.Plan), OperationAssume
		case OperationAssume:
			// 资源预占成功：记 Reserved，下次评估可以安排绑定。
			r.NextOperation = OperationBind
			for ref, state := range r.Members {
				state.Phase, state.PlannedNode = group.PodReserved, r.Plan[ref]
				r.Members[ref] = state
			}
		case OperationBind:
			// 执行器说绑定完成；集群是否已看到绑定，仍由 Observe 单独确认。
			r.NextOperation = ""
			r.ResponsibilitiesSettled = feedback.ResponsibilitiesSettled
			for ref, state := range r.Members {
				state.Phase = group.PodBound
				r.Members[ref] = state
			}
		case OperationRecover:
			// 恢复完成后仍保留原失败原因，Evaluate 要带着它把任务送回 Fail。
			r.NextOperation = ""
			r.RecoveryComplete, r.ResponsibilitiesSettled = true, feedback.ResponsibilitiesSettled
		default:
			return fmt.Errorf("未知执行阶段: %q", feedback.Operation)
		}
	}
	copyFeedback := feedback
	copyFeedback.Failure, copyFeedback.Plan = copyFailure(feedback.Failure), copyPlan(feedback.Plan)
	r.LastFeedback = &copyFeedback
	// Bind/Recover 可以先报告“操作成功，但收尾没完”，这时继续保留同一操作编号。
	// 等执行器再用这个编号报告收尾完成，才清 Pending，让评估继续。
	awaitingSettlement := feedback.Failure == nil && (feedback.Operation == OperationBind || feedback.Operation == OperationRecover) && !feedback.ResponsibilitiesSettled
	if !awaitingSettlement {
		r.PendingAction, r.PendingOperation, r.PendingDecision = "", "", ""
	}
	r.Revision++
	// Report 只更新结果；真正决定下一步的仍是评估线程。
	b.evaluations.Add(feedback.Unit.Group)
	return nil
}
