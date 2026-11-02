package buffer

import (
	"context"
	"fmt"
	"reflect"

	"batchscheduler/src/staging"
)

// Report 是执行器唯一的反馈入口：进展只更新记录，整轮结束才取消 Pending。
// 不生成 Assume/Bind 命令，不推进执行器，也不直接搬 workqueue。
// 恢复结果从这里进入：Finished + Recovered 会保存原始 Failure、标记恢复完成、
// 清掉 Pending 并通知评估。Evaluate 再决定回送，Report 本身不调用 Select。
func (b *Buffer) Report(ctx context.Context, feedback staging.Feedback) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if feedback.Pod != nil {
		return fmt.Errorf("当前按整个操作汇报，不接受单成员反馈")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	r := b.records[feedback.Unit]
	if r == nil {
		return fmt.Errorf("反馈对应的执行记录不存在")
	}
	if r.LastFeedback != nil && reflect.DeepEqual(*r.LastFeedback, feedback) {
		return nil
	}
	if feedback.ActionID == "" || r.PendingAction != feedback.ActionID || r.PendingDecision != staging.ActionExecute || r.PendingOperation != feedback.Operation {
		return fmt.Errorf("反馈不是当前这次执行的结果")
	}
	// 所有检查通过后才修改 Record，错误反馈不能留下改了一半的数据。
	if err := validateFeedback(r, feedback); err != nil {
		return err
	}
	if err := validateResourceResult(r, feedback.Resources); err != nil {
		return err
	}
	// 这就是之前回调的unplaced的部分的pod，也就是partial或者atomic的不同的模式是会有重试队列
	if feedback.Resources != nil {
		// 只记下成员划分；原 Unit、owners 和等待中的 ActionID 都保留，尚不回送。
		r.Resources = feedback.Resources.DeepCopy()
	}
	r.ExecutionBlocked = feedback.Kind == staging.FeedbackBlocked
	updateReportedProgress(r, feedback)
	switch feedback.Kind {
	case staging.FeedbackProgress, staging.FeedbackUnknown:
		// 继续执行或等待确认：同一个 ActionID 还没结束，Evaluate 只能等待。
	case staging.FeedbackFinished:
		r.ResponsibilitiesSettled = true
		r.RecoveryComplete = feedback.Phase == staging.PhaseRecovered
		r.PendingAction, r.PendingOperation, r.PendingDecision = "", "", ""
	case staging.FeedbackBlocked:
		// 代码停在待实现步骤，不靠反复重新执行整轮来填空。
		r.ExecutionBlocked = true
		r.ResponsibilitiesSettled = feedback.ResponsibilitiesSettled
		r.RecoveryComplete = feedback.ResponsibilitiesSettled
		if feedback.ResponsibilitiesSettled {
			r.PendingAction, r.PendingOperation, r.PendingDecision = "", "", ""
		}
	}
	copyFeedback := feedback
	copyFeedback.Plan, copyFeedback.Failure = copyPlan(feedback.Plan), copyFailure(feedback.Failure)
	copyFeedback.Resources = feedback.Resources.DeepCopy()
	r.LastFeedback = &copyFeedback
	r.Revision++
	b.evaluations.Add(feedback.Unit.Group)
	return nil
}
