package buffer

import (
	"context"
	"errors"
	"fmt"

	"batchscheduler/src/staging"
)

// applyHandoff 通知队列：“旧 Unit 要结束了，请处理旧关联；Return 时还要接住重试任务”。
// 队列函数返回后，再由 finishHandoff 决定是否删除 Buffer 里的旧记录。
func (b *Buffer) applyHandoff(ctx context.Context, d staging.Decision) error {
	if b.handlers.Handoff == nil {
		return staging.ErrHandlerUnavailable
	}
	_, request, err := b.beginAction(d)
	if err != nil {
		return err
	}
	err = b.handlers.Handoff(ctx, request)
	return b.finishHandoff(d, request.ActionID, err)
}

// finishHandoff 处理队列函数的返回结果：
// nil：队列已经处理好，删旧记录；普通 error：确定没做成，保留记录并允许下次重试；
// ErrHandoffUnknown：不知道做成没有，保留 Pending，等查清楚后调用 ConfirmHandoff。
func (b *Buffer) finishHandoff(d staging.Decision, id string, handoffErr error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.records[*d.Unit]
	if r == nil || r.PendingAction != id {
		return fmt.Errorf("交接记录已改变")
	}
	if handoffErr != nil {
		if !errors.Is(handoffErr, staging.ErrHandoffUnknown) {
			// 确定没做成，才取消等待标记；否则下次重发可能重复入队。
			r.PendingAction, r.PendingOperation, r.PendingDecision = "", "", ""
			r.Revision++
		}
		return handoffErr
	}
	// 在unit层面标记已经完成
	if err := r.Unit.MarkFinished(); err != nil {
		return err
	}
	// 在buffer中将record移出去
	// 队列确认处理完成后，才删掉自己这边的记录；不是在这里重试或释放资源。
	b.removeRecord(*d.Unit)
	if d.Action == staging.ActionReturn {
		// Partial 的旧 Unit 可能还有未失败、未绑定的成员。旧归属现在已清掉，
		// 再检查一次整个组，让这些成员走普通 Select，而不是一起进入 Fail 或等下一次集群事件。
		b.evaluations.Add(d.Group)
	}
	return nil
}
