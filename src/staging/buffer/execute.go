package buffer

import (
	"context"
	"errors"

	"batchscheduler/src/staging"
)

// applyExecute 把请求交给执行器。执行器完成后调用 Report 告诉我们结果。
// 调用本身是同步的；是否交给后台线程执行，由执行器实现决定。
// 普通 error 可能已有操作在途；只有执行器明确返回 ErrExecutionNotStarted 才取消 Pending。
func (b *Buffer) applyExecute(ctx context.Context, d staging.Decision) error {
	if b.handlers.Execute == nil {
		return staging.ErrHandlerUnavailable
	}
	request, _, err := b.beginAction(d)
	if err != nil {
		return err
	}
	err = b.handlers.Execute(ctx, request)
	if errors.Is(err, staging.ErrExecutionNotStarted) {
		b.mu.Lock()
		// 只撤销刚才这次请求的等待标记，不动后来其他请求或已经完成的结果。
		if record := b.records[request.Unit]; record != nil && record.PendingAction == request.ActionID {
			record.PendingAction, record.PendingOperation, record.PendingDecision = "", "", ""
			record.Revision++
		}
		b.mu.Unlock()
	}
	return err
}
