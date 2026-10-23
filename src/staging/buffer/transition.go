package buffer

import (
	"context"
	"errors"
	"fmt"

	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// applyTransition 让 WorkQueue 按请求移动任务，例如 Fail → Retry。
// 这些任务还在队列里，所以不需要给它们创建 Buffer.Record 或 Pending。
func (b *Buffer) applyTransition(ctx context.Context, d staging.Decision) error {
	if b.handlers.Transition == nil {
		return staging.ErrHandlerUnavailable
	}
	b.mu.RLock()
	_, held := b.records[*d.Unit]
	b.mu.RUnlock()
	if held {
		// Buffer 还在执行这个 Unit 时，不能同时把它当作等待任务搬队列。
		return fmt.Errorf("Unit 仍由 Center 执行，不能同时迁移队列")
	}
	err := b.handlers.Transition(ctx, *d.Transition)
	if err == nil || errors.Is(err, workqueue.ErrRetryNotReady) {
		// 队列已处理了提示，或已记住“相关变化发生过但还没到退避时间”。
		// 所以可以删掉 Buffer 这边暂存的同一条提示。
		b.mu.Lock()
		delete(b.retryHints, *d.Unit)
		b.mu.Unlock()
	}
	if errors.Is(err, workqueue.ErrRetryNotReady) {
		// 只是还要等时间/条件，不是执行故障，不需要 main 把它当错误报告。
		return nil
	}
	return err
}
