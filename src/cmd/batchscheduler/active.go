package main

import (
	"context"
	"fmt"

	"batchscheduler/src/staging/buffer"
	"batchscheduler/src/workqueue"
)

// runActive 持续把 Active 中的 Unit 交给 Buffer，不在这里执行调度或绑定。
// Accept 会登记成员归属并通知评估线程；成功后，这一轮由 Buffer 接着管理。
func runActive(ctx context.Context, queues *workqueue.WorkQueue, center *buffer.Buffer) error {
	for {
		if err := queues.DispatchActive(ctx, center.Accept); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("Active 交给 Buffer: %w", err)
		}
	}
}
