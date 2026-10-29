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
		unit, err := queues.Active.Pop(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("读取 Active: %w", err)
		}
		if err := center.Accept(ctx, unit); err != nil {
			// Pop 只取出 FIFO 条目；Accept 失败时没有完成接收，Unit 仍是 Queued/Active。
			// 原样放回后停止消费并上报，让 main 退出；不丢掉它，也不立即反复尝试同一个错误。
			queues.Active.Add(unit)
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("Unit %s 未进入 Buffer，已放回 Active: %w", unit.Key().ID, err)
		}
	}
}
