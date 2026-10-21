package staging

import (
	"context"
	"fmt"
	"log/slog"

	"batchscheduler/src/group"
)

// RunEvaluations 运行一个评估循环：取组编号，读最新信息，Evaluate，再把决定交给 handle。
// handle 在 main 中就是“逐个调用 Apply”的函数。这里没有自动启动调度执行器。
// 一批最多取 batchSize 个已经排好的组，不等凑满，也不是并行处理这些组。
// 同一组排队时通知十次也只排一份；处理期间又有新通知，处理完会再排一次。
// TODO：同组 Unit 分散进入 Buffer 时，可能分多轮调度；这个 batch 不会自动合并它们。
func (b *Buffer) RunEvaluations(ctx context.Context, batchSize int, handle func(context.Context, []Decision) error) error {
	if batchSize <= 0 || handle == nil {
		return fmt.Errorf("评估 worker 需要正数批量上限及决策处理器")
	}
	b.runMu.Lock()
	if b.runStarted {
		b.runMu.Unlock()
		return fmt.Errorf("评估 worker 只能启动一次")
	}
	b.runStarted = true
	b.runMu.Unlock()
	stopped := make(chan struct{})
	defer close(stopped)
	defer b.evaluations.ShutDown()
	go func() {
		// 退出时关闭通知队列，让正在 Get 中等待的评估线程醒来并退出。
		select {
		case <-ctx.Done():
			b.evaluations.ShutDown()
		case <-stopped:
		}
	}()
	for ctx.Err() == nil {
		key, shutdown := b.evaluations.Get()
		if shutdown {
			return nil
		}
		batch := []group.Key{key}
		// 只有这一个线程取通知，所以看到还有通知就能继续取，不会被别人抢空。
		for len(batch) < batchSize && b.evaluations.Len() > 0 {
			next, quit := b.evaluations.Get()
			if quit {
				break
			}
			batch = append(batch, next)
		}
		for _, key := range batch {
			b.evaluateOne(ctx, key, handle)
		}
	}
	return nil
}

// evaluateOne 处理一个组的通知，处理完必须 Done，让队列知道这次检查已结束。
func (b *Buffer) evaluateOne(ctx context.Context, key group.Key, handle func(context.Context, []Decision) error) {
	defer b.evaluations.Done(key)
	if ctx.Err() != nil {
		return
	}
	// Accept 后可能没有新的 informer 事件，所以评估前主动读一次缓存。
	// false 表示这次读取不再给自己排通知，避免没变化也不停检查。
	err := b.observe(ctx, Observation{Group: key}, false)
	if err == nil {
		var decisions []Decision
		decisions, err = b.Evaluate(ctx, key)
		if err == nil {
			err = handle(ctx, decisions)
		}
	}
	if err != nil && ctx.Err() == nil {
		// 本函数出错后只记日志，不马上无限重试。下一次事件才能让普通 Record 再检查。
		// Fail/Retry 另外有 RunQueueChecks 定时通知；它不是所有 Record 的错误重试器。
		slog.Error("组评估失败，等待后续通知重新评估", "group", key, "error", err)
	}
}
