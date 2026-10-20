package staging

import (
	"context"
	"fmt"
	"log/slog"

	"batchscheduler/src/group"
)

// RunEvaluations 启动一个评估 worker，最多立即拉取 batchSize 个已就绪 Group Key。
// 不等凑满一批，不为每个通知创建 goroutine。同组待处理通知去重；处理期间再次
// 收到通知，Done 后会重新入队，因此不会因合批丢失最后一次变化。
// 这里使用 client-go 的处理队列，区别于保存调度 Unit 的业务 workqueue。
// handle 在锁外接收各组决策；本步 main 仅展示决策，Apply 尚未实现。
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
		// 只有本 worker 消费，Len>0 后 Get 不会被另一个消费者抢空。
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

func (b *Buffer) evaluateOne(ctx context.Context, key group.Key, handle func(context.Context, []Decision) error) {
	defer b.evaluations.Done(key)
	if ctx.Err() != nil {
		return
	}
	// worker 重读一次当前事实，使 Accept 之后无新 Watch 事件也能进行首轮评估。
	// 此次刷新不再次通知自身，否则会形成 Observe → 评估 → Observe 的空转循环。
	err := b.observe(ctx, Observation{Group: key}, false)
	if err == nil {
		var decisions []Decision
		decisions, err = b.Evaluate(ctx, key)
		if err == nil {
			err = handle(ctx, decisions)
		}
	}
	if err != nil && ctx.Err() == nil {
		// 本步只有通知去重，没有错误退避定时器；保留错误日志，后续通知可重评。
		// 不立即把错误 Key 无限 Add 回去，避免缓存/处理器异常时形成忙循环。
		slog.Error("组评估失败，等待后续通知重新评估", "group", key, "error", err)
	}
}
