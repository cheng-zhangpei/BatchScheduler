package staging

import (
	"context"
	"time"

	"batchscheduler/src/workqueue"
)

// retryHint 暂时记住：某一版 Unit 正在等的条件发生过变化。
type retryHint struct {
	revision uint64
	wake     workqueue.RetryWake
}

// NotifyRetry 接收“资源/节点等变了”的通知，找出正在等这个条件的 Retry 任务。
// 记下提示后把组编号放进评估队列；不在这里直接移动任务。
// 也记住 Unit 版本，防止把这次变化用到后来另一轮任务上。
func (b *Buffer) NotifyRetry(condition workqueue.RetryCondition) {
	if b.handlers.WaitingGroups == nil || b.handlers.QueueSnapshot == nil {
		return
	}
	for _, key := range b.handlers.WaitingGroups() {
		for _, entry := range b.handlers.QueueSnapshot(key) {
			if entry.State.Queue != workqueue.QueueRetry || !entry.Retry.Plan.Condition.Matches(condition) {
				continue
			}
			b.mu.Lock()
			b.retryHints[entry.Unit.Key()] = retryHint{revision: entry.State.Revision, wake: workqueue.RetryWake{Changed: condition}}
			b.mu.Unlock()
			b.evaluations.Add(key)
		}
	}
}

// RunQueueChecks 每隔 interval 通知检查一次 Fail/Retry 中的组。
// 否则集群没变化时，退避时间即使到了也没人再评估。时钟通知不等于资源真的改善。
// 当前遍历等待中的组，没有为每个 Unit 开线程；后面再考虑只检查刚到期的任务。
func (b *Buffer) RunQueueChecks(ctx context.Context, interval time.Duration) {
	if interval <= 0 || b.handlers.WaitingGroups == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, key := range b.handlers.WaitingGroups() {
				b.evaluations.Add(key)
			}
		}
	}
}
