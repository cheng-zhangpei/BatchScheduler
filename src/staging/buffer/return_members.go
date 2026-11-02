package buffer

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// applyReturnMembers 把未放置成员交给 Select；成功后只解除这些成员的旧归属。
// 原 Record、冻结 Unit 和绑定的 Pending 都保留，A、B 的 Report 继续使用原 ActionID。
func (b *Buffer) applyReturnMembers(ctx context.Context, d staging.Decision) error {
	if b.handlers.Handoff == nil {
		return staging.ErrHandlerUnavailable
	}
	if d.Unit == nil || len(d.Members) == 0 || d.Failure == nil || d.To != workqueue.QueueFail {
		return fmt.Errorf("部分回送需要旧 Unit、成员、错误以及 Fail 目标")
	}

	// 1. 记下正在交接，暂时不结束旧轮次。绑定反馈仍然可以更新这份 Record。
	b.mu.Lock()
	r := b.records[*d.Unit]
	if r == nil || r.Revision != d.Revision || r.ReturningMembers {
		b.mu.Unlock()
		return staging.ErrStaleDecision
	}
	r.ReturningMembers = true
	r.Revision++
	b.nextAction++
	d.Members, d.Failure = slices.Clone(d.Members), copyFailure(d.Failure)
	request := staging.Handoff{ActionID: fmt.Sprintf("return-members-%d", b.nextAction), Unit: r.Unit, Decision: d}
	b.mu.Unlock()

	// 2. 外部 Handoff 调用 WorkQueue.Select。这里不占着 Buffer 的锁等队列处理。
	// Select 成功才表示新 Unit 已接住 C、D；此时仍未清除 Buffer 中它们的旧归属。
	err := b.handlers.Handoff(ctx, request)

	// 3. 交接成功后标记 C、D 已交出；失败则保留旧归属。两者都不动绑定 Pending。
	b.mu.Lock()
	defer b.mu.Unlock()
	if errors.Is(err, staging.ErrHandoffUnknown) {
		// 不能确认新 Unit 是否已经创建，先保留正在交接的标记，不能盲目重复提交。
		return err
	}
	r.ReturningMembers = false
	r.Revision++
	if err != nil {
		return err
	}
	if r.ReturnedMembers == nil {
		r.ReturnedMembers = make(map[workqueue.PodRef]bool)
	}
	for _, ref := range d.Members {
		r.ReturnedMembers[ref] = true
		if b.owners[d.Group][ref.UID] == *d.Unit {
			delete(b.owners[d.Group], ref.UID)
		}
	}
	// 继续检查其他未放置成员，并让新 Fail 任务有机会按错误类型进入 Retry。
	b.evaluations.Add(d.Group)
	return nil
}
