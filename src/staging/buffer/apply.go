package buffer

import (
	"context"
	"fmt"

	"batchscheduler/src/staging"
)

// Apply 根据 Decision 调用对应函数，不在这里重新编写调度或重试规则。
// Select 安排首次入队；Transition 移动队列里的任务；Execute 让执行器做事；
// Return/Release 先让队列处理旧 Unit 的关联，再删除 Buffer 的旧记录。
func (b *Buffer) Apply(ctx context.Context, d staging.Decision) error {
	if err := b.prepareDecision(ctx, d); err != nil {
		return err
	}
	switch d.Action {
	case staging.ActionWait:
		return nil
	case staging.ActionSelect:
		return b.applySelect(ctx, d)
	case staging.ActionTransition:
		return b.applyTransition(ctx, d)
	case staging.ActionExecute:
		return b.applyExecute(ctx, d)
	case staging.ActionReturn, staging.ActionRelease:
		return b.applyHandoff(ctx, d)
	default:
		return fmt.Errorf("未知动作: %q", d.Action)
	}
}
