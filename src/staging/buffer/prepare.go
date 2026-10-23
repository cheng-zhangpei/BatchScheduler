package buffer

import (
	"context"
	"fmt"
	"reflect"

	"batchscheduler/src/staging"
)

// prepareDecision 检查“刚才做出的决定，现在还能不能执行”。
// Record 的决定重新读缓存、评估并比较；队列迁移的版本由 WorkQueue 自己检查。
func (b *Buffer) prepareDecision(ctx context.Context, d staging.Decision) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.Action == staging.ActionWait {
		return nil
	}
	if d.Action == staging.ActionTransition {
		// 请求中的组、Unit 编号必须对应，且必须带上做决定时看到的队列版本。
		if d.Unit == nil || d.Transition == nil || d.Transition.Unit == nil ||
			d.Transition.Unit.Key() != *d.Unit || d.Group != d.Unit.Group || d.Transition.ExpectedRevision == 0 {
			return fmt.Errorf("Transition 决策需要匹配的 Unit 身份和队列侧版本")
		}
		return nil
	}
	if err := b.observe(ctx, staging.Observation{Group: d.Group}, false); err != nil {
		return err
	}
	// 当前实现每次都重评，比较保守；还没有优化成“观察没变化就跳过”。
	current, err := b.Evaluate(ctx, d.Group)
	if err != nil {
		return err
	}
	for _, candidate := range current {
		if reflect.DeepEqual(candidate, d) {
			return nil
		}
	}
	return staging.ErrStaleDecision
}

// applySelect 读取当前组，交给 WorkQueue.Select 判断要不要入队。
