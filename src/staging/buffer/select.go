package buffer

import (
	"context"
	"fmt"

	"batchscheduler/src/staging"
)

// applySelect 把需要入队的组交给 WorkQueue.Select，由它检查重复归属并组装 Unit。
func (b *Buffer) applySelect(ctx context.Context, d staging.Decision) error {
	if d.Unit != nil {
		return fmt.Errorf("Select 决策不能操作 Center 已持有的 Unit")
	}
	if b.handlers.SelectGroup == nil {
		return staging.ErrHandlerUnavailable
	}
	view, err := b.views.Get(d.Group.Namespace, d.Group.Name)
	if err != nil {
		return err
	}
	if view == nil || view.Group == nil || view.Group.UID != d.Group.UID || view.Group.DeletionTimestamp != nil {
		return staging.ErrStaleDecision
	}
	return b.handlers.SelectGroup(ctx, view)
}
