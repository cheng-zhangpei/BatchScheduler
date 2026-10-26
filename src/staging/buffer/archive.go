package buffer

import (
	"context"
	"fmt"
	"time"

	"batchscheduler/src/staging"
)

// archiveSuccess 在队列交接和内存清理之前保存成功快照。
// Release 也会用于清理已失效的轮次；只有 AttemptComplete 才表示真正调度成功。
func (b *Buffer) archiveSuccess(ctx context.Context, d staging.Decision, actionID string) error {
	if d.Action != staging.ActionRelease || d.Reason != "AttemptComplete" {
		return nil
	}
	if b.history == nil {
		return fmt.Errorf("%w: 成功记录存储未接入", staging.ErrHandlerUnavailable)
	}
	b.mu.RLock()
	r := b.records[*d.Unit]
	if r == nil || r.PendingAction != actionID || r.Observed == nil || !r.ResponsibilitiesSettled {
		b.mu.RUnlock()
		return fmt.Errorf("成功归档时本轮记录已变化或尚未收尾")
	}
	record := staging.SuccessfulRecord{ID: r.ID, Unit: r.Unit.Key(), Mode: r.Unit.Mode(),
		Generation: r.Generation, Revision: r.Revision, SavedAt: time.Now().UTC()}
	for _, ref := range r.Unit.Members() {
		observed := r.Observed.Members[ref]
		observed.Status = *observed.Status.DeepCopy()
		observed.Failure = copyFailure(observed.Failure)
		state := r.Members[ref]
		state.LastFailure = copyFailure(state.LastFailure)
		record.Members = append(record.Members, staging.SuccessfulMember{Pod: ref, State: state, Observed: observed})
	}
	b.mu.RUnlock()
	// 不占着 Buffer 的修改权限等磁盘；Pending 仍在，重复评估不会再次执行这轮。
	if err := b.history.Save(ctx, record); err != nil {
		return fmt.Errorf("保存调度成功记录: %w", err)
	}
	return nil
}
