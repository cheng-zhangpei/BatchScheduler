package workqueue

import (
	"context"
	"fmt"
)

// DispatchActive 取出一个 Unit，固定成员，交给 Buffer，并删除队列中的关联。
// accept 必须是本地接收函数，不能回调 WorkQueue，也不能在这里调度或访问 API。
func (w *WorkQueue) DispatchActive(ctx context.Context, accept func(context.Context, Unit) error) error {
	var unit Unit
	for {
		candidate, err := w.Active.Pop(ctx)
		if err != nil {
			return err
		}
		w.selectMu.Lock()
		if len(candidate.Members()) != 0 {
			unit = candidate
			break
		}
		// 排队期间 Pod 可能全部被删除或已被外部绑定；这条空 FIFO 条目直接结束。
		w.removeQueuedUnit(candidate)
		w.selectMu.Unlock()
	}
	defer w.selectMu.Unlock()

	// 从固定成员到 Buffer 登记归属，再到删除队列索引，Select 都不能穿插进来。
	// 因此不会有“队列已删、Buffer 还没接住”的空档，也不用再建一份交接中的索引。
	if err := unit.Freeze(); err != nil {
		w.Active.Add(unit)
		return err
	}
	if err := accept(ctx, unit); err != nil {
		// 接收失败，旧关联还在；重新打开 Active 成员集合并放回，交给调用方处理错误。
		if restoreErr := unit.MarkQueued(QueueActive, unit.State().LastFailure, ""); restoreErr != nil {
			return fmt.Errorf("接收失败: %w；恢复 Active 失败: %v", err, restoreErr)
		}
		w.Active.Add(unit)
		return fmt.Errorf("Unit %s 未进入 Buffer，已放回 Active: %w", unit.Key().ID, err)
	}
	w.removeQueuedUnit(unit)
	return nil
}

// removeQueuedUnit 只删队列索引，不改 Unit。此后执行进度由 Buffer 管理。
// 调用时已持有 selectMu。
func (w *WorkQueue) removeQueuedUnit(unit Unit) {
	key := unit.Key()
	record := w.groups[key.Group]
	if record == nil {
		return
	}
	removeUnit(record, unit)
	if len(record.units) == 0 {
		delete(w.groups, key.Group)
	}
}
