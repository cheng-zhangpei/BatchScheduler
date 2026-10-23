package buffer

import (
	"context"
	"fmt"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

var _ staging.Center = (*Buffer)(nil)

// Release 是“请求结束这个 Unit”的入口，不是强制删除。
// 先确认 Evaluate 允许 Release，再通过 Apply 让队列处理旧关联，最后删除 Buffer 记录。
func (b *Buffer) Release(ctx context.Context, key workqueue.UnitKey) error {
	ds, err := b.Evaluate(ctx, key.Group)
	if err != nil {
		return err
	}
	for _, d := range ds {
		if d.Unit != nil && *d.Unit == key && d.Action == staging.ActionRelease {
			return b.Apply(ctx, d)
		}
	}
	return fmt.Errorf("单元尚不满足释放条件或已不在暂存区")
}

// ConfirmHandoff 用于补充确认：“刚才不知道队列有没有处理成功，现在查清了”。
// committed=true：队列已处理好，删除旧 Record；false：确定什么也没做成，允许再试。
// 它不是必经步骤；Handoff 当场返回 nil 时，finishHandoff 已经完成清理。
func (b *Buffer) ConfirmHandoff(ctx context.Context, key workqueue.UnitKey, actionID string, committed bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.records[key]
	if r == nil || actionID == "" || r.PendingAction != actionID || (r.PendingDecision != staging.ActionReturn && r.PendingDecision != staging.ActionRelease) {
		return fmt.Errorf("交接确认身份不匹配")
	}
	if committed {
		if err := r.Unit.MarkFinished(); err != nil {
			return err
		}
		b.removeRecord(key)
		if r.PendingDecision == staging.ActionReturn {
			// 和当场回送成功一样，继续检查 Partial 剩余未绑定成员。
			b.evaluations.Add(key.Group)
		}
	} else {
		// 已确认队列还没做，所以取消等待标记，并通知评估重新安排。
		r.PendingAction, r.PendingOperation, r.PendingDecision = "", "", ""
		r.Revision++
		b.evaluations.Add(key.Group)
	}
	return nil
}

// removeRecord 真正删除本轮执行记录、Pod → Unit 关联和组内 Unit 编号。
// 调用前必须确认队列处理已完成；它不删除集群里的 Pod，也不释放节点资源。
// 调用者已经保护好这些 map，所以这里不再自己申请锁。
func (b *Buffer) removeRecord(key workqueue.UnitKey) {
	for ref := range b.records[key].Members {
		// 只删仍指向旧 Unit 的关联，避免误删后来属于另一个 Unit 的关联。
		if b.owners[key.Group][ref.UID] == key {
			delete(b.owners[key.Group], ref.UID)
		}
	}
	delete(b.records, key)
	delete(b.groups[key.Group], key)
	if len(b.groups[key.Group]) == 0 {
		// 这个组在 Buffer 中没有任务了，连空的组目录也一起删掉。
		delete(b.groups, key.Group)
		delete(b.owners, key.Group)
	}
}

// copyPlan 复制节点计划，防止执行器改 map 时顺便改到 Record 里的原计划。
func copyPlan(plan map[workqueue.PodRef]string) map[workqueue.PodRef]string {
	if plan == nil {
		return nil
	}
	result := make(map[workqueue.PodRef]string, len(plan))
	for ref, node := range plan {
		result[ref] = node
	}
	return result
}

// copyFailure 复制错误说明，外部修改自己的副本不会改到 Buffer 内部记录。
func copyFailure(f *group.FailureInfo) *group.FailureInfo {
	if f == nil {
		return nil
	}
	result := *f
	return &result
}
