package staging

import (
	"context"
	"errors"
	"fmt"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
)

var (
	ErrStaleDecision      = errors.New("评估决策已过期，需重新评估")
	ErrHandlerUnavailable = errors.New("动作适配器尚未接入")
	ErrHandoffUnknown     = errors.New("交接结果未知，须确认后再释放")
)

// Handlers 保存外部函数，NewBuffer 时传进来，启动后不要更换。
// “注入”只表示给字段赋一个函数；调用这些字段不会自动新建 goroutine。
type Handlers struct {
	// QueueSnapshot 查询指定组在 Fail、Retry 里的 Unit；只读，不取出任务。
	QueueSnapshot func(group.Key) []workqueue.QueuedUnit
	// WaitingGroups 查询哪些组在 Fail、Retry 中还有任务，供定时检查和事件通知使用。
	WaitingGroups func() []group.Key
	// Transition 调用 WorkQueue.Transition，真正移动 Fail/Retry 中的任务。
	Transition func(context.Context, workqueue.TransitionRequest) error
	// Execute 接收调度/预占/绑定/恢复请求，完成后由执行器调用 Report。
	// 返回 error 时可能已经接收了请求，Buffer 会保留 Pending 等进一步确认。
	Execute func(context.Context, Execution) error
	// SelectGroup 调用 WorkQueue.Select，判断是否需要创建 Unit 并首次入队。
	SelectGroup func(context.Context, *group.View) error
	// Handoff 在旧 Unit 结束时处理队列里的旧关联，失败回送时还要接住下一轮任务。
	// 返回 nil 才表示这些事已做完；普通 error 必须表示完全没做成，才能安全重试。
	// 如果不知道做成没有，返回 ErrHandoffUnknown，等查清后再调用 ConfirmHandoff。
	// 当前真实实现没接上，不能用“直接调用 Select”假装它已经完成。
	Handoff func(context.Context, Handoff) error
}

// Execution 是发给执行器的一张任务单，说明本次操作的编号、成员和节点计划。
type Execution struct {
	ActionID  string
	Unit      workqueue.UnitKey
	Operation Operation
	Members   []workqueue.PodRef
	Plan      map[workqueue.PodRef]string
	Failure   *group.FailureInfo
}

// Handoff 是发给队列处理函数的结束通知：哪个旧 Unit，要回送还是只结束。
// Partial 即使只重试部分成员，也要明确处理旧 Unit 其他成员的记录，不能直接丢掉。
type Handoff struct {
	ActionID string
	Unit     workqueue.Unit
	Decision Decision
}

var _ Center = (*Buffer)(nil)

// Release 是“请求结束这个 Unit”的入口，不是强制删除。
// 先确认 Evaluate 允许 Release，再通过 Apply 让队列处理旧关联，最后删除 Buffer 记录。
func (b *Buffer) Release(ctx context.Context, key workqueue.UnitKey) error {
	ds, err := b.Evaluate(ctx, key.Group)
	if err != nil {
		return err
	}
	for _, d := range ds {
		if d.Unit != nil && *d.Unit == key && d.Action == ActionRelease {
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
	if r == nil || actionID == "" || r.PendingAction != actionID || (r.PendingDecision != ActionReturn && r.PendingDecision != ActionRelease) {
		return fmt.Errorf("交接确认身份不匹配")
	}
	if committed {
		if err := r.Unit.MarkFinished(); err != nil {
			return err
		}
		b.removeRecord(key)
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
