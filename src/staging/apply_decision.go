package staging

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"batchscheduler/src/workqueue"
)

// Apply 根据 Decision 调用对应函数，不在这里重新编写调度或重试规则。
// Select 安排首次入队；Transition 移动队列里的任务；Execute 让执行器做事；
// Return/Release 先让队列处理旧 Unit 的关联，再删除 Buffer 的旧记录。
func (b *Buffer) Apply(ctx context.Context, d Decision) error {
	if err := b.prepareDecision(ctx, d); err != nil {
		return err
	}
	switch d.Action {
	case ActionWait:
		return nil
	case ActionSelect:
		return b.applySelect(ctx, d)
	case ActionTransition:
		return b.applyTransition(ctx, d)
	case ActionExecute:
		return b.applyExecute(ctx, d)
	case ActionReturn, ActionRelease:
		return b.applyHandoff(ctx, d)
	default:
		return fmt.Errorf("未知动作: %q", d.Action)
	}
}

// prepareDecision 检查“刚才做出的决定，现在还能不能执行”。
// Record 的决定重新读缓存、评估并比较；队列迁移的版本由 WorkQueue 自己检查。
func (b *Buffer) prepareDecision(ctx context.Context, d Decision) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.Action == ActionWait {
		return nil
	}
	if d.Action == ActionTransition {
		// 请求中的组、Unit 编号必须对应，且必须带上做决定时看到的队列版本。
		if d.Unit == nil || d.Transition == nil || d.Transition.Unit == nil ||
			d.Transition.Unit.Key() != *d.Unit || d.Group != d.Unit.Group || d.Transition.ExpectedRevision == 0 {
			return fmt.Errorf("Transition 决策需要匹配的 Unit 身份和队列侧版本")
		}
		return nil
	}
	if err := b.observe(ctx, Observation{Group: d.Group}, false); err != nil {
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
	return ErrStaleDecision
}

// applySelect 读取当前组，交给 WorkQueue.Select 判断要不要入队。
// 即使同一个组被反复通知，也由 Select 检查是否已经有 Unit，避免重复创建。
func (b *Buffer) applySelect(ctx context.Context, d Decision) error {
	if d.Unit != nil {
		return fmt.Errorf("Select 决策不能操作 Center 已持有的 Unit")
	}
	if b.handlers.SelectGroup == nil {
		return ErrHandlerUnavailable
	}
	view, err := b.views.Get(d.Group.Namespace, d.Group.Name)
	if err != nil {
		return err
	}
	if view == nil || view.Group == nil || view.Group.UID != d.Group.UID || view.Group.DeletionTimestamp != nil {
		return ErrStaleDecision
	}
	return b.handlers.SelectGroup(ctx, view)
}

// applyTransition 让 WorkQueue 按请求移动任务，例如 Fail → Retry。
// 这些任务还在队列里，所以不需要给它们创建 Buffer.Record 或 Pending。
func (b *Buffer) applyTransition(ctx context.Context, d Decision) error {
	if b.handlers.Transition == nil {
		return ErrHandlerUnavailable
	}
	b.mu.RLock()
	_, held := b.records[*d.Unit]
	b.mu.RUnlock()
	if held {
		// Buffer 还在执行这个 Unit 时，不能同时把它当作等待任务搬队列。
		return fmt.Errorf("Unit 仍由 Center 执行，不能同时迁移队列")
	}
	err := b.handlers.Transition(ctx, *d.Transition)
	if err == nil || errors.Is(err, workqueue.ErrRetryNotReady) {
		// 队列已处理了提示，或已记住“相关变化发生过但还没到退避时间”。
		// 所以可以删掉 Buffer 这边暂存的同一条提示。
		b.mu.Lock()
		delete(b.retryHints, *d.Unit)
		b.mu.Unlock()
	}
	if errors.Is(err, workqueue.ErrRetryNotReady) {
		// 只是还要等时间/条件，不是执行故障，不需要 main 把它当错误报告。
		return nil
	}
	return err
}

// beginAction 给 Record 写上“action-N 这件事正在做”，并准备好交给外部的参数。
// 检查旧版本和登记 Pending 放在同一段受保护的代码里，防止两个调用都启动操作。
func (b *Buffer) beginAction(d Decision) (Execution, Handoff, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if d.Unit == nil {
		return Execution{}, Handoff{}, fmt.Errorf("操作缺少 Unit")
	}
	r := b.records[*d.Unit]
	if r == nil || r.Revision != d.Revision || r.PendingAction != "" {
		return Execution{}, Handoff{}, ErrStaleDecision
	}
	b.nextAction++
	id := fmt.Sprintf("action-%d", b.nextAction)
	r.PendingAction, r.PendingOperation, r.PendingDecision = id, d.Operation, d.Action
	r.Revision++
	members := append([]workqueue.PodRef(nil), d.Members...)
	if len(members) == 0 {
		members = r.Unit.Members()
	}
	// 这里复制成员、计划和错误说明，外部修改参数不会直接改掉 Record 的这些字段。
	request := Execution{ActionID: id, Unit: *d.Unit, Operation: d.Operation,
		Members: members, Plan: copyPlan(r.Plan), Failure: copyFailure(d.Failure)}
	return request, Handoff{ActionID: id, Unit: r.Unit, Decision: d}, nil
}

// applyExecute 把请求交给执行器。执行器完成后调用 Report 告诉我们结果。
// 调用本身是同步的；是否交给后台线程执行，由执行器实现决定。
// Execute 返回 error 也可能已经启动，所以这里不清 Pending，不马上再发一次。
func (b *Buffer) applyExecute(ctx context.Context, d Decision) error {
	if b.handlers.Execute == nil {
		return ErrHandlerUnavailable
	}
	request, _, err := b.beginAction(d)
	if err != nil {
		return err
	}
	return b.handlers.Execute(ctx, request)
}

// applyHandoff 通知队列：“旧 Unit 要结束了，请处理旧关联；Return 时还要接住重试任务”。
// 队列函数返回后，再由 finishHandoff 决定是否删除 Buffer 里的旧记录。
func (b *Buffer) applyHandoff(ctx context.Context, d Decision) error {
	if b.handlers.Handoff == nil {
		return ErrHandlerUnavailable
	}
	_, request, err := b.beginAction(d)
	if err != nil {
		return err
	}
	err = b.handlers.Handoff(ctx, request)
	return b.finishHandoff(d, request.ActionID, err)
}

// finishHandoff 处理队列函数的返回结果：
// nil：队列已经处理好，删旧记录；普通 error：确定没做成，保留记录并允许下次重试；
// ErrHandoffUnknown：不知道做成没有，保留 Pending，等查清楚后调用 ConfirmHandoff。
func (b *Buffer) finishHandoff(d Decision, id string, handoffErr error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.records[*d.Unit]
	if r == nil || r.PendingAction != id {
		return fmt.Errorf("交接记录已改变")
	}
	if handoffErr != nil {
		if !errors.Is(handoffErr, ErrHandoffUnknown) {
			// 确定没做成，才取消等待标记；否则下次重发可能重复入队。
			r.PendingAction, r.PendingOperation, r.PendingDecision = "", "", ""
			r.Revision++
		}
		return handoffErr
	}
	// 在unit层面标记已经完成
	if err := r.Unit.MarkFinished(); err != nil {
		return err
	}
	// 在buffer中将record移出去
	// 队列确认处理完成后，才删掉自己这边的记录；不是在这里重试或释放资源。
	b.removeRecord(*d.Unit)
	return nil
}
