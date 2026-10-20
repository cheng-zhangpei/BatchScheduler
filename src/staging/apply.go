package staging

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
)

var (
	ErrStaleDecision      = errors.New("评估决策已过期，需重新评估")
	ErrHandlerUnavailable = errors.New("动作适配器尚未接入")
	ErrHandoffUnknown     = errors.New("交接结果未知，须确认后再释放")
)

// Handlers 在 NewBuffer 时注入，运行中不能更换。
// Execute 只负责接收/派发操作；实际结果通过 Report 返回，可同步回调 Report。
// Execute 返回错误表示接收结果尚未确认，保留 PendingAction，不能盲目重发。
// Handoff 负责队列侧原子交接：Return 创建下一轮并经 Select 接收，Release 解除
// workqueue 的旧归属；必须按旧 UnitKey 幂等，不能直接改 frozen Unit 或盲调 Transition。
// nil 表示队列侧已接收责任；普通 error 保证未提交；不确定是否提交须返回
// ErrHandoffUnknown，保留记录等待确认。真实适配器本步不实现。
// Return 即使只重排失败子集，也必须结清或承接旧 Unit 其余成员的责任后才能确认，
// 不能仅把 Decision.Members 放进队列就返回成功，导致其余成员丢失归属。
type Handlers struct {
	Execute     func(context.Context, Execution) error
	SelectGroup func(context.Context, *group.View) error
	Handoff     func(context.Context, Handoff) error
}

type Execution struct {
	ActionID  string
	Unit      workqueue.UnitKey
	Operation Operation
	Members   []workqueue.PodRef
	Plan      map[workqueue.PodRef]string
	Failure   *group.FailureInfo
}

type Handoff struct {
	ActionID string
	Unit     workqueue.Unit
	Decision Decision
}

var _ Center = (*Buffer)(nil)

// Apply 是决策翻译器：先刷新并重评，再登记动作，最后在锁外调用适配器。
// 同版本的重复决策只有一个能登记，后来的调用不会重复启动绑定或交接。
func (b *Buffer) Apply(ctx context.Context, requested Decision) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if requested.Action == ActionWait {
		return nil
	}
	if err := b.observe(ctx, Observation{Group: requested.Group}, false); err != nil {
		return err
	}
	current, err := b.Evaluate(ctx, requested.Group)
	if err != nil {
		return err
	}
	var selected *Decision
	for i := range current {
		if reflect.DeepEqual(current[i], requested) {
			selected = &current[i]
			break
		}
	}
	if selected == nil {
		return ErrStaleDecision
	}
	d := *selected
	if d.Unit == nil {
		if d.Action != ActionReturn || b.handlers.SelectGroup == nil {
			return ErrHandlerUnavailable
		}
		view, err := b.views.Get(d.Group.Namespace, d.Group.Name)
		if err != nil {
			return err
		}
		if view == nil || view.Group == nil || view.Group.UID != d.Group.UID || view.Group.DeletionTimestamp != nil {
			return ErrStaleDecision
		}
		// 无本地 Unit 的组级入口由 Select 自己去重，不虚构暂存归属。
		return b.handlers.SelectGroup(ctx, view)
	}
	b.mu.Lock()
	r := b.records[*d.Unit]
	if r == nil || r.Revision != d.Revision || r.PendingAction != "" {
		b.mu.Unlock()
		return ErrStaleDecision
	}
	if (d.Action == ActionExecute && b.handlers.Execute == nil) || ((d.Action == ActionReturn || d.Action == ActionRelease) && b.handlers.Handoff == nil) {
		b.mu.Unlock()
		return ErrHandlerUnavailable
	}
	if d.Action != ActionExecute && d.Action != ActionReturn && d.Action != ActionRelease {
		b.mu.Unlock()
		return fmt.Errorf("未知动作: %q", d.Action)
	}
	b.nextAction++
	id := fmt.Sprintf("action-%d", b.nextAction)
	r.PendingAction, r.PendingOperation, r.PendingDecision = id, d.Operation, d.Action
	r.Revision++
	if d.Action == ActionExecute {
		members := append([]workqueue.PodRef(nil), d.Members...)
		if len(members) == 0 {
			members = r.Unit.Members()
		}
		request := Execution{ActionID: id, Unit: *d.Unit, Operation: d.Operation, Members: members, Plan: copyPlan(r.Plan), Failure: copyFailure(d.Failure)}
		b.mu.Unlock()
		return b.handlers.Execute(ctx, request)
	}
	handoff := Handoff{ActionID: id, Unit: r.Unit, Decision: d}
	b.mu.Unlock()
	err = b.handlers.Handoff(ctx, handoff)
	b.mu.Lock()
	defer b.mu.Unlock()
	r = b.records[*d.Unit]
	if r == nil || r.PendingAction != id {
		return fmt.Errorf("交接记录已改变")
	}
	if err != nil {
		if !errors.Is(err, ErrHandoffUnknown) {
			r.PendingAction, r.PendingOperation, r.PendingDecision = "", "", ""
			r.Revision++
		}
		return err
	}
	if err := r.Unit.MarkFinished(); err != nil {
		return err
	}
	b.removeRecord(*d.Unit)
	return nil
}

// Release 同样经过重评和队列侧归属交接，不能直接删除 map。
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

// ConfirmHandoff 用于适配器核实未知交接结果后确认，不重新执行交接动作。
// 必须对应原 ActionID；committed=false 明确证明未提交，才允许重新评估交接。
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
		r.PendingAction, r.PendingOperation, r.PendingDecision = "", "", ""
		r.Revision++
		b.evaluations.Add(key.Group)
	}
	return nil
}

// removeRecord 在持锁且交接确认后调用；最后一个 Unit 离开时同步删除组索引。
func (b *Buffer) removeRecord(key workqueue.UnitKey) {
	for ref := range b.records[key].Members {
		if b.owners[key.Group][ref.UID] == key {
			delete(b.owners[key.Group], ref.UID)
		}
	}
	delete(b.records, key)
	delete(b.groups[key.Group], key)
	if len(b.groups[key.Group]) == 0 {
		delete(b.groups, key.Group)
		delete(b.owners, key.Group)
	}
}

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

func copyFailure(f *group.FailureInfo) *group.FailureInfo {
	if f == nil {
		return nil
	}
	result := *f
	return &result
}
