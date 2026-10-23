package buffer

import (
	"fmt"

	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// beginAction 给 Record 写上“action-N 这件事正在做”，并准备好交给外部的参数。
// 检查旧版本和登记 Pending 放在同一段受保护的代码里，防止两个调用都启动操作。
func (b *Buffer) beginAction(d staging.Decision) (staging.Execution, staging.Handoff, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if d.Unit == nil {
		return staging.Execution{}, staging.Handoff{}, fmt.Errorf("操作缺少 Unit")
	}
	r := b.records[*d.Unit]
	if r == nil || r.Revision != d.Revision || r.PendingAction != "" {
		return staging.Execution{}, staging.Handoff{}, staging.ErrStaleDecision
	}
	b.nextAction++
	id := fmt.Sprintf("action-%d", b.nextAction)
	r.PendingAction, r.PendingOperation, r.PendingDecision = id, d.Operation, d.Action
	if d.Action == staging.ActionExecute {
		r.ExecutionPhase, r.ExecutionBlocked = "", false
		r.RecoveryComplete, r.ResponsibilitiesSettled = false, false
		// 外部观测触发恢复时，保存这次失败的成员范围，后续进展不会把 Partial 扩成整组错误。
		if d.Operation == staging.OperationRecover && d.Failure != nil && r.Failure == nil {
			for _, ref := range d.Members {
				state := r.Members[ref]
				state.LastFailure = copyFailure(d.Failure)
				r.Members[ref] = state
			}
		}
	}
	r.Revision++
	members := append([]workqueue.PodRef(nil), d.Members...)
	if len(members) == 0 {
		members = r.Unit.Members()
	}
	// 这里复制成员、计划和错误说明，外部修改参数不会直接改掉 Record 的这些字段。
	request := staging.Execution{ActionID: id, Unit: *d.Unit, Operation: d.Operation,
		Mode: r.Unit.Mode(), Members: members, Plan: copyPlan(r.Plan), Failure: copyFailure(d.Failure)}
	return request, staging.Handoff{ActionID: id, Unit: r.Unit, Decision: d}, nil
}
