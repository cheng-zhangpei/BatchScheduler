package buffer

import (
	"fmt"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
	"k8s.io/apimachinery/pkg/types"
)

// acceptAtomicMembers 把后到成员汇入尚未启动的 Atomic Record。
// 例如先收到 A、B，再收到 C、D，最终只启动包含 A、B、C、D 的一次 Run。
// 调用时已持有 Buffer 写锁；它不访问 WorkQueue。
func (b *Buffer) acceptAtomicMembers(incoming workqueue.Unit, definition *group.Definition) (bool, error) {
	key := incoming.Key().Group
	for recordKey := range b.groups[key] {
		r := b.records[recordKey]
		if r.Unit.Mode() != workqueue.AllTogether || r.Generation != definition.Generation {
			return false, fmt.Errorf("本组仍有其他模式或声明版本的执行记录")
		}
		if r.PendingAction != "" || r.ExecutionPhase != "" || r.ReturningMembers || r.Failure != nil || len(r.Plan) != 0 {
			return false, fmt.Errorf("Atomic 已启动或正在结束，不能追加成员")
		}
		for _, ref := range incoming.Members() {
			if _, held := b.owners[key][ref.UID]; held {
				return false, fmt.Errorf("Atomic 成员 %s 已经收到，不能重复汇入", ref.Name)
			}
		}

		// 先准备完整名单，再改 Record。准备失败时原 Record 不变。
		combined, err := combineWaitingAtomic(r.Unit, incoming, definition)
		if err != nil {
			return false, err
		}
		if err := incoming.MarkProcessing(); err != nil {
			return false, err
		}
		if err := incoming.MarkFinished(); err != nil {
			return false, err
		}
		counts := combined.State().FailedAttempts
		for _, ref := range incoming.Members() {
			r.Members[ref] = group.PodState{Phase: group.PodUnscheduled, UnitID: recordKey.ID, FailedAttempts: counts[ref.UID]}
			b.owners[key][ref.UID] = recordKey
		}
		r.AcceptedUnits[incoming.Key()] = incoming.Members()
		r.Unit = combined
		// 新成员还没有观察结果，下一次 Observe 按合并后的名单重新填写。
		r.Observed = nil
		r.Revision++
		b.evaluations.Add(key)
		return true, nil
	}
	return false, nil
}

// combineWaitingAtomic 为尚未执行的一轮重建完整 Unit，沿用主 Record 的编号。
// 它不改两个输入对象；真正执行后不再使用这条合并路径。
func combineWaitingAtomic(current, incoming workqueue.Unit, definition *group.Definition) (workqueue.Unit, error) {
	combined := workqueue.NewUnit(current.Key(), workqueue.AllTogether)
	if err := combined.SetDefinition(definition); err != nil {
		return nil, err
	}
	counts := make(map[types.UID]int)
	for _, source := range []workqueue.Unit{current, incoming} {
		for _, ref := range source.Members() {
			if err := combined.AddMember(ref); err != nil {
				return nil, err
			}
		}
		for uid, count := range source.State().FailedAttempts {
			counts[uid] = count
		}
	}
	if len(combined.Members()) > int(definition.Spec.Members) {
		return nil, fmt.Errorf("Atomic 收到的成员超过组声明数量")
	}
	if err := combined.SetFailedAttempts(counts); err != nil {
		return nil, err
	}
	if err := combined.MarkQueued(workqueue.QueueActive, nil, ""); err != nil {
		return nil, err
	}
	if err := combined.MarkProcessing(); err != nil {
		return nil, err
	}
	return combined, nil
}

func compareAcceptedUnit(actual, requested workqueue.SubmitMode, accepted []workqueue.PodRef, refs map[workqueue.PodRef]bool) (bool, error) {
	if actual != requested || len(accepted) != len(refs) {
		return true, fmt.Errorf("重复 Accept 的模式或成员发生变化")
	}
	for _, ref := range accepted {
		if !refs[ref] {
			return true, fmt.Errorf("重复 Accept 的成员发生变化")
		}
	}
	return true, nil
}
