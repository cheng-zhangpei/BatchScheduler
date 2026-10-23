package workqueue

import (
	"fmt"

	"batchscheduler/src/group"
)

// newUnit 创建一个尚未登记、尚未入队的空单元。普通组装和失败回送共用它。
// 调用时已持有 selectMu，保证编号不会重复。
func (w *WorkQueue) newUnit(key group.Key, mode SubmitMode) (Unit, error) {
	w.nextID++
	unitKey := UnitKey{Group: key, ID: fmt.Sprintf("unit-%d", w.nextID)}
	unit := w.MakeUnit(unitKey, mode)
	if unit == nil || unit.Key() != unitKey || unit.Mode() != mode || unit.State().Phase != UnitNew || len(unit.Members()) != 0 {
		return nil, fmt.Errorf("单元工厂必须返回身份和模式匹配的组装中空单元")
	}
	return unit, nil
}

// prepareEnqueue 冻结成员并登记目标队列，还不把 Unit 放进实际队列。
// 失败回送先完成这一步，确定能入队后才替换旧记录，避免处理一半返回错误。
func prepareEnqueue(unit Unit, decision QueueDecision) error {
	if !isQueue(decision.To) {
		return fmt.Errorf("Select 收到未知目标队列: %q", decision.To)
	}
	if err := unit.Freeze(); err != nil {
		return err
	}
	var failure *group.FailureInfo
	if decision.Reason != "" || decision.Message != "" {
		failure = &group.FailureInfo{Reason: decision.Reason, Message: decision.Message}
	}
	return unit.MarkQueued(decision.To, failure, decision.WaitingFor)
}

// publishUnit 把准备好的 Unit 放入实际队列；组内索引必须已经登记好。
// 这些队列的 Add/Put 当前没有失败返回，调用后就表示队列已经接住任务。
func (w *WorkQueue) publishUnit(unit Unit, decision QueueDecision) {
	switch decision.To {
	case QueueActive:
		w.Active.Add(unit)
	case QueueFail:
		w.Fail.Put(Failure{Unit: unit, Reason: decision.Reason, Message: decision.Message, RecoveryComplete: decision.RecoveryComplete})
	case QueueRetry:
		w.Retry.Put(Retry{Unit: unit, WaitingFor: decision.WaitingFor})
	case QueueFastRetry:
		w.FastRetry.Add(unit)
	}
}
