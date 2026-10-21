package workqueue

import (
	"sort"

	"batchscheduler/src/group"
)

// QueuedUnit 是“刚才查到的一条 Fail/Retry 任务信息”。查询不会把任务取出队列。
// State 等字段是当时读到的值，Unit 仍指向原对象；执行迁移时必须再查版本。
type QueuedUnit struct {
	Unit    Unit
	State   UnitState
	Failure Failure
	Retry   Retry
}

// GroupSnapshot 查询这个组目前在 Fail、Retry 中有哪些 Unit，以及错误和等待条件。
// 先保护好队列再读取，避免正搬到一半时读到前后不一致的位置。
func (w *WorkQueue) GroupSnapshot(key group.Key) []QueuedUnit {
	w.selectMu.Lock()
	defer w.selectMu.Unlock()
	var snapshots []QueuedUnit
	if record := w.groups[key]; record != nil {
		for _, unit := range record.units {
			state := unit.State()
			entry := QueuedUnit{Unit: unit, State: state}
			switch state.Queue {
			case QueueFail:
				failure, ok := w.Fail.Get(unit.Key())
				if !ok {
					continue
				}
				entry.Failure = failure
			case QueueRetry:
				retry, ok := w.Retry.Get(unit.Key())
				if !ok {
					continue
				}
				entry.Retry = retry
			default:
				continue
			}
			snapshots = append(snapshots, entry)
		}
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Unit.Key().ID < snapshots[j].Unit.Key().ID })
	return snapshots
}

// WaitingGroups 返回 Fail、Retry 中还有任务的组编号，不包含只有 Active 任务的组。
func (w *WorkQueue) WaitingGroups() []group.Key {
	w.selectMu.Lock()
	defer w.selectMu.Unlock()
	var keys []group.Key
	for key, record := range w.groups {
		for _, unit := range record.units {
			queue := unit.State().Queue
			if queue == QueueFail || queue == QueueRetry {
				keys = append(keys, key)
				break
			}
		}
	}
	return keys
}
