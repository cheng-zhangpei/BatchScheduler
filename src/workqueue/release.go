package workqueue

import "fmt"

// ReleaseUnit 删除一轮执行结束后，WorkQueue 里留下的旧 Unit 和成员关联。
// 调用者必须先由 Buffer 确认可以 Release；Processing 本身不代表工作已完成。
// 本方法不删除 Pod、不释放资源，也不把 Unit 标 Done，最后一步由 Buffer 完成。
func (w *WorkQueue) ReleaseUnit(key UnitKey) error {
	if key.ID == "" || key.Group.Namespace == "" || key.Group.Name == "" || key.Group.UID == "" {
		return fmt.Errorf("ReleaseUnit 缺少完整的组和 Unit 编号")
	}
	w.selectMu.Lock()
	defer w.selectMu.Unlock()

	// 旧记录已经删掉，就算重复清理成功。不影响同组其他 Unit。
	record := w.groups[key.Group]
	if record == nil || record.units[key.ID] == nil {
		return nil
	}
	unit := record.units[key.ID]
	state := unit.State()
	// 仍在等待队列里的任务不能直接删除。这里只接受已交给 Buffer 处理的旧轮次。
	if state.Phase != UnitProcessing || state.Queue != QueueNone {
		return fmt.Errorf("ReleaseUnit 只清理 Processing 且 Queue=None 的单元，当前为 %s/%s", state.Phase, state.Queue)
	}

	removeUnit(record, unit)
	// 组内已没有任何 Unit 时，删除这份队列管理记录；真实组仍在 informer 中。
	if len(record.units) == 0 {
		delete(w.groups, key.Group)
	}
	return nil
}

// removeUnit 只删这个旧 Unit 的索引和成员状态；Release 和 Select 的回送分支共用。
// 不删除组目录，回送时还要在同一目录登记新 Unit。调用时已持有 selectMu。
func removeUnit(record *selectionState, unit Unit) {
	key := unit.Key()
	// 删除旧成员的内部状态（含 Pod → 旧 Unit 的关联），避免失败和预占状态留给下一轮。
	// informer 中的 Pod 不受影响；已关联到其他 Unit 的成员记录也保留。
	for _, ref := range unit.Members() {
		if pod := record.pods[ref.UID]; pod != nil && pod.UnitID == key.ID {
			delete(record.pods, ref.UID)
		}
	}
	delete(record.units, key.ID)
	delete(record.group.UnitIDs, key.ID)
}
