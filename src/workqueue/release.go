package workqueue

// removeUnit 删除待调度 Unit 的索引。调用时已持有 selectMu。
// 它不删除 Pod，也不替 Buffer 结束执行；正常在 DispatchActive 交出任务后调用。
func removeUnit(record *selectionState, unit Unit) {
	key := unit.Key()
	for _, ref := range unit.Members() {
		if pod := record.pods[ref.UID]; pod != nil && pod.UnitID == key.ID {
			delete(record.pods, ref.UID)
		}
	}
	delete(record.units, key.ID)
	delete(record.group.UnitIDs, key.ID)
}
