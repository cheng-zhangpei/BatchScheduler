package resourcecache

import "container/list"

// bindingCheckHistory 保存已经释放暂占的旧执行记录，最近使用的放在队头。
// 它不保存待核查记录，不修改节点资源账，也不决定 Buffer 如何结束 Pending。
// 接入 Cache 后，由 Cache.mu 保护这里的读写；查询会调整 LRU 顺序，也需要写锁。
type bindingCheckHistory struct {
	capacity int
	order    *list.List
	entries  map[BindingCheckKey]*list.Element
}

func newBindingCheckHistory(capacity int) *bindingCheckHistory {
	return &bindingCheckHistory{
		capacity: capacity,
		order:    list.New(),
		entries:  make(map[BindingCheckKey]*list.Element),
	}
}

// remember 在暂占实际释放后保存记录。容量不足时，只删除最久未使用的历史。
// 淘汰历史不调用 Forget；晚到的已绑定 Pod 仍必须正常通过 UpdatePod 记账。
// capacity <= 0 表示不保留历史。
func (h *bindingCheckHistory) remember(record BindingCheck) {
	if h.capacity <= 0 {
		return
	}
	if entry := h.entries[record.Key]; entry != nil {
		entry.Value = record
		h.order.MoveToFront(entry)
		return
	}
	h.entries[record.Key] = h.order.PushFront(record)
	if h.order.Len() > h.capacity {
		oldest := h.order.Back()
		delete(h.entries, oldest.Value.(BindingCheck).Key)
		h.order.Remove(oldest)
	}
}

// find 返回记录的值副本，并将命中的记录移到队头。
func (h *bindingCheckHistory) find(key BindingCheckKey) (BindingCheck, bool) {
	entry := h.entries[key]
	if entry == nil {
		return BindingCheck{}, false
	}
	h.order.MoveToFront(entry)
	return entry.Value.(BindingCheck), true
}
