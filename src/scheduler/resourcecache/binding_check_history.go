package resourcecache

import (
	"container/list"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// bindingCheckHistory 保存已经释放暂占的旧执行记录，最近使用的放在队头。
// 它不保存待核查记录，不修改节点资源账，也不决定 Buffer 如何结束 Pending。
// 接入 Cache 后，由 Cache.mu 保护这里的读写；查询会调整 LRU 顺序，也需要写锁。
type bindingCheckHistory struct {
	capacity int
	order    *list.List
	entries  map[BindingCheckKey]*list.Element
	byPod    map[types.UID]map[BindingCheckKey]struct{}
}

func newBindingCheckHistory(capacity int) *bindingCheckHistory {
	return &bindingCheckHistory{
		capacity: capacity,
		order:    list.New(),
		entries:  make(map[BindingCheckKey]*list.Element),
		byPod:    make(map[types.UID]map[BindingCheckKey]struct{}),
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
	if h.byPod[record.Key.Pod.UID] == nil {
		h.byPod[record.Key.Pod.UID] = make(map[BindingCheckKey]struct{})
	}
	h.byPod[record.Key.Pod.UID][record.Key] = struct{}{}
	if h.order.Len() > h.capacity {
		oldest := h.order.Back()
		h.remove(oldest.Value.(BindingCheck).Key)
	}
}

// empty 是 O(1) 判断；普通 Pod 通知在没有历史时不做额外查询。
func (h *bindingCheckHistory) empty() bool { return len(h.entries) == 0 }

// observeBound 在实际资源记账后移除该 Pod 的旧执行记录，不扫描整个 LRU。
func (h *bindingCheckHistory) observeBound(pod *corev1.Pod) {
	for key := range h.byPod[pod.UID] {
		if key.Pod.Name == pod.Name && key.Unit.Group.Namespace == pod.Namespace {
			h.remove(key)
		}
	}
}

func (h *bindingCheckHistory) remove(key BindingCheckKey) {
	entry := h.entries[key]
	if entry == nil {
		return
	}
	h.order.Remove(entry)
	delete(h.entries, key)
	delete(h.byPod[key.Pod.UID], key)
	if len(h.byPod[key.Pod.UID]) == 0 {
		delete(h.byPod, key.Pod.UID)
	}
}

// SetBindingHistoryCapacity 在启动时设置历史容量，缩小时按 LRU 顺序淘汰。
func (c *Cache) SetBindingHistoryCapacity(capacity int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := c.releasedChecks
	h.capacity = max(0, capacity)
	for h.order.Len() > h.capacity {
		h.remove(h.order.Back().Value.(BindingCheck).Key)
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
