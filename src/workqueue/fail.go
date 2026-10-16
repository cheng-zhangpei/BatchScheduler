package workqueue

import "sync"

// failQueue 当前是按调度单元查询的失败记录表，不按 FIFO 消费。
type failQueue struct {
	mu    sync.Mutex
	items map[UnitKey]Failure
}

func (q *failQueue) Put(failure Failure) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items[failure.Unit.Key()] = failure
}

func (q *failQueue) Get(key UnitKey) (Failure, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	failure, exists := q.items[key]
	return failure, exists
}

func (q *failQueue) Remove(key UnitKey) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.items, key)
}
