package workqueue

import "sync"

// retryQueue 保存等待条件的组，暂不实现条件匹配、计时或自动唤醒。
type retryQueue struct {
	mu    sync.Mutex
	items map[UnitKey]Retry
}

func (q *retryQueue) Put(retry Retry) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items[retry.Unit.Key()] = retry
}

func (q *retryQueue) Get(key UnitKey) (Retry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	retry, exists := q.items[key]
	return retry, exists
}

func (q *retryQueue) Remove(key UnitKey) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.items, key)
}
