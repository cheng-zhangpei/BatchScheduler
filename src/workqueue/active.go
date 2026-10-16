package workqueue

import (
	"context"
	"sync"
)

// activeQueue 先使用 FIFO；pending 只对仍在队列中的单元去重。
// 取出后再次 Add 会留下下一次处理机会，尚不管理正在处理的单元。
type activeQueue struct {
	mu      sync.Mutex
	items   []Unit
	pending map[UnitKey]bool
	changed chan struct{}
}

func newActiveQueue() *activeQueue {
	return &activeQueue{pending: make(map[UnitKey]bool), changed: make(chan struct{})}
}

func (q *activeQueue) Add(unit Unit) {
	key := unit.Key()
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pending[key] {
		return
	}
	q.items = append(q.items, unit)
	q.pending[key] = true
	// 唤醒正在等条目的 Pop，然后为下一次等待准备新通知。
	close(q.changed)
	q.changed = make(chan struct{})
}

func (q *activeQueue) Pop(ctx context.Context) (Unit, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		q.mu.Lock()
		if len(q.items) > 0 {
			unit := q.items[0]
			q.items[0] = nil
			q.items = q.items[1:]
			delete(q.pending, unit.Key())
			q.mu.Unlock()
			return unit, nil
		}
		changed := q.changed
		q.mu.Unlock()
		// 在锁内取得通知通道，避免检查完空队列后错过 Add 的通知。
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}
