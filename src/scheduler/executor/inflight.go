package executor

import (
	"context"
	"sync"
)

// Inflight 是调度与实际绑定之间的交接区，队列里保存已选点、已 Assume 的任务。
// worker 取出一轮，执行 Bind 和 Report；调度线程只负责 Submit，不直接启动绑定。
type Inflight struct {
	tasks   chan *executionAttempt
	slots   chan struct{}
	workers int
	done    sync.WaitGroup
}

func newInflight(config Config) *Inflight {
	return &Inflight{tasks: make(chan *executionAttempt, config.MaxInFlight),
		slots: make(chan struct{}, config.MaxInFlight), workers: config.BindWorkers}
}

// Reserve 在调度前取得容量名额。满了就在这里等，不先 Assume 一批任务再等空位。
// 名额包括正在调度、已经入队等待绑定、正在绑定的轮次。
func (i *Inflight) Reserve(ctx context.Context) error {
	select {
	case i.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Submit 把这一轮交给 worker；调用者必须已经 Reserve，之后不再修改这个 Attempt。
// 每个名额只提交一次，队列容量覆盖所有名额，因此这里无需等待 worker 完成绑定。
func (i *Inflight) Submit(a *executionAttempt) { i.tasks <- a }

// Release 在调度失败或后台任务结束后，归还容量名额。
func (i *Inflight) Release() { <-i.slots }

func (i *Inflight) start(ctx context.Context, executor *Executor) {
	for range i.workers {
		i.done.Add(1)
		go func() {
			defer i.done.Done()
			for a := range i.tasks {
				err := executor.runBinding(ctx, a)
				executor.completeRequest(ctx, a.Request, err)
				i.Release()
			}
		}()
	}
}

// closeAndWait 在调度线程停止提交后调用，等队列中的任务及正在执行的 worker 全部退出。
func (i *Inflight) closeAndWait() {
	close(i.tasks)
	i.done.Wait()
}
