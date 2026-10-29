package executor

import (
	"context"
	"testing"
	"time"

	"batchscheduler/src/staging"
)

// 只有一个 worker 时，后续两轮应留在 Inflight 等待，而不是另开 goroutine 绕过队列。
func TestInflightQueuesAttemptsBehindBindingWorker(t *testing.T) {
	e, resources, requests := asyncBindingFixture(3, "16")
	e.config.MaxInFlight, e.config.BindWorkers = 3, 1
	assumed := make(chan struct{}, 3)
	binding := make(chan struct{}, 3)
	release := make(chan struct{})
	e.Report = func(ctx context.Context, f staging.Feedback) error {
		if f.Phase == staging.PhaseAssumed {
			assumed <- struct{}{}
		}
		if f.Phase == staging.PhaseBinding {
			binding <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	startAsyncExecutor(t, e)
	defer close(release)
	for _, request := range requests {
		if err := e.Execute(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		awaitBindingEvent(t, assumed)
	}
	awaitBindingEvent(t, binding)
	select {
	case <-binding:
		t.Fatal("等待任务绕过了单个绑定 worker")
	case <-time.After(100 * time.Millisecond):
	}
	if resources.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 3000 {
		t.Fatal("Inflight 中等待的计划没有保留暂占")
	}
}

func TestExecutorRejectsInvalidInflightConfig(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.MaxInFlight = 0 },
		func(c *Config) { c.BindWorkers = 0 },
		func(c *Config) { c.BindWorkers = c.MaxInFlight + 1 },
		func(c *Config) { c.BindTimeout = 0 },
	} {
		config := DefaultConfig()
		mutate(&config)
		if err := NewExecutor(nil, nil, config).Run(context.Background()); err == nil {
			t.Fatalf("非法配置未被拒绝: %+v", config)
		}
	}
}
