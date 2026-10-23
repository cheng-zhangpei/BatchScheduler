package executor

import (
	"context"
	"errors"
	"testing"

	"batchscheduler/src/staging"
)

func TestRunStopsWhenBufferRejectsProgress(t *testing.T) {
	e := NewExecutor(nil, nil, nil)
	want := errors.New("buffer rejected progress")
	calls := 0
	e.Report = func(_ context.Context, f staging.Feedback) error {
		calls++
		if f.Phase != staging.PhaseFiltering {
			t.Fatalf("反馈被拒绝后仍进入了恢复或结束流程: %+v", f)
		}
		return want
	}
	err := e.runExecution(context.Background(), staging.Execution{Operation: staging.OperationRun})
	if !errors.Is(err, want) || calls != 1 {
		t.Fatalf("汇报失败应直接停止，不应伪装成调度失败: calls=%d err=%v", calls, err)
	}
}
