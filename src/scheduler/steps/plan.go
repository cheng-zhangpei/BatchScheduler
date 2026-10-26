package steps

import (
	"context"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/workqueue"
)

// PlanGroup 是 Executor 到组规划的入口，内部环节由 planner 按顺序组织。
// 输入来自前面的 Score；只有选出最终计划后，Executor 才继续 Assume、Bind。
func (r *Runner) PlanGroup(ctx context.Context, _ *Attempt, input scheduler.GroupScoreInput) (map[workqueue.PodRef]string, error) {
	return r.Planner.Plan(ctx, input)
}
