package steps

import (
	"context"
	"fmt"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

// PlanGroup 接收提交模式和各 Pod 的基础评分，生成整轮放置计划。
func (r *Runner) PlanGroup(_ context.Context, _ *Attempt, input scheduler.GroupScoreInput) (map[workqueue.PodRef]string, error) {
	// TODO：根据 input.Mode、input.PodScores 设计组评分和组内试放。
	// Pod 基础分没有计算本轮成员之间的资源竞争，不能直接各取最高分就结束。
	return nil, fmt.Errorf("%w: Pod Score 已完成，组评分和组选点尚未实现（mode=%s）", staging.ErrHandlerUnavailable, input.Mode)
}
