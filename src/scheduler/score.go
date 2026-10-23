package scheduler

import (
	"context"

	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
)

// PodScore 只给已经通过 Filter 的候选节点评分，不选择最终节点。
type PodScore interface {
	Score(context.Context, *corev1.Pod, []string) ([]NodeScore, error)
}

// NodeScore 是一个 Pod 对一个候选节点的基础评分，分数越大越优先。
type NodeScore struct {
	Node         string
	Score        int64            // 各插件归一化后，乘权重再相加的总分。
	PluginScores map[string]int64 // 各插件归一化后的分数，尚未乘权重，便于解释排序。
}

// GroupScoreInput 把整轮的 Pod 评分交给后续组规划。
// Mode 来自冻结的 Unit；这里只透传，Atomic/Partial 的组评分规则下一步再实现。
type GroupScoreInput struct {
	Mode      workqueue.SubmitMode
	PodScores map[workqueue.PodRef][]NodeScore // 每个列表按总分从高到低排列，保留所有候选。
}
