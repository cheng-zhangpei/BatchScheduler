package scheduler

import (
	"context"

	scheduling "batchscheduler/src/apis/scheduling/v1alpha1"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
	corev1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
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

// GroupScoreInput 把本轮成员、节点信息和 Pod 评分一起交给后续组规划。
// Mode 来自冻结的 Unit；这里只透传，Atomic/Partial 的组评分规则下一步再实现。
type GroupScoreInput struct {
	Mode       workqueue.SubmitMode
	Placement  *scheduling.PlacementSpec        // 本轮冻结的通信关系和资源整合开关；nil 表示未声明偏好。
	Pods       []*corev1.Pod                    // 本轮完整成员对象，与 PodScores 用名称和 UID 对应。
	NodeInfos  map[string]fwk.NodeInfo          // 节点名 → 本轮节点信息；资源统计是独立副本，可供后续试放。
	PodScores  map[workqueue.PodRef][]NodeScore // 每个列表按总分从高到低排列，保留所有候选。
	Context    staging.GroupContext             // 同组其他活跃 Unit，以及已归档成功记录。
	BoundPeers []*corev1.Pod                    // 本轮缓存中已绑定且未结束的同组成员；历史位置不自动变成这个列表。
}
