package planner

import (
	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
)

// PlanResult 保存本轮选定的规划结果，此时尚未 Assume 或 Bind。
// Placements 是找到位置的成员；Unplaced 是本轮没有找到位置的成员及原因。
// 两部分合起来覆盖本轮输入成员，每个成员只出现在其中一部分。
// Atomic 要求 Unplaced 为空才继续；Partial 可以继续处理已放置部分。
// 这里只表达结果，不修改冻结 Unit，也不创建下一轮 Unit 或操作队列。
type PlanResult struct {
	Placements map[workqueue.PodRef]string
	Unplaced   []UnplacedMember
}

// UnplacedMember 说明哪个成员本轮没有找到位置，以及调度器内部如何分类。
// 没找到位置不代表整个集群一定没有可行解；原因应按实际掌握的信息填写。
type UnplacedMember struct {
	Pod     workqueue.PodRef
	Failure group.FailureInfo
}
