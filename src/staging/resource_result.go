package staging

import (
	"slices"

	"batchscheduler/src/group"
	"batchscheduler/src/workqueue"
)

// ResourceResult 把本次资源接纳结果分为两部分，供 Buffer 后续决定如何交接。
// 例如原 Unit 有 A、B、C：A、B 暂占成功放入 Assumed，C 未暂占放入 Unplaced。
// 两部分覆盖本次处理的全部成员，每个成员只出现一次，不改变原 Unit 的冻结成员。
// 这是结果说明，不代表 Buffer 已经接住回送成员，也不代表整轮执行结束。
type ResourceResult struct {
	Assumed  []workqueue.PodRef // 已在 ResourceCache 记账；可以继续绑定，尚不代表绑定成功。
	Unplaced []UnplacedMember   // 没有遗留暂占或在途绑定，可以交给 Buffer 安排下一轮的成员。
}

// UnplacedMember 保留未放置成员及内部错误分类，后续按 Failure.Reason 决策。
// Failure.Message 只解释发生了什么，不通过匹配文字决定是否重试。
type UnplacedMember struct {
	Pod     workqueue.PodRef
	Failure group.FailureInfo
}

// DeepCopy 复制两份成员名单，发送方之后修改自己的结果不会改变 Buffer 的记录。
func (r *ResourceResult) DeepCopy() *ResourceResult {
	if r == nil {
		return nil
	}
	return &ResourceResult{
		Assumed:  slices.Clone(r.Assumed),
		Unplaced: slices.Clone(r.Unplaced),
	}
}
