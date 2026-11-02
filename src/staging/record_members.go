package staging

import "batchscheduler/src/workqueue"

// ActiveMembers 返回本轮仍负责处理的成员，保持原 Unit 的成员顺序。
// C、D 交回后，Unit 的冻结名单还是 A、B、C、D，但本轮只继续跟踪 A、B。
func (r *Record) ActiveMembers() []workqueue.PodRef {
	var members []workqueue.PodRef
	for _, ref := range r.Unit.Members() {
		if !r.ReturnedMembers[ref] {
			members = append(members, ref)
		}
	}
	return members
}
