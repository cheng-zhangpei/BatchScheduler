package utils

import "batchscheduler/src/group"

// ShouldEnqueue 是进入 Select 前的过滤入口，true 表示继续处理。
func ShouldEnqueue(view *group.View) bool {
	// TODO：讨论并实现过滤规则；当前仅占位，默认放行。
	return true
}
