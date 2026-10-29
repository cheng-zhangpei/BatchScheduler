// Package executor 接收一次整轮执行请求，按顺序调用 steps，并通过同一个 Report 报告进展。
//
// executor.go 接收请求并串行调度；schedule.go 走到 Assume，再 Submit 给 Inflight。
// inflight.go 保存等待任务，并用固定 worker 调用 binding.go；flow.go 处理恢复和回报。
// Buffer 只提交一次 Run，Filter、Pod Score、组规划、Assume、Bind 和失败恢复都由这里组织。
package executor
