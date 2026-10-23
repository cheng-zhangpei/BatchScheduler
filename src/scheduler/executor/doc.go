// Package executor 接收一次整轮执行请求，按顺序调用 steps，并通过同一个 Report 报告进展。
//
// executor.go 接收请求和启动工作线程；schedule.go 写步骤顺序；flow.go 处理成功、失败和回报。
// Buffer 只提交一次 Run，Filter、Pod Score、组规划、Assume、Bind 和失败恢复都由这里组织。
package executor
