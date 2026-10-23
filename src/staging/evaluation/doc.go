// Package evaluation 只决定一个执行记录下一步该做什么，不修改记录，也不调用外部组件。
// 从 record.go 的 EvaluateRecord 开始，依次查看预处理、情况汇总、完成、错误、正常执行。
// 返回的 Decision 交回 buffer.Apply 执行。
package evaluation
