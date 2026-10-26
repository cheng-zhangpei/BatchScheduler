// Package buffer 保存 Unit 的执行记录，连接队列、集群观察和执行器。
//
// 阅读主线：Accept 建记录 → Observe 更新集群事实 → Evaluate 产生决定 → Apply 调用对应组件。
// 执行器持续调用 Report 更新进展；调度成功先归档到 history，再交接队列并清理活跃记录。
// 这些方法操作同一份 Buffer，放在一个包内，不再增加中间回调层。
//
// 每个文件对应一件事；纯决策规则放在相邻的 evaluation 包，错误规则放在 error_engine 包。
package buffer
