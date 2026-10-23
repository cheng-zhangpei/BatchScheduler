# Staging 源码导航

先分清四个目录：

| 位置 | 职责 | 从哪里开始读 |
| --- | --- | --- |
| 当前目录 | 共用数据和对外接口 | `types.go`、`contracts.go`、`center.go` |
| `buffer/` | 保存执行记录，接收观察，执行决定，记录反馈和清理 | `doc.go`、`buffer.go` |
| `evaluation/` | 按阶段决定下一步，只返回 Decision | `record.go` |
| `error_engine/` | 把原生错误映射成内部错误，再按类型返回处理意见 | `observed_error.go`、`engine.go` |

## 跟着一个 Unit 看

1. `buffer/buffer.go`：Accept 接收出队的 Unit，建立 Record 和成员关联。
2. `buffer/observe.go`：读取集群缓存，更新 Observed，通知评估。
3. `buffer/evaluate.go`：检查 Buffer 中的 Unit、等待重试的 Unit、尚未接收的 Pod。
4. `evaluation/record.go`：一个 Unit 按预处理、完成、错误、正常执行四步产生决定。
5. `buffer/apply.go`：按 Action 分发。具体调用分别在 select、transition、execute、handoff 文件里。
6. `buffer/report.go`：执行器每有进展就更新记录；中间进展不结束 Pending。
7. `buffer/handoff.go`、`buffer/release.go`：队列先处理旧关联，成功后 Buffer 删除旧记录。

## 执行器在哪里

Apply 只提交一次 Run。`scheduler/executor/` 自己组织 Filter → Pod Score → PlanGroup → Assume → Bind，失败时内部 Recover。
同一次执行使用同一个 ActionID，通过一个 Report 接口持续回报。Buffer 不再逐步派发 Assume、Bind。

目前已实现过滤和 Pod 基础评分。组评分、组选点、真实预留、绑定、有资源时的恢复和 Active 消费循环尚未实现。
详细说明见 [Buffer 源码阅读说明](../../docs/buffer.md)。
