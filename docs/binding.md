# Binding 与 Inflight

这一节只讲调度结束后，任务如何交给绑定 worker，以及结果如何回到 Buffer。

## 主线

```text
Buffer.Apply
  → Executor.Execute：接收一次整轮请求
  → Executor.Run：从执行请求队列取出请求
      → Inflight.Reserve：先为这一轮取得容量名额
      → schedule：Filter → Score → Plan → Assume → Report Assumed
      → Inflight.Submit：把已准备好的 Attempt 放入绑定队列
      → 继续调度下一轮

Inflight 的固定 worker
  → 从绑定队列取出 Attempt
  → runBinding：Report Binding → steps.Bind → Report 结果
  → 删除请求接收标记，归还容量名额

Informer 观测到真实绑定
  → ResourceCache.UpdatePod：实际占用接替暂占，不重复累计
  → Buffer.Observe → Evaluate → Apply Release
  → 保存成功历史，完成队列交接，再删除本轮 Record
```

调度线程不会直接调用 `steps.Bind`。绑定 API 的等待发生在 Inflight worker 中；等待期间，串行调度线程仍可处理其他任务，直到容量用完。

## Inflight 保存什么

实现位于 `src/scheduler/executor/inflight.go`。`tasks` 是实际的有界任务队列，保存 `*executionAttempt`。里面有本轮请求、成员列表、最终 Pod → Node 计划、暂占标记和执行进度。

`slots` 只负责限制数量。它不是任务队列，也不保存绑定结果。调度之前调用 Reserve，调度成功后 Submit，worker 做完 Report 后 Release。调度阶段失败、不需要进入绑定时，Executor 自己 Release。

容量名额覆盖“正在调度 + 等待绑定 + 正在绑定”。这样一轮 Assume 成功后，一定有位置进入 tasks，不会先占住资源，再发现交接区没有地方放。

每个 worker 每次拿一轮，调用 `runBinding`，做完再取下一轮。Executor 不为每个任务临时开绑定 goroutine。一个 Attempt 在 Submit 后只交给一个 worker 修改。

## 参数

配置定义在 `executor/config.go`，main 的命令行参数直接填入配置：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `--max-inflight` | 4 | 调度、等待绑定和正在绑定的最大总轮数 |
| `--bind-workers` | 2 | 同时执行绑定的 worker 数量 |
| `--bind-timeout` | 30s | 一个 worker 开始处理一轮后，整轮 Bind 的最长等待时间，不含队列等待 |

参数必须为正，worker 数不能超过容量。比如 `--max-inflight=16 --bind-workers=4` 允许 4 轮同时绑定，其余名额可以容纳调度中和等待中的任务。这个限制按 Unit 的一轮执行计算，不是按 Pod 数计算。

## steps.Bind 如何提交

实现位于 `src/scheduler/steps/bind.go`，分为两段：

1. `bindingRequests` 根据冻结的成员引用和最终 Plan，先组装整轮请求。每个 Binding 包含 namespace、Pod 名称、Pod UID 和目标 Node。
2. `Bind` 按成员顺序调用 `Client.CoreV1().Pods(namespace).Bind(ctx, binding, options)`，等待 API 返回，再提交下一个成员。遇到第一个错误就停。

同一轮不另外开 Pod 级 goroutine；不同轮之间由 worker 池并行。这个 API 调用与上游 DefaultBinder 的直接客户端路径相同，使用的是 Pod 的 binding 子资源，不是直接修改 informer 中的 Pod。

所有成员的 API 请求都成功后，worker 报告 Finished / Bound。这里的成功是节点分配成功，不表示容器已经 Running。Buffer 还要等 Observe 看到真实绑定，才产生完成清理决策。

Bind 不直接撤销缓存中的暂占。Informer 更新到来前，原来的资源占用继续有效；更新到来后，UpdatePod 保留实际占用并移除 assumed 身份。这期间不会出现 Bind 一成功就把资源退给其他任务的空档。

## 失败如何回去

`steps/bind_error.go` 将 API 错误转成内部分类。Executor 收到 BindingError 后统一进入 Recover。

| API 结果 | 内部分类 | 本步行为 |
| --- | --- | --- |
| Forbidden / Unauthorized | Forbidden | 没有成功成员时 Forget，回 Fail，不自动重试 |
| Invalid / BadRequest / MethodNotSupported | InvalidSpec | 没有成功成员时 Forget，回 Fail |
| NotFound | ObjectDeleted | 没有成功成员时撤销暂占；后续评估根据对象是否还存在处理旧轮次 |
| TooManyRequests | BindingRejected | 没有成功成员时 Forget，回 Fail，再按已有策略进入带退避的 Retry |
| Conflict、超时、网络错误、5xx 或其他未确认结果 | BindingUnknown | Recover 保留资源和 Pending，报告 Unknown，不重发绑定 |
| 前面的成员成功，后面的成员失败 | 保留具体错误与成功成员列表 | Recover 报告 Blocked，保留记录，不宣称已回滚 |

`Attempt.Binding` 表示某个请求的结果尚未确认；`Attempt.BoundMembers` 记录已经得到成功响应的成员。Recover 据此区分“完全没有绑定，可以 Forget”和“已经影响集群，不能只改本地账”。

部分成功的 Atomic 回滚、Partial 成功/失败成员分别交接，以及未知结果的 API 查询对账，本步尚未实现。因此成功路径和没有成员成功时的明确拒绝路径已经接通，不等于所有失败恢复都已完成。

## 退出与当前边界

Executor 停止提交后关闭 Inflight 队列，并等待 worker 退出，之后 main 才能关闭 Buffer 的数据库。取消上下文会传到 Bind 网络请求。停机期间若 Report 因上下文取消而失败，仍不能把尚未确认的资源当成已经清理；重启恢复和失败 Report 对账仍待实现。

main 已传入真实 Kubernetes 客户端，执行路径可以发送真实 Binding 请求，需要 `pods/binding` 的 create 权限。缓存同步后，main 启动 `runActive` 自动完成 Active.Pop → Buffer.Accept，Accept 再通知评估线程。FastRetry 消费仍未启用。Accept 失败时，当前实现把 Unit 放回 Active 并让 main 返回错误退出；过期 Unit 的自动重组后续单独实现。

测试覆盖真实 client-go HTTP 请求的路径与 UID、Inflight 队列和 worker 上限、绑定等待时后续调度、成功回报与观测后的归档清理、明确拒绝的 Fail/Retry 流程，以及未知结果和部分成功不会被误报为已回滚。没有连接真实集群测试。
