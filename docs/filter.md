# Filter 适配：先掌握怎么调用

当前 Execute 已接到 Buffer，内部 schedule 的 Step 1 已经真实调用过滤器。Filter 从共享 ResourceCache 取得本轮快照，后面的 Pod Score 和通信规划沿用同一份资源视图。首版可选出唯一计划并批量 Assume；Bind 尚未实现，当前会先 Forget，再报告 Blocked。

## 输入和输出

```go
snapshot := resources.Snapshot()
single, err := upstream.NewPodFilterFromSnapshot(ctx, snapshot.Nodes)
// 先处理 err，再构造组过滤器。
filter := steps.NewGroupFilter(single)
results, err := filter.Filter(ctx, unitPods)
```

- `snapshot.Nodes`：共享资源账中的节点及累计占用，包含真实绑定和本地暂占；执行步骤已调用缓存的批量 Assume。
- `snapshot.BoundPods`：同一次快照中已经确认绑定的 Pod，供 Filter 提取同组运行成员；不把暂占当成真实绑定。
- `unitPods`：这一个 Unit 本轮要调度的完整 Pod 对象，由调用方从组视图取出。
- `results`：按 Pod 名称和 UID 查询每个成员的候选节点、未通过的节点及原因。

一个 Pod 没有候选节点，仍会收集其他成员的结果。程序执行出错则返回 error，不能把它当成资源不足。
这一步没有选择最终节点，没有预留资源，也不代表组已经可以提交。

## 我们自己写的部分

1. `src/scheduler/resourcecache/snapshot.go / Snapshot`：一次取得节点资源账和已确认绑定的 Pod。`upstream/filter_snapshot.go` 把 NodeInfo 按名称排列、保留既有硬约束检查，再交给原生插件，不重算资源账。
2. `src/scheduler/upstream/filter.go / PodFilter.Filter`：每个 Pod 建一份 CycleState，调用原生 PreFilter，再逐节点调用原生 Filter，转换结果。处理 PreFilter 的 Skip 和候选范围限制。
3. `src/scheduler/steps/group_filter.go / GroupFilter.Filter`：对本轮每个 Pod 调用上述单 Pod 过滤，把结果收集起来。

先看懂这三步即可。过滤器内部的资源计算和匹配规则来自上游。

节点视图在构造时固定。`resourcecache.Cache.Assume` 将一批占用一起写入共享资源账，下一轮快照会包含它们，不依赖 informer 的绑定通知。`steps.Assume` 已组装本轮成员、节点和执行编号调用它，成功才设置 Attempt.Reserved。`steps.Recover` 在确认本轮没有提交绑定时调用 `Cache.Forget` 撤销暂占。

批量登记在最新账的节点副本上逐个检查资源和 HostPort，全部通过后一起写回 nodes、pods、assumed；任一成员失败都不改变共享账。同一执行的相同计划重复调用沿用旧占用，不重复计算。它不重新运行整个 Filter，也不向 API Server 申请集群范围的资源锁。

Forget 接收 Unit、ActionID 和成员引用，按 UID 查缓存里的 Assumption，用保存的 Pod 从目标节点副本撤销占用，再统一删除 pods、assumed 记录。重复调用不再退账，已确认绑定的成员保留实际占用；归属不一致则整次不修改。调用方仍须先确认可以撤销，Forget 不自行判断绑定结果。已发出的绑定或新的 Recover 请求接手旧执行仍未接入实际恢复，不能直接用新 ActionID 撤销旧暂占。

Assume 返回的资源/端口不满足由执行器映射为内部 Unschedulable，目标节点消失映射为 NodeUnavailable，进入恢复、Report、Evaluate、回送 Fail 的原有路径；Fail 再通过 Transition 等待相关条件变化。暂未细分 CPU 与端口错误，不解析错误字符串。

2026-10-27：`scheduler/resourcecache` 已通过 informer 的 Node、已绑定 Pod 事件维护共享资源账。main 等初始记账回调完成才启动执行与评估线程，并把同一实例传给 Executor → Runner。Filter 已改读共享资源快照，不从 Node/Pod lister 重建占用；组声明和当前待调度成员仍从 GroupView 读取。`NewPodFilter(nodes, pods)` 保留给独立适配测试，运行路径使用新的快照入口。

## 本次接入哪些规则

| 原生插件 | 检查什么 |
| --- | --- |
| NodeName | Pod 已声明的节点名称 |
| NodeUnschedulable | 节点是否禁止调度，以及相应容忍 |
| TaintToleration | 节点污点能否容忍 |
| NodeAffinity | nodeSelector 和节点硬亲和性 |
| NodePorts | 宿主机端口是否冲突 |
| NodeResourcesFit | CPU、内存、扩展资源、Pod 数量等资源是否足够 |

没有启用完整默认插件集。Pod 间硬亲和性、硬拓扑分布、需要存储过滤的卷和 DRA 先明确报错；既有 Pod 的硬反亲和性同样不能忽略。尚未解除的 schedulingGates 也会阻止过滤。
软偏好留给后续选择/评分阶段，本次不计算。

## 第三方代码和当前进度

`third_party/kubernetes` 保留 v1.34.7 原生源码，54 个 Go 文件未经修改。`SOURCE.json` 记录来源和每个文件的校验值，`LICENSE` 保留上游许可证；本地 go.mod 只列该子集所需依赖。

当前已用真实插件测试资源占用、init 容器与 overhead、扩展资源、Pod 数量、节点选择、污点、禁止调度和端口冲突，以及每个成员独立的准备状态。

执行模块分成三个目录：

| 目录 | 看什么 |
| --- | --- |
| `src/scheduler/executor/` | 请求接收、工作线程、步骤顺序、Report |
| `src/scheduler/steps/` | Filter、Pod Score、组选点、Assume、Bind、Recover 的具体实现 |
| `src/scheduler/upstream/` | 原生插件的输入准备和调用适配 |

从 `executor/schedule.go` 看整条执行线即可，不必先读插件内部：

```text
Buffer.Apply → Executor.Execute（接收进内部队列，快速返回）
  Executor.Run 工作线程：
    Step 1：steps.Filter → GroupFilter → upstream.PodFilter → 原生插件
    Step 2：steps.Score → 原生 Pod 评分列表
    Step 3：steps.PlanGroup，通信搜索生成候选并选定唯一计划
    Step 4：steps.Assume → Cache.Assume，批量暂占 → Report Assumed
    Step 5：steps.Bind，TODO
    失败时：Executor 自己调用 steps.Recover
  每个进展和整轮结束 → 同一个 Buffer.Report
```

Buffer 只提交一次 Run，同一轮始终使用同一个 ActionID。Progress 只更新记录并保留 Pending；Finished 且资源已结清才结束本轮等待。Buffer 不会再派发 Assume、Bind。执行器也不直接搬 workqueue。

当前完整路径是 Filter → Score → PlanGroup → Assume → Report Assumed → Bind 占位返回“未开始”。Executor 确认绑定未提交后进入 Recover，Forget 撤销暂占，再报告 Blocked/Recovered 且资源已结清。Buffer 保留执行记录并停止等待开发，不把缺少 Bind 当成业务失败放进 Fail。

真实的过滤或 Assume 冲突则报告失败，完成本轮收尾后回 Fail。Report 调用失败不等于调度或绑定失败，现有路径保留暂占及 Buffer 记录，不擅自 Forget；后续对账仍待实现。

尚未实现：多语义最终取舍、真实 Bind 及已提交绑定的恢复、旧执行接手和 Report 失败对账，以及 Active 消费循环。本次没有运行集群测试。

Pod 评分的调用和输入输出见 [Pod Score](score.md)。
