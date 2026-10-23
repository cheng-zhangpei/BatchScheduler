# Filter 适配：先掌握怎么调用

当前 Execute 已接到 Buffer，内部 schedule 的 Step 1 已经真实调用过滤器。后面已经接入 Pod Score；组评分和组选点还没实现。

## 输入和输出

```go
single, err := upstream.NewPodFilter(ctx, nodes, assignedPods)
// 先处理 err，再构造组过滤器。
filter := steps.NewGroupFilter(single)
results, err := filter.Filter(ctx, unitPods)
```

- `nodes`：本轮候选节点。
- `assignedPods`：集群中已分配节点的 Pod，包括其他调度器管理的普通 Pod，用来计算已有资源和端口占用。
- `unitPods`：这一个 Unit 本轮要调度的完整 Pod 对象，由调用方从组视图取出。
- `results`：按 Pod 名称和 UID 查询每个成员的候选节点、未通过的节点及原因。

一个 Pod 没有候选节点，仍会收集其他成员的结果。程序执行出错则返回 error，不能把它当成资源不足。
这一步没有选择最终节点，没有预留资源，也不代表组已经可以提交。

## 我们自己写的部分

1. `src/scheduler/upstream/snapshot.go / buildNodeInfos`：复制 Node、已分配 Pod，交给原生 NodeInfo 统计资源和端口。不会改 informer 对象。
2. `src/scheduler/upstream/filter.go / PodFilter.Filter`：每个 Pod 建一份 CycleState，调用原生 PreFilter，再逐节点调用原生 Filter，转换结果。处理 PreFilter 的 Skip 和候选范围限制。
3. `src/scheduler/steps/group_filter.go / GroupFilter.Filter`：对本轮每个 Pod 调用上述单 Pod 过滤，把结果收集起来。

先看懂这三步即可。过滤器内部的资源计算和匹配规则来自上游。

节点视图在构造时固定；下一轮、或组内试放改变资源后，需要使用更新的视图。Assume 的本地占用尚未实现，后面也必须合入这份资源账，避免重复分配。

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
    Step 3：steps.PlanGroup，组评分和组选点 TODO
    Step 4：steps.Assume，TODO
    Step 5：steps.Bind，TODO
    失败时：Executor 自己调用 steps.Recover
  每个进展和整轮结束 → 同一个 Buffer.Report
```

Buffer 只提交一次 Run，同一轮始终使用同一个 ActionID。Progress 只更新记录并保留 Pending；Finished 且资源已结清才结束本轮等待。Buffer 不会再派发 Assume、Bind。执行器也不直接搬 workqueue。

当前过滤和 Pod 基础评分成功后，组评分和组选点还没实现，报告 Blocked：保留执行记录，不报告调度成功，也不自动重新执行。过滤确认失败时，由执行器内部恢复；此时尚无预留或绑定，所以可以直接结清，再交回错误原因。真正持有资源时的 Recover 仍未实现。

尚未实现：组内放置、完整计划、Assume/Bind/有资源时的 Recover、本地 Assume/Forget 和 Active 消费循环。本次没有运行集群测试。

Pod 评分的调用和输入输出见 [Pod Score](score.md)。
