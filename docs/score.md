# Pod Score：原生评分到组评分的交接

这一步为每个 Pod 的候选节点打基础分，再把本轮完整 Pod、NodeInfo、评分列表和提交模式交给组规划。
组评分和组内选点仍是 TODO。

## 调用顺序

```text
Buffer 从冻结 Unit 取 Mode，填进 Execution
  → Executor
  → steps.Filter：固定本轮成员、节点资源视图，返回各 Pod 的候选节点
  → steps.Score：逐个 Pod 调用 upstream.PodScore
  → GroupScoreInput{Mode, Pods, NodeInfos, PodScores}
  → steps.PlanGroup：后续实现组评分和组选点
```

Atomic 和 Partial 的 Mode 都来自 Unit，不重新猜测当前 CR 的模式。
两种模式在 Pod 基础评分这一步走同样的规则，后续组规划再使用 Mode。

## 单个 Pod 怎样打分

[upstream/score.go](../src/scheduler/upstream/score.go) 的入口是 `NewPodScore(ctx, filter)`。
它复用这次 Filter 的 NodeInfo，避免打分时又从缓存读入另一份资源数据。
随后调用 `Score(ctx, pod, candidates)`，只给传入的候选打分。

一个 Pod 使用一份 CycleState，依次对每个插件执行：

1. PreScore：准备本 Pod 的数据；返回 Skip 时跳过这个插件。
2. Score：给每个候选节点打分。
3. NormalizeScore：插件有这个方法时，对整份候选列表统一归一化。
4. 归一化分数乘插件权重，累加到节点总分。

详细调用在 [score_cycle.go](../src/scheduler/upstream/score_cycle.go)。

## 当前插件配置

| 插件 | 当前含义 | 插件权重 |
| --- | --- | --- |
| NodeResourcesFit | LeastAllocated；偏好剩余资源较多的节点 | 1 |
| NodeResourcesBalancedAllocation | 偏好 CPU/内存使用比例均衡 | 1 |
| NodeAffinity | Pod 声明的节点软亲和性 | 1 |
| TaintToleration | 尽量避开未容忍的 PreferNoSchedule 污点 | 1 |

资源插件暂时都只评分 CPU 和内存，二者等权。Filter 仍检查它已经支持的其他资源。
配置集中在 [score_plugins.go](../src/scheduler/upstream/score_plugins.go)，没有引入新配置系统。
这是我们选择的四个插件和权重，不等于 Kubernetes 的完整默认评分配置。
原生实现直接复用现有 third_party 源码，没有新增或修改第三方 Go 文件。

## 传给后面什么

[scheduler/score.go](../src/scheduler/score.go) 中的 `GroupScoreInput` 包含：

- Mode：冻结 Unit 的提交模式。
- Placement：冻结组声明中的通信关系及资源整合开关。
- Pods：本轮完整成员对象，保留资源需求和约束；按本轮成员顺序排列。
- NodeInfos：节点名 → Filter/Score 这一轮的节点信息，包含节点标签、容量和已有 Pod 占用。
- PodScores：PodRef → NodeScore 列表。
- Context：同组其他活跃 Unit，以及 BoltDB 中的成功记录。
- BoundPeers：本轮缓存中同组已绑定、未删除且未终态的 Pod，提供当前绑定位置。

Pods 使用独立副本。NodeInfos 从 Filter 保存的节点信息调用原生 Snapshot 导出，不重新读缓存。它的资源统计、端口和成员列表已复制，后续试放可在副本上操作。已有 Pod 和 Node 对象仍只读；需要修改对象本身时先 DeepCopy，不能直接改共享对象。

2026-10-27：Filter 的起点已改为共享 ResourceCache 的本轮快照，不再从 Node/Pod lister 重建资源占用。缓存之后继续变化，也不会改动本轮 Score 的资源视图。BoundPeers 同样来自这一轮快照中的已确认绑定成员；暂占只计资源，不作为已经运行的同组成员。

每个 NodeScore 保留节点名、总分、各插件归一化后的分数。插件分数尚未乘权重；被 Skip 的插件不出现在明细中，贡献为零。
列表按总分降序排列，同分按节点名排列；保留全部候选，不提前截断。

这些分数没有扣减本轮其他成员将要使用的资源，也没有表达成员间拓扑关系。
后续组评分和试放需要补上这些考虑，不能各取最高分就当作完整计划。

Pod Score 完成后报告 Scored，Pending 仍属于同一个 ActionID。
目前 PlanGroup 已能生成通信候选并按搜索排序选定第一套完整计划。Executor 报告 Planned 后会批量 Assume 并报告 Assumed；Bind 尚未实现，确认未提交绑定后由 Recover 调用 Forget，再报告 Blocked/Recovered。不提交真实绑定。集中评分及多语义最终取舍仍未实现。

Context 的历史记录不代表成员仍在运行，也不重复计入 NodeInfo 的资源占用。说明见 [组上下文与成功记录](group-context.md)。

通信语义设计见[通信语义方案](communication-semantics.md)，本轮实现和阅读顺序见[通信放置源码说明](communication-placement.md)。完整组规划仍未完成。
