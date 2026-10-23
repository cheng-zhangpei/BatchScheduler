# Pod Score：原生评分到组评分的交接

这一步只做两件事：为每个 Pod 的候选节点打基础分，把列表和提交模式传给组规划。
组评分和组内选点仍是 TODO。

## 调用顺序

```text
Buffer 从冻结 Unit 取 Mode，填进 Execution
  → Executor
  → steps.Filter：固定本轮成员、节点资源视图，返回各 Pod 的候选节点
  → steps.Score：逐个 Pod 调用 upstream.PodScore
  → GroupScoreInput{Mode, PodScores}
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
- PodScores：PodRef → NodeScore 列表。

每个 NodeScore 保留节点名、总分、各插件归一化后的分数。插件分数尚未乘权重；被 Skip 的插件不出现在明细中，贡献为零。
列表按总分降序排列，同分按节点名排列；保留全部候选，不提前截断。

这些分数没有扣减本轮其他成员将要使用的资源，也没有表达成员间拓扑关系。
后续组评分和试放需要补上这些考虑，不能各取最高分就当作完整计划。

Pod Score 完成后报告 Scored，Pending 仍属于同一个 ActionID。
目前 PlanGroup 报告“组评分和组选点未实现”，Executor 转为 Blocked；不提交预留或绑定。
