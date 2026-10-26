# Buffer 源码阅读说明

更新日期：2026-10-25。本文按当前源码说明实际行为；“还没实现”会明确写出来。

## 1. 先记住 Buffer 在做什么

Buffer 保存正在处理的 Unit 的执行进度。它还会观察这些成员在集群中变成了什么样，并决定下一步做什么。

例如：这一轮执行到哪里了？执行器还在处理吗？集群看见绑定了吗？失败是否已恢复？这一轮能不能删记录？

Buffer 不负责计算最佳节点，不直接提交绑定，也不是保存所有 Pod 历史的数据库。节点选择、资源预占、绑定和恢复交给执行器；正在排队的任务由 WorkQueue 保存。

后面始终使用同一个例子：组 G 有 Pod A、B，Select 把 A、B 组织为 Unit U。

```text
WorkQueue：U 在这里等待一次执行机会。
Buffer：U 进来后，在这里记录这一轮做到哪一步。
执行器：真正计算节点、预占资源、绑定、恢复。
informer：持续读取集群中的组和 Pod，告诉 Buffer 实际发生了什么。
```

## 2. 当前到底哪些接好了

| 部分 | 当前情况 |
| --- | --- |
| Pod/Group 变化 → Observe | 已在 main 注册 |
| Observe → Record → 评估通知 | 已实现 |
| 三种标准 Pod 调度原因 → 内部错误 | 已实现，具体规则见第 8 节 |
| Evaluate → Apply | 已在 main 的评估线程中调用 |
| Apply → WorkQueue.Select | 已接真实函数 |
| Evaluate 生成 Transition → Apply → WorkQueue.Transition | 已接真实函数，自动路径有测试 |
| 节点变化、已绑定 Pod 删除、调度门移除 → 重试通知 | 已注册；只是条件可能改善，不保证下次能调度 |
| 退避时间到 → 再次评估 | main 每秒通知检查 Fail/Retry |
| Active.Pop → Accept 的主循环 | **尚未实现**；测试中直接调用 |
| Apply → Execute → Report | 已接独立执行队列、内部 Filter/Pod Score 和持续 Report；组选点和真实资源操作待实现 |
| 成功结束 → BoltDB 归档 → 解除队列旧关联 → 删除活跃 Record | 已接，写库失败时保留活跃记录；失效清理不写成成功历史 |
| 失败恢复后 → 创建下一轮并回队列 | 已接 Handoff 的 Return 分支，通过 SelectRequest.Return 组装新 Unit 并放入 Fail |
| compaction、FastRetry 准入、容量限制、Pending 超时核查 | **尚未实现** |

所以“模拟执行器可以走完流程”不代表真实集群中已经能完成一次调度。Select、Transition、Handoff 的 Release/Return、Execute 入口及 Report 回报通道已接；具体执行阶段和 Active 消费循环未实现。

## 3. 源码应该从哪里读

目录先按四块看：`staging/` 是共用数据和接口，`buffer/` 保存和修改记录，`evaluation/` 只做决定，`error_engine/` 管错误映射及规则。

| 文件 | 先回答的问题 |
| --- | --- |
| [types.go](../src/staging/types.go) | Record、Decision、Feedback 分别装什么 |
| [contracts.go](../src/staging/contracts.go) | 外部函数和 Execution、Handoff 参数是什么 |
| [buffer/buffer.go](../src/staging/buffer/buffer.go) | Unit 怎样进入 Buffer，三个 map 怎样登记 |
| [buffer/observe.go](../src/staging/buffer/observe.go) | 集群观察怎样更新到 Record |
| [error_engine/observed_error.go](../src/staging/error_engine/observed_error.go) | Pod 原生调度原因怎样匹配成内部错误 |
| [buffer/evaluation_queue.go](../src/staging/buffer/evaluation_queue.go) | 谁消费组通知、调用评估 |
| [buffer/evaluate.go](../src/staging/buffer/evaluate.go) | 怎样同时检查 Buffer、Fail/Retry、新成员 |
| [evaluation/record.go](../src/staging/evaluation/record.go) | 一个 Unit 的四个评估阶段入口 |
| [error_engine/engine.go](../src/staging/error_engine/engine.go) | 错误怎样分到不同 handler |
| [error_engine/queue.go](../src/staging/error_engine/queue.go) | 什么错误可以去 Retry，什么条件可以回 Active |
| [buffer/apply.go](../src/staging/buffer/apply.go) | Decision 怎样分发到 Select、Transition、Execute、Handoff |
| [buffer/report.go](../src/staging/buffer/report.go) | 执行进展和整轮结束怎样更新记录 |
| [buffer/handoff.go](../src/staging/buffer/handoff.go) | 怎样让队列先处理旧关联，再删除 Record |
| [buffer/release.go](../src/staging/buffer/release.go) | 结束确认和真正删除 Record |
| [buffer/queue_events.go](../src/staging/buffer/queue_events.go) | 节点事件、时钟怎样让等待任务再次评估 |
| [executor/schedule.go](../src/scheduler/executor/schedule.go) | 执行器内部怎样串起具体步骤 |
| [main.go](../src/cmd/batchscheduler/main.go) | 上面这些函数在哪里连接 |

不必一次看完。建议先跟着 U 走 Accept → Observe → Evaluate → Apply → Report，再单独看队列重试和 Handoff。

## 4. 五类数据，先分清谁是谁

### 4.1 View：集群里现在有什么

View 来自 `group.Store.Get`，包含 PodGroup 和当前关联到它的 Pod。Store 读取 informer 的本地缓存，不是每次发请求问 API Server。

View 不表示我们的调度操作已经执行成功。它只是读取时缓存中的资源情况；Pod 缓存和组缓存也不是同一个瞬间一起更新的。

### 4.2 Unit：这一轮一起处理谁

U 的成员是 A、B。成员用 `PodRef{Name, UID}` 表示，完整组身份包含 namespace、name、UID。

UID 是为了区分“旧 A 被删了，新建了同名 A”。同名不代表同一个 Pod，旧轮次的结果不能用到新 Pod 身上。

Unit 放进队列前已经 Freeze。执行期间不能往 U 里追加或删除成员。以后要合并任务、拆出新的重试范围，必须创建新的 Unit。

当前组装方式是 Atomic 等整组到齐，Partial 把当前可接收的同组成员批量装进 Unit，不必等整组到齐，也不限制为一个 Pod。assemble 通过 Buffer.ContainsPod 排除已经在执行的成员，再排除已有队列归属的成员。

同组仍可有多个 Partial Unit：旧 Unit 已冻结或进入 Buffer 时，后到成员另成一批，不直接追加到旧 Unit。Pod 事件只是触发检查，不再把本轮候选范围限制为事件对应的一个 Pod。Partial 的区别在于允许部分成功和按失败范围恢复，不是固定拆成单 Pod 调度。

组装中的正常成员与已有明确失败的成员分开成批，避免正常成员因为混入失败任务而被一起放进 Fail。已经组装好后才发生的成员失败，以及执行中的部分结果，仍需要后续完善反馈、拆分和回送；本次没有实现完整的部分恢复流程。

### 4.3 Record：U 这一轮做到哪一步

`records[U.Key()]` 是 U 的执行记录。它不是 Pod，也不是另一个队列单元。

最重要的是两份信息分开保存：

```text
Record.Members：我们自己的执行进度，例如 Reserved、Bound。
Record.Observed：从集群读到的情况，例如 nodeName、PodStatus、映射后的错误。
```

例如执行器说绑定成功，`Members` 可以先变成 Bound；但 informer 尚未收到更新，`Observed.NodeName` 仍可能为空。代码不会强行把这两份信息写成一致，而是等待后续观察。

### 4.4 Decision：准备做什么

Evaluate 返回 Decision，只是安排，不是执行结果。

| Action | 意思 |
| --- | --- |
| Wait | 现在先等，不调用执行器 |
| Select | 请 WorkQueue 检查这个组是否有成员需要组装、首次入队 |
| Transition | 已经在队列里的 Unit，考虑 Fail → Retry 或 Retry → Active |
| Execute | 让执行器完成 Operation 指定的工作 |
| Return | 旧执行轮次结束，需要让队列接住下一轮任务 |
| Release | 旧执行轮次已完成，只处理旧关联并结束本轮 |

`ActionExecute` 的 `Operation` 只有两种：Run 启动整轮执行，Recover 处理执行之外发现的旧轮次异常。Assume、Bind 是 Executor 内部步骤，不是 Buffer 单独发出的任务。

### 4.5 Execution / Feedback：请求与结果

Apply 把 Decision 里的执行安排装成 Execution，交给 `handlers.Execute`。

执行器每有进展或整轮结束，都调用同一个 `Report(ctx, Feedback{...})`。反馈中的 `ActionID` 必须对应这次请求，例如 action-3。它不是 informer 事件，也不是任意 Pod 状态更新。

## 5. Buffer 的三个 map 不是三份任务

源码字段如下：

```go
records map[workqueue.UnitKey]*Record
groups  map[group.Key]map[workqueue.UnitKey]struct{}
owners  map[group.Key]map[types.UID]workqueue.UnitKey
```

U 进入后，它们的内容可以画成：

```text
records[U]       → U 的 Record
groups[G]        → {U}
owners[G][A.UID] → U
owners[G][B.UID] → U
```

分别解决三个查询：找 U 的进度；找 G 在 Buffer 中有哪些 Unit；检查 A 是否已经由某个 Unit 处理。

`groups[G]` 是一个集合。Go 用 `map[UnitKey]struct{}` 表示“这些编号存在”，`struct{}` 不包含额外任务数据。

Accept 登记时一起写这三处，removeRecord 删除时一起清这三处。只删 Record 不删 owners，会让 Select 误以为 Pod 还在 Buffer；只删 owners 不删 Record，则可能重复创建任务。

## 6. Accept：U 怎样进入 Buffer

方法在 `buffer.go`。它做的是“接住任务并记进度”，没有运行调度算法。

### 6.1 前半段检查什么

| 检查 | 避免的具体情况 |
| --- | --- |
| Unit、组、成员编号完整，成员非空且已冻结 | 认不出任务，或者登记后成员又改变 |
| 成员名称和 UID 不重复 | 同一 Pod 在一轮里出现两次，map 覆盖记录 |
| 相同 UnitKey 已经登记过 | 重复调用导致重建或覆盖进度 |
| 来源是 Queued + Active/FastRetry | Fail/Retry 还没获准执行，就绕过重试条件进入 Buffer |
| 当前组仍是原 UID，没有删除 | 同名新组错误继承旧轮次 |
| Unit 模式符合组声明，Atomic 成员完整 | 把整组要求偷偷拆成 Partial，或只接半组 |
| 原 Pod 仍属于此组、未绑定且未结束 | 排队后 Pod 已改变，仍按旧引用执行 |
| 同组没有冲突的 Atomic 轮次，也没有重复 Pod | 一个成员被两个执行轮次同时处理 |

重复检查不是每次都重新验证一遍全部数据。若查到同一个已接收的 Unit、模式和成员都相同，就直接返回成功，不重新启动执行。

### 6.2 为什么查重有好几次

先查一次，方便重复请求直接返回。读缓存时没有一直占住 Buffer 的修改权限，这段时间另一个 Accept 可能已经登记成功，所以写入前再查一次。

源码中的 `mu.Lock()` 表示这段时间不让另一段代码同时修改受保护的数据；`Unlock()` 表示这一段改完了。这里需要一起保护“检查没有旧记录”和“创建新记录”，否则两个调用都可能认为自己是第一个。

### 6.3 真正写入什么

通过检查后：

1. 新建 Record：Revision=1，Generation=当前组声明版本。
2. `unit.MarkProcessing()`：Unit 从 Queued/Active 变为 Processing/None。
3. `Record.Members[A/B]` 初始记为 Unscheduled。
4. 写 records、groups、owners。
5. `evaluations.Add(G)`：通知稍后评估这个组。

此时还没有节点计划、没有资源预占、没有绑定。

### 6.4 Accept 的未完成边界

Active.Pop 当前只把条目取出，并不提供“这次出队已被谁接收”的凭据。Accept 只验证状态，不能证明调用者真的先 Pop 了。

main 也还没有 Active.Pop → Accept 的消费循环。将来写这段时，需要由我们一起明确：出队后 Accept 拒绝，任务应当回哪里；现在不能假设已有自动补救。

## 7. Observe：读集群，更新观察

Observe 不接收 Accept 的结果。Accept 已经创建 Record；Observe 是另外一个入口，可以先于或后于 Accept 到来。

### 7.1 实际调用顺序

```text
informer 先更新自己的缓存
→ events.go 从变化对象取组编号
→ main 调用 center.Observe
→ Store.Get 重新读取这个组和成员
→ 更新 Buffer 已有 Record 的 Observed
→ evaluations.Add(G)
```

传给 Observe 的主要输入是组编号。`Observation.Pod` 和 `Observation.View` 当前没有用于覆盖记录，不能以为传一个 View 进去就会直接被采用。

重新读缓存的原因是：收到的事件可能比较旧。例如先收到新 Pod 的事件，再收到旧 Pod 的删除事件，不能拿旧事件中的对象把新观察覆盖掉。

### 7.2 给 U 保存什么

Observe 只为 U 的成员 A、B 保存观察，不把同组所有其他 Pod 都复制进去。

| 字段 | 意思 |
| --- | --- |
| Exists | 还能不能查到原来那个组 UID |
| Deleting、Generation | 组是否删除中，声明是否改过 |
| Known | 能否从原组的当前视图判断这个成员 |
| Present | 原名称和原 UID 的成员是否还在组里 |
| NodeName | API 对象里实际分到的节点 |
| Status | Pod 原生状态的一份复制 |
| Failure | 对支持的原生调度原因匹配出的内部错误 |

`Known=false` 和 `Present=false` 不一样：原组都查不到时，不能据此确认旧 Pod 已删除；组还在但原成员不见了，才表示它不在这个组视图中。

Observe 替换旧 Observed，不累积每次变化的历史；只有内容变化才增加 Record.Revision。它不改 Members 的执行阶段、不改 owners，也不清 Pending。

### 7.3 为什么没有 Record 也通知评估

operator 可能刚创建了一名新成员，它还没有进入 Buffer。此时没有 Record 可更新，但 Evaluate 可以根据 View 产生 Select 决策，让 WorkQueue 检查是否需要首次入队。

没有 Record 时不长期保存一份额外组快照，避免 Buffer 变成第二个全量 informer 缓存。

### 7.4 notify=false 是什么意思

普通 Observe 更新后通知评估。评估线程在处理通知时又要主动读取当前缓存，这次使用 `observe(..., false)`，意思是“这次读完别再给自己排通知”。

否则每次评估都会引发下一次评估，即使集群什么都没变也停不下来。Apply 执行前的再次读取也使用 false。

## 8. 原生错误怎样变成内部错误

函数是 `error_engine.MapPodSchedulingError`，在 Observe 读取每个 Pod 时调用：

```go
fact.Failure = errorengine.MapPodSchedulingError(pod)
```

Evaluate 读取的是 `ObservedPod.Failure`，不用再解析原生 Reason。执行器报告的错误则放在 `Record.Failure`，两条来源分开保存。

### 8.1 当前明确支持的规则

只检查未绑定、未删除、未终态的 Pod，并且条件必须是 `PodScheduled=False`。

| 原生 Reason | 内部 Reason | 后续含义 |
| --- | --- | --- |
| Unschedulable | Unschedulable | 暂时调度不了；相关资源、节点或放置条件变化后可以再次尝试 |
| SchedulerError | SchedulingError | 调度时出了错误，但不能证明只是临时故障；不直接准许自动重试 |
| SchedulingGated | DependencyNotReady | 还有调度门没有移除，需要等前置条件变化 |

规则来自当前依赖 `k8s.io/api v0.34.7` 的 `core/v1/types.go` 中对这三个常量的定义，也核对了本地上游 `pkg/scheduler/schedule_one.go` 写入 PodScheduled 条件的位置。

Unschedulable 可能包含资源不足、标签、污点、拓扑等多种情况。单看这个 Reason 不能把它硬分成 InsufficientResources；当前不靠解析人类可读的 Message 猜分类。

SchedulerError 的官方注释包括声明解析异常，所以当前没有直接把它归成 TransientSchedulingError 并无限重试。

容器 CrashLoopBackOff、镜像拉取失败、PodReady=False 不在这个映射中。已绑定 Pod 的运行失败，需要后续讨论 operator 是否创建新 Pod，不能直接拿旧 Pod 改绑节点。

### 8.2 没有匹配到是什么意思

未知原生原因返回 nil，原始状态仍留在 Observed.Status。nil 只表示“当前规则没有匹配到支持的调度异常”，不表示这个 Pod 绝对正常。

API 请求返回的 Forbidden、Timeout 等不在这个 Pod 条件函数中，未来真实绑定执行器要单独分类后调用 Report。

### 8.3 多种错误谁优先

Record.Failure 来自执行器，表示本轮已确认的操作失败，优先使用。成员已有内部 LastFailure 时也优先于观测匹配。

尚未生成节点计划时，可以采用 Observe 匹配出的成员错误。已经收到执行进度或已有 Plan 后，不用还没刷新的旧 PodScheduled=False 推翻刚成功生成的计划。

这仍不是完整的错误新旧判断：原生条件不会携带我们的 ActionID，当前还没有逐次证明一个条件属于哪个调度轮次。这个边界需要以后单独讨论，不能把三条匹配规则说成已经覆盖所有集群异常。

## 9. 评估通知队列究竟存什么

有两个不同的队列概念：

| 队列 | 存什么 | 做什么 |
| --- | --- | --- |
| WorkQueue 的 Active/Fail/Retry/FastRetry | Unit | 保存调度任务和它的等待位置 |
| Buffer.evaluations | Group Key | 保存“哪个组需要再检查一次”的通知 |

`RunEvaluations(ctx, 32, handle)` 中的 32 是最多取 32 个组编号，不是一次调度 32 个 Pod，也不是尽量凑齐一个组的所有 Unit。

只有一个评估线程，批内还是逐组顺序执行。没有通知时 Get 等待；拿到第一个后，尽量顺手取已在队列里的其他组，不等凑满。

同一组等待处理时被通知十次，只保留一个待检查编号。如果处理期间又收到通知，Done 之后会再检查一次，避免丢掉最后一次变化。

`evaluateOne` 实际只有三步：

```text
observe(..., false) 读最新缓存
→ Evaluate 生成本组的 Decision 列表
→ handle 把这些决定逐个交给 Apply
```

函数返回前调用 Done。Done 只是告诉通知队列“这个组本次检查结束”，不是删除 Unit，也不是说明业务任务完成。

出错时这里只记录日志，不给任意 Record 自动安排无限重试。Fail/Retry 另有时钟通知；普通执行记录如果再没有事件或反馈，仍可能停在原处。

## 10. Evaluate 为什么要看三处数据

Evaluate 入口收到组编号 G，先读当前 View，再读取 WorkQueue 对 G 的 Fail/Retry 查询结果，然后检查 Buffer 中的 Record。

当前代码的三块分别是：

1. **Buffer 里的 Unit**：根据执行进度、观察、失败信息决定等待、继续执行、恢复或结束。
2. **Fail/Retry 里的 Unit**：根据错误和重试条件决定是否迁移队列。
3. **View 里的 Pod**：查找未绑定、未结束且不在 Buffer 的成员，请 Select 检查是否需要入队。

第三步没有直接判断“这个 Pod 不在 Buffer，所以一定没有 Unit”。它可能已在队列中，因此由 Select 再查归属；找到一名候选成员就发一个组级 Select 决策，不给每个 Pod 各发一份。

### 10.1 QueueSnapshot 不是出队

`handlers.QueueSnapshot` 实际指向 `WorkQueue.GroupSnapshot`。

```go
queued = b.handlers.QueueSnapshot(G)
```

意思就是“查 G 在 Fail/Retry 中的任务”。每项 QueuedUnit 包含 Unit 引用、复制的 UnitState，以及 Failure 或 Retry 信息。

它不搬任务，不把任务放进 Buffer。Unit 仍指向原对象；读完之后队列也可能变化，所以真正迁移还要比较版本。

Buffer 先查 WorkQueue，再保护自己的 map。原因是 WorkQueue 查询/迁移过程可能调用 ContainsPod，而 ContainsPod 要读 Buffer。如果两边各占住自己的锁再等对方，就可能互相卡住。

## 11. 一个 Unit 的四个评估阶段

入口是 `decision_engine.go` 的 evaluateRecord。每次只产生一个决定；下一步完成后再来一次。

### 11.1 预处理：现在能不能判断

`evaluationWaitReason` 检查 PendingAction、Binding/BindingUnknown、Observed 是否存在。

| 情况 | 决定 | 为什么 |
| --- | --- | --- |
| PendingAction 不为空 | Wait | 已经发过一次操作，尚未确认结束，不能再发一次 |
| 成员内部状态是 Binding/BindingUnknown | Wait | 不能在不知道绑定成败时重绑或释放预占 |
| Observed 为空或必要成员情况未知 | Wait | 不能凭空猜成功、失败或删除 |

它目前没有 Pending 超时核查。长时间等待不等于自动清理。

`summarizeAttempt` 再收集本轮情况：成员是否都已绑定、内部是否都未调度、哪些成员失败、组或成员是否已不能执行原计划。这些临时信息不写回 Record。

### 11.2 报喜：能结束本轮吗

`evaluateCompletion` 要求：本轮没有失效、没有失败、所有成员都已绑定。

还没完成资源收尾则 Wait；执行器已确认收尾完成则 Release。

虽然阶段名叫报喜，它不会把“已经绑定但还有错误”的 Unit 提前删掉，也不会从 Atomic 中先删几个成功成员。

### 11.3 报忧：交给错误引擎

`evaluateErrors` 确定成员范围，把情况交给 `errorengine.Decide`，再给返回结果加上 UnitKey 和 Record.Revision。

Atomic 的失败范围是整轮；Partial 能定位某个成员失败时用失败子集。当前 Report 只接受整个操作的结果，因此 Record.Failure 无法定位成员时，Partial 也先处理整个 Unit。

### 11.4 正常推进：下一件事是什么

`evaluateProgress` 只决定是否启动一次 Run：尚未开始、没有旧计划、成员都未调度，才交给执行器。执行中的 Assume、Bind 和恢复由 Executor 自己继续，Buffer 不再逐步派发。待实现的步骤报告 Blocked 后先 Wait，不自动重新执行整轮。

如果旧计划中的部分成员已经在集群分到了节点，就先等反馈，不继续对整轮重复预占或绑定。

四阶段都是逻辑上的分类，不要求一批只能报喜或只能报忧。同一批的 U1 可以完成，U2 失败，U3 继续绑定。

## 12. 错误引擎当前到底做哪些决定

唯一入口是 `error_engine.Decide`，没有直接操作 Buffer 或队列。

### 12.1 Buffer 中的执行记录

| 情况 | handler | 当前结果 |
| --- | --- | --- |
| 组删除、重建、声明变化，或原成员已不能执行 | handleInvalidatedAttempt | 没收尾则 Recover，收尾后 Release，结束旧轮次 |
| 已确认的可重试错误 | handleRetryableFailure | 先 Recover，恢复且收尾完成后 Return 到 Fail |
| 无效声明、权限问题 | handleConfigurationFailure | 也是先恢复再回 Fail，后续不自动重试 |
| 其他已记录错误 | handleOtherFailure | 保留已有的恢复/回 Fail 行为，不因此自动允许重试 |

必须明确：这几个失败 handler 当前仍共用 `recoverThenFail`，没有写出复杂的逐错误恢复策略。结构分开只是方便我们以后一个一个讨论和实现；不能把分了函数说成策略已经全部完成。

### 12.2 Fail 中的任务

先要求 Failure.RecoveryComplete=true，明确证明之前的执行和清理已经完成。只是“任务现在位于 Fail”不能当作恢复完成。(对于这个调度器的错误后续针对每一个任务都要细化，现在其实还有明显可能堵死调度器的可能，后续一些强语义的情况清洗我认为最好是可以尝试用Agent接入)

| 内部错误 | 是否生成 Fail → Retry | Retry 之后等什么 |
| --- | --- | --- |
| Unschedulable | 是 | 资源、节点或放置条件变化之一 |
| InsufficientResources | 是 | 资源变化 |
| NodeUnavailable | 是 | 节点条件变化 |
| PlacementConstraints | 是 | 放置条件变化 |
| DependencyNotReady | 是 | 依赖条件变化 |
| TransientSchedulingError、BindingRejected | 是 | 当前统一退避一秒 |
| SchedulingError、Forbidden、InvalidSpec、BindingUnknown、其他未识别错误 | 否 | 暂留 Fail，不自行定时重试 |

Return 分支通过 `SelectRequest.Return.RecoveryComplete=true` 把确认传给 Select，再写入 `Failure.RecoveryComplete`。这个 true 来自 Evaluate 已确认恢复和收尾完成，不是只看某个 Pod 为 Pending。普通首次入 Fail 仍使用 `QueueDecision.RecoveryComplete`。

### 12.3 Retry 中的任务

时间型要求截止时间到了。事件型要求相关变化发生过；如果同时设了截止时间，还要等时间。

相关事件先到但时间未到，Transition 会记下 `ChangeObserved=true`，这次不激活。以后时钟再通知评估，时间到了就不用再等第二次事件。

`RetryCondition.Matches` 负责判断事件是否相关。普通条件一对一匹配；通用 Unschedulable 接受资源、节点、放置这三类变化。

## 13. Apply 怎样把决定交出去

Apply 入口只做：`prepareDecision` → 按 Action 选一个函数。

| Action | 调用函数 | 是否写 Pending |
| --- | --- | --- |
| Wait | 直接返回 | 否 |
| Select | applySelect → handlers.SelectGroup | 否 |
| Transition | applyTransition → handlers.Transition | 否；这些 Unit 在队列里，没有 Center.Record |
| Execute | beginAction → handlers.Execute | 是；明确没有资源操作时取消等待 |
| Return/Release | beginAction → handlers.Handoff → finishHandoff | 是 |

### 13.1 prepareDecision 检查什么

对于 Record 的决定，当前做法是再次 Observe、Evaluate，再寻找与传入 Decision 完全相同的一条。它比较保守，可能重复扫描组；还没实现“观察没变化就省掉重评”。

之后 beginAction 还会检查 Record.Revision，以及是否已经有 Pending。因为重评和真正登记之间，仍可能有另一段代码更新记录。

Transition 不走 Record 的重评：先核对组、Unit 和请求参数，实际队列版本由 WorkQueue.Transition 在操作时检查。

### 13.2 beginAction 登记什么

假设准备启动一整轮执行：

```text
PendingAction    = action-3
PendingDecision  = Execute
PendingOperation = Run
Revision         = 原值 + 1
```

这三个 Pending 字段描述同一件事：编号、动作类型、具体操作。不是三个独立阶段，也不是 Pod.status.phase 的 Pending。

检查与登记在同一段保护内完成，只有一个调用能启动这次操作。完成登记后再调用外部函数；不会一直占着 Buffer 的修改权限等网络结果。

如果调用外部函数时还占着修改权限，外部函数一旦调用 Report，也会等待修改同一个 Record；前面等后面返回，后面等前面放开，流程就卡住了。

### 13.3 handler 和“回调”到底是什么意思

Handlers 里的字段就是 Go 函数。创建 Buffer 时把函数赋给字段，以后调用该字段就运行这个函数，没有隐藏的消息系统，也不会自动变成异步线程。

`handlers.Execute(ctx, request)` 这次调用本身是普通函数调用。现在它快速把请求放入 Executor 的有界队列，然后返回；过滤和后续资源操作由 Executor.Run 的一个工作线程执行。

入口在 `src/scheduler/executor/executor.go`。main 创建 Executor 和 Buffer，`handlers.go` 传入 Executor.Execute，再设置 `executor.Report = center.Report`，最后在 informer 就绪后启动 Executor.Run。没有额外的进度监控线程；Executor 直接调用 Report。

整轮执行顺序在 `executor/schedule.go`：Filter → Pod Score → PlanGroup → Assume → Bind。执行过程中失败，由 `executor/flow.go` 自己进入 Recover。具体实现分别放在 `scheduler/steps/`，原生 Filter 适配放在 `scheduler/upstream/`。

当前 Filter 和 Pod 基础评分已实现。组评分和组选点尚未实现，所以评分完成后报告 Blocked；这不是成功，也不是调度失败。过滤已经确认无可用节点时，会报告内部错误并进入恢复；因为此时还没有资源操作，可以直接结清。真正持有预留或绑定时的恢复尚未实现。

Execute 如果没有接收请求，例如执行队列已满，返回 ErrExecutionNotStarted，Apply 只取消对应 ActionID 的 Pending。已接收后的进展、错误、阻塞则通过 Report 返回。不能用“请求未接收”去掩盖已经开始的资源操作。

`beginAction` 同时准备 Execution、Handoff 两种参数，调用者只取需要的一种；同一个动作不会同时走两条路。

## 14. Report 怎样记录进展和结束

先确认 Unit 记录还在、ActionID 和 Operation 对得上。相同的最近一次 Feedback 重复到来直接返回，不再改一次状态。校验全部通过后才修改记录；不完整计划、预占后换计划、进度倒退都会被拒绝。

### 14.1 Kind 说明这份报告是不是整轮结束

| Kind | Record 的修改 |
| --- | --- |
| Progress | 更新 ExecutionPhase、必要的计划和成员进度，保留同一次 Pending |
| Unknown | 绑定或恢复的结果未确认，保留 Pending；不能自动当成失败重做 |
| Finished | 必须已绑定或已恢复，且明确结清资源；清 Pending，允许 Evaluate 判断结束或回送 |
| Blocked | 代码停在待实现步骤，记录 ExecutionBlocked；没有资源责任时可结束 Pending，有责任时继续保留 |

### 14.2 Phase 说明已经做到哪一步

| Phase | 保存什么 |
| --- | --- |
| Filtering / Filtered | 正在过滤 / 已完成过滤，还没有最终节点计划 |
| Scored | Pod 基础评分完成；仍在等待组规划，没有完整计划 |
| Planned | 完整成员 → 节点计划 |
| Assumed | Members 设 Reserved，并保存 PlannedNode；实际预留由执行器负责 |
| Binding | Members 设 Binding；若 Kind=Unknown 则设 BindingUnknown |
| Bound | Members 设 Bound；仍需 Finished 才能结束本次执行等待 |
| Recovering | 正在处理失败；确认的执行错误存入 Record.Failure |
| Recovered | 本轮恢复完成，成员进度回 Unscheduled；失败原因继续保留，供回送 Fail |

每次有效报告修改 Record.Revision，并通知评估。Progress 到来时 Evaluate 看到 Pending，返回 Wait。Executor 不用等它批准，就直接执行下一步。

Finished 只代表执行器确认本轮结束，Observed 仍由 informer 更新。成功释放还需要集群观察也确认成员已绑定；失败返回还需要恢复和资源收尾都完成。

`ResponsibilitiesSettled=true` 是执行器的明确承诺，不是这个 bool 自己释放了资源。真实资源缓存还没实现。有真实资源的恢复也不能靠清标记冒充。

Report 只保留最近一份 Feedback；活跃 Record 删除后，迟到反馈仍报“记录不存在”。成功历史现已存入 BoltDB，但没有实现迟到反馈处理或未完成操作的跨重启恢复。Report 调用失败时，Executor 停在当前进度，不把汇报错误当成调度失败去追加资源操作。

## 15. Handoff 到底在处理什么

### 15.1 先把三种“清理”分开

| 清理什么 | 谁应当处理 | 当前状态 |
| --- | --- | --- |
| 本轮预占资源、尚未确认的执行请求 | 执行/恢复组件 | 真实组件未实现 |
| WorkQueue 中旧 Unit 与 Pod 的关联、下一轮入队 | handlers.Handoff | Release 调用 ReleaseUnit；Return 调用 Select 的回送输入，两条已接 |
| Buffer 的 Record、groups、owners | finishHandoff / ConfirmHandoff → removeRecord | 已有代码，前提是队列已处理成功 |

Handoff 处理第二项，并在成功后允许第三项发生。它不处理第一项，不删除集群 Pod，不计算新节点。

### 15.2 为什么不能只删 Record

U 是通过 WorkQueue.Select 创建的，WorkQueue 内部还记着：A、B 属于 U。

即使 Active.Pop 把 U 从 FIFO 里取出了，这些关联也不会自动消失。Buffer 把 U 执行完后，如果只删自己的 Record，WorkQueue 以后仍可能认为 A、B 已经有旧 Unit，拒绝组织下一轮。

因此结束一轮要同时处理两边的旧记录。

### 15.3 成功 Release 时要做什么

当前 Release 已接好，顺序是：

```text
执行器已确认绑定和资源收尾完成
→ Evaluate 产生 Release
→ archiveSuccess 保存成功快照到 BoltDB，失败则保留 Record
→ Apply.applyHandoff 调用队列侧函数
→ WorkQueue.ReleaseUnit 清掉旧 U 的关联
→ Handoff 返回 nil，表示已经处理好
→ finishHandoff 把 U 标 Done
→ removeRecord 删除 Buffer 中 U 的记录和索引
```

不是删除 G、A、B；只是本轮 U 不再需要被跟踪。以后 informer 仍会观察这个组。

ReleaseUnit 在 workqueue/release.go 中实现。先检查 Unit 为 Processing 且 Queue=None，然后删除该 Unit 的队列管理记录、组内编号，以及仍指向它的 Pod 内部状态。同组其他 Unit 和成员保留；没有任何 Unit 后删除空组目录。它不调用 MarkFinished，后面的 finishHandoff 才把 Unit 标成 Done。旧记录已经不存在时直接返回成功，重复请求不会误删新 Unit。

### 15.4 失败 Return 时要做什么

先由 Recover 处理必要的资源清理，Report 确认完成，Evaluate 才返回 Return。

main 的 Handoff 调用 `queues.Select(SelectRequest{Return: ...})`，提供旧 U 的编号、回送成员、内部错误，以及恢复完成确认。Return 是 Select 的一种输入，不是第二套公开入队方法。

普通 Pod/View 输入会跳过 Buffer 正在处理的成员；Return 表示 Buffer 明确交回这些成员，因此不走普通事件的跳过规则。它只允许回送指定旧 U 的成员，不会取走同组其他 Unit 的 Pod。

源码在 `src/workqueue/select_return.go`，按三步阅读：

1. `validateReturn` 检查旧 U 还在 Processing，组和 Pod 身份没有变化。Atomic 必须回送整轮；Partial 可回送多个失败成员，仍然组成一个 Unit。回送成员必须未绑定、未结束；没有列入失败范围的成员不跟着进 Fail。
2. `newUnit` 创建新编号，加入回送成员；`prepareEnqueue` 冻结并登记 Fail 状态。这时新 Unit 还只在局部变量中，旧记录没有修改。工厂或 Unit 方法失败时可以保留旧轮次再处理。
3. 准备成功后，`removeUnit` 删除旧队列关联，登记新 Unit 和新成员状态，`publishUnit` 放入 Fail。新成员内部进度重新从 Unscheduled 开始，同时保存这次错误和恢复确认。与普通 Select 使用同一把管理锁，其他提交不能在替换中途插入重复任务。

`newUnit`、`prepareEnqueue`、`publishUnit` 在 `select_enqueue.go`，普通提交也使用这些函数。Return 不再调用普通 Policy 重新猜目标队列，因为 Evaluate 已经决定回 Fail；之后能不能重试，仍由已有 Transition 路径判断。

以上成功后 Handoff 返回 nil，Buffer 的 `finishHandoff` 才把旧 U 标 Done、删除旧 Record，并重新通知这个组的评估。Partial 剩余未失败、未绑定的成员会被下一次 Evaluate 找到，生成普通 Select 决策并按正常流程入队，不会因为旧轮次结束被漏掉。`ConfirmHandoff` 后补确认回送成功时也会发出这个通知。

WorkQueue 不替 Buffer 删除记录，也不修改旧 U 的冻结成员。新 Unit 暂时仍对应 Buffer 持有的 Pod，因此 Transition 在旧 Record 清理前会拒绝迁移；清理后的评估通知和 main 周期检查会继续检查 Fail/Retry。

同一张 Return 请求再到达时，旧 U 已不在队列索引中，会报“旧 Unit 已不存在”，不会再建一个新 Unit。正常 Apply 通过 Pending 和 Record 版本阻止重复执行；当前本地 Handoff 没有网络调用，也没有成功一半再返回普通 error 的步骤。不保存永久的回送历史，也不承诺跨进程重启恢复。

### 15.5 为什么分成三个函数

| 函数 | 用一句话理解 |
| --- | --- |
| applyHandoff | 发出“旧 U 要结束，请队列处理”的请求 |
| finishHandoff | 处理这次调用当场返回的结果 |
| ConfirmHandoff | 当场不知道成败，之后查清楚了再补确认 |

finishHandoff 的三个分支：

| 返回值 | 我们知道什么 | Buffer 怎么做 |
| --- | --- | --- |
| nil | 队列的相关工作全部完成 | U 标 Done，删除旧记录 |
| 普通 error | 明确知道队列没有处理成功，而且没有留下半套修改 | 保留记录，取消 Pending，允许以后重试 |
| ErrHandoffUnknown | 可能做成了，但现在无法确认 | 保留 Pending，不重复入队；等待 ConfirmHandoff |

普通 error 必须保证没有留下部分修改，这是对未来 Handoff 实现的要求，不是 Buffer 能检查或自动保证的。若已经新建了一半任务又返回普通 error，下次重试就可能重复。

ConfirmHandoff 不是每次都要调用。Handoff 已直接返回 nil 时，finishHandoff 已完成删除。只有先前结果未知，才用原 ActionID 补确认：true 表示做成，false 表示确认什么也没做成、可以再试。

### 15.6 removeRecord 实际删哪几处

1. 对 U 的每个成员，删除仍指向 U 的 owners 记录。
2. 删除 records[U]。
3. 从 groups[G] 删除 U。
4. 如果 G 在 Buffer 中已没有其他 Unit，删除这个空的 groups[G] 和 owners[G]。

它不会自动调用 WorkQueue.Select、Transition、Forget，也不会调用 API Server。

Release 方法只是“请检查这个 U 能否结束”的入口，会先 Evaluate，再把 Release 决策交给 Apply。它不是强制删除接口。

## 16. 三种版本和编号，不要混成一个

| 字段 | 标识什么 | 在哪里用 |
| --- | --- | --- |
| Group UID / Pod UID | 是不是原来那个 Kubernetes 对象 | 观察、成员匹配、删除与重建检查 |
| Record.Generation | Accept 时组声明的版本 | 发现声明变化，使旧轮次失效 |
| Record.Revision | Buffer 这条进度记录改过几次 | Execute/Return/Release 决策执行前检查 |
| UnitState.Revision | Unit 生命周期改过几次 | Transition.ExpectedRevision，在 WorkQueue 内验证 |
| ActionID | Record 中某一次具体操作，例如 action-3 | Report 和 ConfirmHandoff 核对对应哪次操作 |

Record.Revision 和 UnitState.Revision 不应该要求数值相等：它们数的不是同一件事。前者可能因为每次观察更新而增加，后者主要因 MarkQueued/MarkRetry/MarkActive/MarkProcessing/MarkFinished 等生命周期变化而增加。

当前队列版本也不是完整事件历史版本：Retry.ChangeObserved 变化、直接修改队列条目等不是全都使 UnitState.Revision 增加。因此只能说已有生命周期版本核对，不能说所有跨组件一致性已经解决。

Record 的独立 ID 与成功快照会写入 BoltDB；尚未完成的操作和 ActionID 仍在进程内，重启后不会自动恢复。

## 17. 时钟和 informer 怎样触发队列迁移

### 17.1 时钟

main 启动 `RunQueueChecks(ctx, time.Second)`。每秒向 WorkQueue 查询哪些组在 Fail/Retry 中还有任务，再把组编号放入评估通知队列。

它不清理 Record、不直接搬任务，也不证明资源改善。事件型任务没有相关提示时，即使每秒评估也不会自动进入 Active。

当前做法易理解，但要遍历等待组。高负载下应不应该改成只检查到期任务，留到以后单独权衡。

### 17.2 informer 事件

`OnRetryChange` 当前观察：节点增加/删除/属性状态变化；已绑定 Pod 删除；Pod 的 SchedulingGates 从有变无。

它们产生 ResourcesChanged、NodeConditionsChanged、PlacementConditionsChanged 或 DependencyChanged 提示。

NotifyRetry 找到正在等待相应条件的 Retry Unit，把提示和当前 Unit 版本暂存到 retryHints，再通知评估。提示不会直接绑定或重排任务。

Apply 成功处理提示后删除暂存提示；如果 Transition 只是记住了“事件已到但时间未到”，也删除这边重复的提示，因为 Retry.ChangeObserved 已记住。

其他依赖资源（例如特定存储对象）还没有 informer。调度门移除已接，并不表示所有 DependencyNotReady 都有事件来源。

## 18. 跟着 U 完整走两次

### 18.1 正常执行：当前是模拟执行器验证

| 时刻 | Record 里主要变化 | 谁决定下一步 |
| --- | --- | --- |
| Select 后还在 Active | 没有 Record | 等未来消费循环 Pop |
| Accept U | Members=Unscheduled，登记关联 | 通知评估 |
| Observe | Observed 中 A/B 未绑定，无匹配错误 | Evaluate → Execute Run |
| Apply | Pending=action-1 / Execute / Run | 请求交给 Executor |
| Report Progress / Planned | 保存完整 Plan，保留 action-1 | Executor 继续 Assume |
| Report Progress / Assumed | Members=Reserved，保留 action-1 | Executor 继续 Bind |
| Report Finished / Bound | Members=Bound，Settled=true，清 Pending | 等集群观察确认绑定 |
| Observe 看见 A/B 都有 NodeName | 只更新 Observed | Evaluate → Release |
| 队列侧结束处理成功 | Unit=Done，删除 Record/groups/owners | 本轮结束 |

Report 可以先于 Observe，也可以反过来。只看到 Pod 绑定不能代替执行器确认资源收尾；只有 Report 也不能凭空写成 API 已经观察到了绑定。

### 18.2 观察到调度失败

```text
API 中 A 的条件变成 PodScheduled=False / Unschedulable
→ informer 更新缓存并通知组 G
→ Observe 调用 MapPodSchedulingError
→ Record.Observed[A].Failure = 内部 Unschedulable
→ Evaluate 发现本轮有错误，Atomic 把范围扩为 A、B
→ error_engine 返回 Recover 决策
→ Apply 交给 Executor 的独立 Recover 入口（真实资源恢复待实现）
→ Report 确认恢复和收尾完成
→ Evaluate 返回 Return
→ Handoff 调用 Select 的 Return 输入，创建下一轮并放入 Fail
→ 队列里这条失败已明确 RecoveryComplete=true
→ Evaluate 生成 Fail → Retry
→ 相关资源/节点/放置条件变化
→ NotifyRetry → Evaluate 生成 Retry → Active
→ Apply 调用 WorkQueue.Transition
```

这里真实资源操作仍未实现。现有测试覆盖模拟执行反馈、真实 Handoff/Select/Transition；入口测试还覆盖真实 Buffer → Execute → 原生 Filter，确认不会把候选节点报成完整计划。目前没有 Active 消费循环。

## 19. 设计取舍与已知缺口

| 选择 | 为什么当前这样写 | 代价或未完成部分 |
| --- | --- | --- |
| Evaluate 不改 Record | 可以单独检查“给这份状态会做什么决定” | Apply 前要复核，当前重复扫描比较多 |
| 内部进度与集群观察分开 | API 更新可能晚到，不能互相冒充 | 读代码要区分 Members、Observed 两份字段 |
| 三个关联 map | 快速查 Unit、同组 Unit 和 Pod 正由谁处理 | 新增和删除必须一起维护 |
| Unit 成员固定 | 同一次操作的成员范围不会中途改变 | 合并/拆分要创建新 Unit，compaction 尚未实现 |
| 一条组通知去重队列 | 不给每个 Pod 事件开线程，可以合并重复通知 | 多个 Unit 仍分别决策，不等于联合调度 |
| 一个评估线程 | 当前控制流程容易推理 | 外部函数若耗时不返回，会挡住其他组 |
| 未知结果先等待 | 避免重复绑定、重复入队 | Pending 缺少超时核查，可能长期保留 |
| 成功先落盘，再删除活跃 Record | 释放内存同时保留跨 Unit 的成功位置 | BoltDB 可跨重启查询；历史淘汰、未完成操作恢复尚未实现 |
| Handler 通过函数字段提供 | Buffer 不直接依赖执行组件 | Handoff 和 Execute/Report 入口已接，具体执行阶段待实现 |
| 匹配标准 Pod 调度条件 | 原生异常终于能进入内部错误引擎 | 不是所有 Reason 的完整词典，也没有完整的新旧轮次判断 |

还需要以后逐项讨论，但本轮没有擅自改行为的地方：

- 没有 Buffer 容量或在途成员上限。任务持续进来、旧任务无法结束时，记录可能增长；不能靠强制删除未知结果记录解决。
- Observe 仍在 informer 的通知函数里同步读取组视图，并由 observeMu 串行执行。大组和高频更新可能增加观察延迟。
- 同组多 Unit 只是关联、分别决策，还没有合并计划。batch=32 不解决同组 Unit 到达时间分散的问题。
- 已有节点的 Pod 运行失败不等于可以直接重绑；重建和业务恢复与 operator 的边界需要单独明确。
- 绑定或恢复结果未知时保留 Pending，但尚未有超时核查和继续执行机制；长期无结果可能一直等待。
- 权限/声明错误当前留 Fail。外部修复后怎样重新确认错误已经消失，需要后续补规则，不是收到任意节点事件就放行。
- Failed Unit、过期组、重试提示的最终清理还没有一个覆盖所有状态的清理机制。
- Handoff 已实现本地队列的先准备、后替换。未来如果接入可能部分成功的外部服务，仍要另外实现结果未知时的查询与确认。
- 没有做真实集群、竞态检测或性能压测。已有单元测试不能证明生产正确性。

## 20. 测试在哪里证明了什么

| 文件 | 证明的范围 |
| --- | --- |
| buffer_test.go | Accept 的来源、重复接收、成员冲突和组模式检查 |
| observe_test.go | 观察更新不改执行归属；fake API → informer → Observe 错误映射 → Evaluate/error_engine；绑定后不保留原未调度错误 |
| error_engine/observed_error_test.go | 三种标准原因映射；未知原因与容器错误不乱归类 |
| evaluate_test.go | 等待、正常调度、失败恢复、结束、Partial 范围及通知去重 |
| decision_engine_test.go | 报喜不掩盖失败；队列版本校验与真实 Transition 调用 |
| error_engine/engine_test.go | 错误收尾规则与输入输出不共享可改成员/错误 |
| queue_flow_test.go | 不手写 Decision，自动评估并执行 Fail → Retry → Active；无关事件及不允许重试的错误不放行 |
| buffer/apply_test.go、buffer/report_test.go | 同一 Run 的进度报告、内部恢复、未知结果、结束确认和非法报告拒绝 |
| release_handoff_test.go / return_handoff_test.go | 真实队列的成功清理，以及模拟执行失败 → Select 回送 → Fail/Retry/Active → 再次 Accept |
| workqueue/select_return_test.go | Partial 多成员回送、保留同组其他 Unit；无效请求和工厂失败时保留旧记录 |

命令是 `go test ./src/... -count=1`。这些测试使用内存结构和 fake client，没有往用户的集群绑定、删除或创建实际工作负载。

## 21. 以后怎么继续读、继续写

这份文档是当前源码的说明，不是要求你一次掌握所有分支。

以后一次只选一个函数或一条错误规则：先由你说明希望输入什么、返回什么，我们讨论取舍，再写代码和检查。注释应直接回答“这一步做什么”和“如果省掉，会发生哪个具体问题”。

需要讨论的下一块由你决定；不因为这里列出缺口就自动开始整段实现。

## 22. 跨 Unit 的组上下文与成功归档

2026-10-25 新增：GroupContext 汇总其他活跃 Unit 和 BoltDB 成功历史；组规划另从本轮集群视图获取 BoundPeers。成功先落盘再清理内存，冻结 Unit 不强制合并。源文件和边界见 [组上下文与成功记录](group-context.md)。
