# WorkQueue 与 Buffer 的交接

更新：2026-11-18。本次只调整任务交接、Active 组装、Atomic 等齐和失败回送。Fail 长期驻留、过期时间、容量淘汰暂不处理。

## 1. 谁保存什么

| 位置 | 保存的任务 | 何时移除 |
| --- | --- | --- |
| WorkQueue | 正在组装、Active 等待、Fail、Retry 中的 Unit 及其 Pod 关联 | Active 交给 Buffer 成功后移除 |
| Buffer | 接收后的执行记录、成员归属、观察和反馈 | 完成归档或失败回送后结束本轮 |
| Informer / Store | 集群中的真实组和 Pod | 跟随 Kubernetes 对象变化 |

执行中的 Unit 不再同时挂在 WorkQueue 的组目录里。删队列关联不会删除 Pod，也不会丢掉 Buffer 的 Record。

## 2. Active 怎么交给 Buffer

入口：[runActive](../src/cmd/batchscheduler/active.go)。交接实现在 [DispatchActive](../src/workqueue/dispatch.go)。

```text
Active.Pop 取出 FIFO 条目
→ 固定本轮成员
→ Buffer.Accept 建立 Record 和 Pod 归属
→ 删除 WorkQueue 的 Unit 索引、成员关联和空组目录
```

最后三步使用 WorkQueue 的同一把管理锁。Select 不能在中途另造一个包含相同 Pod 的 Unit。Accept 成功时 Buffer 已经持有这些 Pod；后续事件会通过 ContainsPod 跳过它们。

这把锁只跨过本地 Accept：读取缓存、登记记录、通知评估。它不跨 Filter、规划、Assume、Bind，不等 API 响应。Accept 不能反过来调用 WorkQueue，否则会等到自己持有的锁。

Accept 失败时不删除旧队列关联，恢复 Active 可追加状态并放回 FIFO，再向 runActive 返回错误。当前主循环仍会上报并停止消费，不在这里无限重试。

ActiveQueue.Pop 本身只是低层 FIFO 操作。运行时应使用 DispatchActive，不再手工拼 Pop、Accept 和删除索引。

## 3. Active 里的 Unit 可以继续收成员

组装入口在 [assembly.go](../src/workqueue/assembly.go)：

1. pendingMembers 找出未绑定、未结束、也未进入 Buffer 的 Pod。
2. refreshAssembly 更新尚可调整的 Unit，去掉已删除、已绑定的旧成员。
3. assemblyTarget 找到同组可追加的 Unit；没有才新建。
4. 新 Unit 由 Policy 决定首次入队位置；已经在 Active 的 Unit 不再重复 Add。

Active 保存的是 Unit 引用，因此追加成员后原 FIFO 条目就能看到更新。出队交接开始后固定名单。Fail、Retry 的等待范围保持固定；Retry 重新进入 Active 后可以接收新成员。

Policy 不再检查 Atomic 是否齐全，也不再逐个回扫 View 验证同一批 Pod。它只决定新任务先去 Active，还是因已确认的错误进入 Fail。真实资源是否够用，仍由后面的规划和 Assume 判断。

## 4. Atomic 在 Buffer 等齐

例如组声明需要四个成员，Informer 先看到 A、B，稍后才看到 C、D：

```text
Active 的 U1(A,B) → Buffer：保存，等另外两个成员
Active 的 U2(C,D) → Buffer：汇入同一份待启动 Record
→ 完整名单 (A,B,C,D) → Evaluate 生成一次 OperationRun
```

实现：[accept_atomic.go](../src/staging/buffer/accept_atomic.go)、[EvaluateRecord](../src/staging/evaluation/record.go)。

汇集只发生在尚未启动的 Atomic Record 上。Buffer 用合并后的 Unit 替换待启动名单，沿用主 Record 的编号，统一成员索引，并重新读取观察。它不会追加到正在调度或绑定的 Unit。

AcceptedUnits 保存各批次原始编号和名单，用于认出重复 Accept。它随 Record 一起删除，不是永久历史。凑齐之前返回 AwaitingGroupMembers；组被删除或成员失效仍走原来的恢复、结束流程，不会一直等齐。

Partial 保持原语义：当前可执行的一批成员可以独立启动，不要求等整组。

## 5. Release 与 Return

适配入口：[handlers.go](../src/cmd/batchscheduler/handlers.go)。

**Release：** 队列关联已经在出队时移除，因此 Handoff 的 Release 分支直接确认成功。Buffer 仍负责先归档成功记录，再结束 Record；不是省略资源结清。

**Return / ReturnMembers：** Handoff 把 Source Unit、回送成员、错误和恢复确认交给 Select。Source 自带模式、声明与成员失败次数，不需要去队列里找旧执行记录。Select 创建下一轮，登记待调度关联，放入 Fail 或 Retry。

ReturnMembers 成功后，Buffer 只交出指定成员；其他成员的 Record、ActionID 和绑定反馈继续保留。整个 Return 成功后，Buffer 才结束旧 Record。

每次回送只给这批失败成员的 FailedAttempts 加一。计数随 Unit 转入 Buffer，之后再随 Source 带回，原来的递增退避不会因为删除队列索引而重置。

## 6. 本次没有改什么

Fail 长期无法恢复时怎么清理、Retry 长期没有改善时怎么办、Buffer 等待成员是否需要到期，这些仍然待讨论。没有增加 TTL 或自动淘汰规则，也没有改变 FastRetry 的准入。
