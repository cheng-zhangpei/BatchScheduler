# 跨 Unit 的组信息与成功记录

更新日期：2026-10-25。

这次先解决一件事：U1 成功调度并离开 Buffer 后，U2 还能知道同组以前放在哪里、现在还有谁正在执行。
组评分、真正的 compaction、运行期失败回调都没有在这次实现。

## 1. 记录怎样保存

旧流程在 Release 时删除内存 Record。现在成功路径是：

```text
Report 确认执行已结束，Observe 也确认成员已绑定
  → Evaluate 返回 Release / AttemptComplete
  → Apply 登记本次 Pending
  → archiveSuccess 把成功快照写入 BoltDB
  → 写入成功，才调用队列 Handoff
  → 队列处理成功，清理内存 Record 和旧关联
```

入口在 [archive.go](../src/staging/buffer/archive.go)，调用位置在 [handoff.go](../src/staging/buffer/handoff.go)。
写失败时队列还没动，Record 继续保留，取消这次 Pending，允许以后重新保存。
归档写磁盘时不持有 Buffer 的记录锁；这次仍采用同步写入，确认事务成功后再继续交接。

Release 也用于清理已经失效的轮次。所以 archiveSuccess 只保存 AttemptComplete，不能把组删除、声明变更等清理路径写成调度成功。
这里的“成功”表示本轮调度和收尾已完成，不代表容器业务执行成功。

## 2. 存的是什么

[SuccessfulRecord](../src/staging/group_context.go) 保存：

- Record 的独立 ID、UnitKey、提交 Mode。
- 接收时的组 Generation、归档时的 Record Revision、保存时间。
- 成员的 PodRef、内部进度，以及成功时观察到的状态和节点名称。

Unit 接口、锁、队列和运行中的请求不能直接写进数据库，所以使用一份普通数据结构保存快照。
Record ID 在 Accept 时生成。Unit 的数字编号重启后可能重复，数据库以 Record ID 区分不同轮次。
重复交接不会重复插入，也不会用另一份成员位置覆盖已保存的同一 Record。

存储代码在 [history/bolt.go](../src/staging/history/bolt.go)，使用 `go.etcd.io/bbolt`。
结构是：`successful-records-v1 → 完整组身份 → Record ID → JSON 快照`。
组身份包含 namespace、name 和 UID；同名重建的组不会读到旧组的数据。

main 默认打开 `.batchscheduler/history.db`，可用 `--history-db` 指定位置，退出时关闭数据库。
默认目录已加入 gitignore。

## 3. 后续 Unit 怎样读取

[Buffer.GroupContext](../src/staging/buffer/group_context.go) 返回两类记录：

| 字段 | 从哪里来 | 表达什么 |
| --- | --- | --- |
| InFlight | 当前 Buffer 的 records | 同组其他 Unit 正在哪个阶段，有什么成员和计划 |
| Succeeded | BoltDB | 同组历史上哪些轮次调度成功，当时分别绑定到了哪里 |

先复制活跃记录，再查询磁盘。因为成功记录先落盘才删内存，清理恰好与查询交错时，也不会两边都漏掉。
同一 Record 如果两边都有，只返回一次成功记录。不同 Record 即使 Unit 编号相同也不会混为一条。
查询返回副本，不改变执行归属，不把冻结的 Unit 强制拼成新 Unit。

Executor 启动前通过 `SetGroupContextReader(center)` 接入 Buffer；steps.Score 读取这个组上下文，再传给 PlanGroup。
它会从 InFlight 中排除当前正在规划的 Unit，保留其他 Unit。

## 4. 历史位置与当前位置分开

旧记录写着 Pod A 在 Node X，不代表 A 现在还在。
所以 [GroupScoreInput](../src/scheduler/score.go) 另外提供 BoundPeers：

- 来源是本轮 Filter 使用的集群 Pod 列表。
- 只包含同组、已绑定、未删除、未终态的成员。
- 它表达当前缓存确认的绑定位置，不保证节点或业务一定健康。

Context.Succeeded 保留历史，BoundPeers 提供当前绑定事实，Context.InFlight 保留其他轮次的计划。
计划不能冒充已绑定事实，历史也不能冒充当前位置。
读取历史不会把 Pod 重新加入 NodeInfo，避免重复计算资源占用。

例如 U1 负责 A，成功归档后清理内存；U2 负责 B。U2 既可以查到 U1 的历史，也能从 BoundPeers 看见 A 当前的位置。
如果 A 已删除，历史仍在，但 BoundPeers 不再出现 A。组算法以后需要根据这两份信息作选择。

## 5. compaction 暂时做到哪里

现在具备了跨 Unit 查询的输入，没有实现强制合并或联合调度算法。
冻结、预占或正在绑定的 Unit 继续沿用自己的成员和 ActionID。后续如何联合规划尚未出发的成员，需要另外讨论。

本次只持久化成功历史，不恢复进程退出时尚未完成的操作，不自动重试已运行失败的 Pod。
历史的过期、删除组后的磁盘清理、数据量上限也尚未实现；不能把这份历史库当成完整的重启恢复机制。

## 6. 验证范围

- history/bolt_test.go：关闭重开后仍可查询、重复保存不增条目、重启后 Unit 编号重复不覆盖历史、组 UID 隔离。
- buffer/archive_test.go：先保存再交接、写库失败保留活跃记录、失效清理不伪装成成功、成功历史可在重启后查询。
- steps/group_context_test.go：组规划收到其他轮次与历史，已结束成员和历史位置不混入 BoundPeers，不重复计资源。

这些是本地数据库、内存和 fake 数据测试，没有对真实集群执行绑定或恢复。
