# Documentation / 文档

## Getting Started / 开始使用

| English | 简体中文 |
| --- | --- |
| [Project overview](../README.md) | [项目介绍](../README.zh-CN.md) |
| [User guide](user-guide.md) | [使用指南](user-guide.zh-CN.md) |

The user guides cover installation, local and Kubernetes deployment, task submission, communication configuration, and result inspection.

使用指南包含安装、本地与集群部署、任务提交、通信配置和结果查看。

## Source Walkthroughs

The following documents are written in Chinese and follow the implementation by component. The project overviews and user guides provide the usage entry points; dated notes in source walkthroughs record the development context of that component.

以下源码说明按组件展开。项目介绍与使用指南提供使用入口，源码长文中带日期的记录保留相应阶段的开发上下文。

| Document / 文档 | Topic / 内容 |
| --- | --- |
| [PodGroup and member views](podgroup-view.md) | CR definition, ownership, member indexes / CR 定义、所属关系与成员索引 |
| [WorkQueue and Buffer handoff](queue-buffer-ownership.md) | Unit assembly, ownership transfer, Atomic collection, Partial admission / Unit 组装、归属交接、Atomic 汇集与 Partial 接纳 |
| [Buffer](buffer.md) | Records, observation, evaluation, application, and feedback / 记录、观测、评估、动作执行与反馈 |
| [Filtering](filter.md) | Native filter adapters and candidate nodes / 原生过滤适配与候选节点 |
| [Pod scoring](score.md) | Native scores and group-planning input / 原生评分与组规划输入 |
| [Communication placement](communication-placement.md) | Search expansion, cumulative accounting, ranking, and pruning / 搜索扩展、累计资源、排序与剪枝 |
| [Binding](binding.md) | Inflight, binding workers, API requests, and reports / Inflight、绑定 Worker、API 请求与报告 |
| [Group context and history](group-context.md) | Group context, BoltDB records, and current placement / 组上下文、BoltDB 记录与当前放置 |

## Configuration / 配置文件

- [PodGroup CRD](../config/crd/scheduling.batchscheduler.io_podgroups.yaml)
- [Communication example / 通信关系示例](../config/examples/podgroup-communication.yaml)
- [Kubernetes deployment / Kubernetes 部署](../config/deploy/minikube-demo.yaml)
