// Package scheduler 定义执行模块共享的过滤、评分接口和结果。
//
// 目录按职责分开：
//   - executor：接收请求、组织执行顺序、报告进展。
//   - steps：过滤、Pod 评分、组内选点、Assume、Bind、Recover 的具体方法。
//   - planner：组候选方案、链路放置与集中评分接口，以及规划的内部调用顺序。
//   - plugins：具体组规划插件，用户也可以在子包中实现 planner 定义的接口。
//   - upstream：调用原生过滤与评分插件所需的适配代码。
//
// 调用方向为 executor → steps → upstream / planner → plugins 实现。复制的原生源码保留在 third_party/kubernetes。
// 当前已实现过滤、Pod 基础评分和本轮通信候选生成；完整组规划与真实资源操作仍待实现。
package scheduler
