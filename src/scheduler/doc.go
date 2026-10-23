// Package scheduler 定义执行模块共享的过滤、评分接口和结果。
//
// 目录按职责分开：
//   - executor：接收请求、组织执行顺序、报告进展。
//   - steps：过滤、Pod 评分、组内选点、Assume、Bind、Recover 的具体方法。
//   - upstream：调用原生过滤与评分插件所需的适配代码。
//
// 调用方向为 executor → steps → upstream。复制的原生源码保留在 third_party/kubernetes。
// 当前已实现过滤和 Pod 基础评分；组评分、组内选点、真实资源操作仍待逐步实现。
package scheduler
