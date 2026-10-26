// Package plugins 是组规划插件实现的目录入口，不负责注册或安排插件执行顺序。
// planner 包定义接口和调用流程；此目录的子包实现接口，communication 是第一个实现位置。
// 自定义插件可以放在新的子包中，再通过 Executor.SetPlanner 于启动前接入。
package plugins
