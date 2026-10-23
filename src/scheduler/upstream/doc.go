// Package upstream 适配复制到 third_party/kubernetes 的原生过滤与评分插件。
// snapshot.go 准备节点资源视图；filter.go 调用 PreFilter/Filter 并转换结果。
// score.go 复用同一节点视图，调用 PreScore/Score/NormalizeScore，输出排序后的候选评分。
// 这里只处理单 Pod 规则；不负责组评分、组内选点，也不修改资源占用。
package upstream
