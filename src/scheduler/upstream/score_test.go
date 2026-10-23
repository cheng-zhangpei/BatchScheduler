package upstream_test

import (
	"context"
	"reflect"
	"testing"

	"batchscheduler/src/scheduler"
	"batchscheduler/src/scheduler/upstream"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNativeScorePlugins(t *testing.T) {
	for _, name := range []string{"resources", "balanced", "affinity", "soft-taint", "tolerated-soft-taint", "filter-excluded"} {
		t.Run(name, func(t *testing.T) {
			nodes := []*corev1.Node{filterNode("node-a"), filterNode("node-b")}
			pod := requestingPod("target", "1")
			pod.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("2Gi")
			var assigned []*corev1.Pod
			plugin, better, worse := "", "node-b", "node-a"
			switch name {
			case "resources", "balanced":
				used := requestingPod("running", "2")
				used.Spec.NodeName = "node-a"
				assigned = []*corev1.Pod{used}
				plugin = "NodeResourcesBalancedAllocation"
				if name == "resources" {
					used.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("4Gi")
					plugin = "NodeResourcesFit"
				}
			case "affinity":
				nodes[0].Labels = map[string]string{"disk": "ssd"}
				pod.Spec.Affinity = preferredDisk("ssd")
				plugin, better, worse = "NodeAffinity", "node-a", "node-b"
			case "soft-taint", "tolerated-soft-taint":
				nodes[0].Spec.Taints = []corev1.Taint{{Key: "batch", Effect: corev1.TaintEffectPreferNoSchedule}}
				plugin = "TaintToleration"
				if name == "tolerated-soft-taint" {
					pod.Spec.Tolerations = []corev1.Toleration{{Key: "batch", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectPreferNoSchedule}}
				}
			case "filter-excluded":
				nodes[0].Spec.Unschedulable = true
			}
			filter, err := upstream.NewPodFilter(context.Background(), nodes, assigned)
			if err != nil {
				t.Fatal(err)
			}
			filtered, err := filter.Filter(context.Background(), pod)
			if err != nil {
				t.Fatal(err)
			}
			scorer, err := upstream.NewPodScore(context.Background(), filter)
			if err != nil {
				t.Fatal(err)
			}
			scores, err := scorer.Score(context.Background(), pod, filtered.Candidates)
			if err != nil {
				t.Fatal(err)
			}
			if len(scores) != len(filtered.Candidates) {
				t.Fatal("Score 增减了 Filter 的候选")
			}
			byName := make(map[string]scheduler.NodeScore)
			for i, score := range scores {
				var sum int64
				for _, value := range score.PluginScores {
					if value < 0 || value > 100 {
						t.Fatal("未归一化的插件分数被传出")
					}
					sum += value // 本版四个插件权重均为 1。
				}
				if score.Score != sum || (i > 0 && scores[i-1].Score < score.Score) {
					t.Fatal("加权汇总或降序排列错误")
				}
				byName[score.Node] = score
			}
			if name == "filter-excluded" {
				if len(scores) != 1 || scores[0].Node != "node-b" {
					t.Fatal("评分把过滤排除的节点加回来了")
				}
				return
			}
			if name == "tolerated-soft-taint" {
				if byName["node-a"].PluginScores[plugin] != 100 || byName["node-b"].PluginScores[plugin] != 100 {
					t.Fatal("已容忍的软污点不应扣分")
				}
			} else if byName[better].PluginScores[plugin] <= byName[worse].PluginScores[plugin] {
				t.Fatalf("原生插件 %s 的偏好未生效: %+v", plugin, scores)
			}
			if name == "affinity" || name == "soft-taint" {
				if byName[better].PluginScores[plugin] != 100 || byName[worse].PluginScores[plugin] != 0 {
					t.Fatal("没有正确调用原生 NormalizeScore")
				}
			}
		})
	}
}

func preferredDisk(value string) *corev1.Affinity {
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
		Weight: 10, Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "disk", Operator: corev1.NodeSelectorOpIn, Values: []string{value}}}},
	}}}}
}

func TestNativeScoreKeepsSnapshotAndPodStatesSeparate(t *testing.T) {
	nodes := []*corev1.Node{filterNode("node-a"), filterNode("node-b")}
	nodes[0].Labels, nodes[1].Labels = map[string]string{"disk": "ssd"}, map[string]string{"disk": "hdd"}
	filter, err := upstream.NewPodFilter(context.Background(), nodes, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Filter 已固定本轮视图。修改外部对象后构建 Score，也必须沿用旧视图。
	nodes[0].Labels["disk"] = "hdd"
	scorer, err := upstream.NewPodScore(context.Background(), filter)
	if err != nil {
		t.Fatal(err)
	}
	a, b := requestingPod("a", "1"), requestingPod("b", "2")
	a.Spec.Affinity, b.Spec.Affinity = preferredDisk("ssd"), preferredDisk("hdd")
	var first []scheduler.NodeScore
	for i, pod := range []*corev1.Pod{a, b, a} {
		got, err := scorer.Score(context.Background(), pod, []string{"node-b", "node-a"})
		if err != nil {
			t.Fatal(err)
		}
		want := "node-a"
		if i == 1 {
			want = "node-b"
		}
		if got[0].Node != want {
			t.Fatalf("不同 Pod 的偏好串了，或复用了外部变化后的节点: %+v", got)
		}
		if i == 0 {
			first = got
		} else if i == 2 && !reflect.DeepEqual(first, got) {
			t.Fatal("B 的准备状态影响了 A 的再次评分")
		}
	}
	plain, err := scorer.Score(context.Background(), requestingPod("plain", "1"), []string{"node-b", "node-a"})
	if err != nil {
		t.Fatal(err)
	}
	if plain[0].Node != "node-a" || plain[0].Score != plain[1].Score {
		t.Fatal("同分时应按节点名稳定排列")
	}
	if _, exists := plain[0].PluginScores["NodeAffinity"]; exists {
		t.Fatal("没有软亲和性时 PreScore 的 Skip 未生效")
	}
	for _, names := range [][]string{{"missing"}, {"node-a", "node-a"}} {
		if _, err := scorer.Score(context.Background(), a, names); err == nil {
			t.Fatal("错误候选输入应明确报错")
		}
	}
	if got, err := scorer.Score(context.Background(), a, nil); err != nil || len(got) != 0 {
		t.Fatal("无候选时不应产生分数", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := scorer.Score(ctx, a, []string{"node-a"}); err == nil {
		t.Fatal("取消后仍然评分")
	}
}
