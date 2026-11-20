package resourcecache

import (
	"errors"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestAssumePartialSkipsRejectedMemberAndContinues(t *testing.T) {
	for _, kind := range []string{"resources", "port", "missing-node"} {
		t.Run(kind, func(t *testing.T) {
			c := New()
			c.UpdateNode(observedNode())
			batch := assumptionBatch()
			last := batch[0]
			last.Pod = batch[0].Pod.DeepCopy()
			last.Pod.Name, last.Pod.UID = "c", "c"
			last.Pod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("1")
			batch = append(batch, last)
			want := ErrAssumeNoFit
			switch kind {
			case "resources":
				batch[1].Pod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("3")
			case "port":
				for _, record := range batch[:2] {
					record.Pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{HostPort: 8080, Protocol: corev1.ProtocolTCP}}
				}
			case "missing-node":
				batch[1].Pod.Spec.NodeName = "gone"
				want = ErrAssumeNodeMissing
			}
			for range 2 {
				result, err := c.AssumePartial(batch)
				if err != nil || len(result.Assumed) != 2 || len(result.Rejected) != 1 {
					t.Fatalf("应接纳 A、C，并拒绝 B: %+v, %v", result, err)
				}
				if result.Assumed[0].Pod.Name != "a" || result.Assumed[1].Pod.Name != "c" || result.Rejected[0].Assumption.Pod.Name != "b" || !errors.Is(result.Rejected[0].Err, want) {
					t.Fatal("接纳范围或失败原因不准确")
				}
				if c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 3000 || len(c.assumed) != 2 || c.pods["b"] != nil {
					t.Fatal("拒绝的成员仍被记账，重复提交多扣资源，或漏掉后续可接纳成员")
				}
			}
		})
	}
}

func TestAssumePartialAllRejectedLeavesCacheUnchanged(t *testing.T) {
	c := New()
	node := observedNode()
	node.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("1")
	c.UpdateNode(node)
	before := c.Snapshot()
	result, err := c.AssumePartial(assumptionBatch())
	if err != nil || len(result.Assumed) != 0 || len(result.Rejected) != 2 || !reflect.DeepEqual(before, c.Snapshot()) || len(c.assumed) != 0 {
		t.Fatalf("全拒绝应没有新增占用: %+v, %v", result, err)
	}
}

func TestAssumePartialOwnershipConflictDoesNotCommitNewMembers(t *testing.T) {
	c := New()
	c.UpdateNode(observedNode())
	batch := assumptionBatch()
	other := batch[1]
	other.ActionID = "other-action"
	if err := c.Assume([]Assumption{other}); err != nil {
		t.Fatal(err)
	}
	before := c.Snapshot()
	if _, err := c.AssumePartial(batch); err == nil {
		t.Fatal("执行归属冲突不能被当作普通资源不足跳过")
	}
	if !reflect.DeepEqual(before, c.Snapshot()) || len(c.assumed) != 1 || c.pods["a"] != nil {
		t.Fatal("归属冲突时提交了前面的新占用，或改变了其他执行的占用")
	}
}

func TestConcurrentPartialAssumeCannotOvercommit(t *testing.T) {
	c := New()
	c.UpdateNode(observedNode())
	start := make(chan struct{})
	results := make(chan AssumeResult, 2)
	errs := make(chan error, 2)
	for _, record := range assumptionBatch() {
		record.Pod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("3")
		go func(record Assumption) {
			<-start
			result, err := c.AssumePartial([]Assumption{record})
			results <- result
			errs <- err
		}(record)
	}
	close(start)
	accepted, rejected := 0, 0
	for range 2 {
		result := <-results
		accepted += len(result.Assumed)
		rejected += len(result.Rejected)
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if accepted != 1 || rejected != 1 || len(c.assumed) != 1 || c.Snapshot().Nodes["node-a"].GetRequested().GetMilliCPU() != 3000 {
		t.Fatal("并发 Partial 超卖了资源或没有报告被拒绝的成员")
	}
}
