package history

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	"batchscheduler/src/workqueue"
)

func TestSuccessHistorySurvivesReopenAndKeepsDistinctAttempts(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "success.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	key := group.Key{Namespace: "work", Name: "job", UID: "group-old"}
	record := staging.SuccessfulRecord{ID: "record-first", Unit: workqueue.UnitKey{Group: key, ID: "unit-1"}, Mode: workqueue.AllowPartial,
		SavedAt: time.Now().UTC(), Members: []staging.SuccessfulMember{{Pod: workqueue.PodRef{Name: "a", UID: "pod-a"},
			Observed: staging.ObservedPod{Known: true, Present: true, NodeName: "node-a"}}}}
	if err := store.Save(ctx, record); err != nil {
		t.Fatal(err)
	}
	// 重复交接不插第二条。重启后的另一轮即使叫 unit-1，也不能覆盖第一轮。
	if err := store.Save(ctx, record); err != nil {
		t.Fatal(err)
	}
	second := record
	second.ID = "record-second"
	if err := store.Save(ctx, second); err != nil {
		t.Fatal(err)
	}
	conflict := record
	conflict.Members = append([]staging.SuccessfulMember(nil), record.Members...)
	conflict.Members[0].Observed.NodeName = "node-b"
	if err := store.Save(ctx, conflict); err == nil {
		t.Fatal("相同 Record ID 不能被另一份放置覆盖")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.List(ctx, key)
	if err != nil || len(got) != 2 || got[0].Members[0].Observed.NodeName != "node-a" {
		t.Fatalf("重新打开后成功记录丢失或被覆盖: %+v %v", got, err)
	}
	key.UID = "group-new"
	if got, err := store.List(ctx, key); err != nil || len(got) != 0 {
		t.Fatal("同名新组混入旧组历史", err)
	}
}
