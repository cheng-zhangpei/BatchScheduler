// Package history 用 BoltDB 保存调度成功的记录，供同组后续 Unit 查询。
package history

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"batchscheduler/src/group"
	"batchscheduler/src/staging"
	bolt "go.etcd.io/bbolt"
)

var successes = []byte("successful-records-v1")

type Store struct{ db *bolt.DB }

var _ staging.SuccessHistory = (*Store)(nil)

// Open 打开本地文件。使用正常同步写入；事务提交成功后，Buffer 才能继续清理。
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("打开成功记录数据库: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Save 按组身份和 Record ID 保存成功快照。重复交接保留第一次快照，不重复插入。
func (s *Store) Save(ctx context.Context, record staging.SuccessfulRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := record.Unit.Group
	if key.Namespace == "" || key.Name == "" || key.UID == "" || record.ID == "" || record.Unit.ID == "" || len(record.Members) == 0 {
		return fmt.Errorf("成功记录缺少组、Record、Unit 或成员身份")
	}
	for _, member := range record.Members {
		if member.Pod.Name == "" || member.Pod.UID == "" || !member.Observed.Known || !member.Observed.Present || member.Observed.NodeName == "" {
			return fmt.Errorf("成功记录的成员必须已确认绑定")
		}
	}
	groupKey, _ := json.Marshal(key)
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		root, err := tx.CreateBucketIfNotExists(successes)
		if err != nil {
			return err
		}
		bucket, err := root.CreateBucketIfNotExists(groupKey)
		if err != nil {
			return err
		}
		if previous := bucket.Get([]byte(record.ID)); previous != nil {
			var saved staging.SuccessfulRecord
			if err := json.Unmarshal(previous, &saved); err != nil {
				return err
			}
			// 重试交接可以改变 Revision/状态时间，但不能让同一编号指向另一轮或另一批成员。
			if saved.Unit != record.Unit || saved.Mode != record.Mode || saved.Generation != record.Generation || !samePlacements(saved.Members, record.Members) {
				return fmt.Errorf("成功记录编号重复但内容不一致: %s", record.ID)
			}
			return nil
		}
		return bucket.Put([]byte(record.ID), data)
	})
}

// List 只查询一个组，组 UID 不同则分开保存，不会混入同名重建组的旧记录。
func (s *Store) List(ctx context.Context, key group.Key) ([]staging.SuccessfulRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	groupKey, _ := json.Marshal(key)
	var records []staging.SuccessfulRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(successes)
		if root == nil || root.Bucket(groupKey) == nil {
			return nil
		}
		return root.Bucket(groupKey).ForEach(func(_, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var record staging.SuccessfulRecord
			if err := json.Unmarshal(value, &record); err != nil {
				return err
			}
			records = append(records, record)
			return nil
		})
	})
	return records, err
}

func samePlacements(a, b []staging.SuccessfulMember) bool {
	positions := func(members []staging.SuccessfulMember) map[string]string {
		result := make(map[string]string, len(members))
		for _, member := range members {
			result[member.Pod.Name+"/"+string(member.Pod.UID)] = member.Observed.NodeName
		}
		return result
	}
	return len(a) == len(b) && reflect.DeepEqual(positions(a), positions(b))
}
