package executor

import (
	"fmt"
	"time"

	"batchscheduler/src/scheduler/resourcecache"
	"k8s.io/client-go/kubernetes"
)

// Config 在启动前设置。MaxInFlight 限制等待绑定和正在执行的总轮数；
// BindWorkers 决定同时有多少轮真正调用 Bind，BindTimeout 限制一轮绑定的等待时间。
type Config struct {
	MaxInFlight          int
	BindWorkers          int
	BindTimeout          time.Duration
	BindRetry            BindRetryPolicy
	BindingCheckInterval time.Duration
	Client               kubernetes.Interface
}

func DefaultConfig() Config {
	return Config{MaxInFlight: 4, BindWorkers: 2, BindTimeout: 30 * time.Second,
		BindingCheckInterval: resourcecache.DefaultBindingCheckPolicy().CheckInterval,
		BindRetry: BindRetryPolicy{MaxAttempts: 5, RequestTimeout: 3 * time.Second,
			InitialBackoff: 200 * time.Millisecond, MaxBackoff: 1600 * time.Millisecond}}
}

func (c Config) Validate() error {
	if c.MaxInFlight <= 0 || c.BindWorkers <= 0 || c.BindWorkers > c.MaxInFlight || c.BindTimeout <= 0 {
		return fmt.Errorf("MaxInFlight、BindWorkers、BindTimeout 必须为正，且 BindWorkers 不能超过 MaxInFlight")
	}
	if c.BindRetry.MaxAttempts < 1 || c.BindRetry.RequestTimeout <= 0 ||
		c.BindRetry.InitialBackoff <= 0 || c.BindRetry.MaxBackoff < c.BindRetry.InitialBackoff {
		return fmt.Errorf("绑定重试次数、请求超时和退避间隔必须为正，最大间隔不能小于初始间隔")
	}
	if c.BindingCheckInterval <= 0 {
		return fmt.Errorf("后台绑定核查间隔必须为正")
	}
	return nil
}
