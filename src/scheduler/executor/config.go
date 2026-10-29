package executor

import (
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"
)

// Config 在启动前设置。MaxInFlight 限制等待绑定和正在执行的总轮数；
// BindWorkers 决定同时有多少轮真正调用 Bind，BindTimeout 限制一轮绑定的等待时间。
type Config struct {
	MaxInFlight int
	BindWorkers int
	BindTimeout time.Duration
	Client      kubernetes.Interface
}

func DefaultConfig() Config {
	return Config{MaxInFlight: 4, BindWorkers: 2, BindTimeout: 30 * time.Second}
}

func (c Config) Validate() error {
	if c.MaxInFlight <= 0 || c.BindWorkers <= 0 || c.BindWorkers > c.MaxInFlight || c.BindTimeout <= 0 {
		return fmt.Errorf("MaxInFlight、BindWorkers、BindTimeout 必须为正，且 BindWorkers 不能超过 MaxInFlight")
	}
	return nil
}
