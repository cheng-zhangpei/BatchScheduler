package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"batchscheduler/src/group"
	"batchscheduler/src/informer"
	execution "batchscheduler/src/scheduler/executor"
	"batchscheduler/src/scheduler/resourcecache"
	"batchscheduler/src/staging"
	"batchscheduler/src/staging/buffer"
	"batchscheduler/src/staging/history"
	"batchscheduler/src/utils"
	"batchscheduler/src/workqueue"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if err := run(); err != nil {
		slog.Error("程序退出", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// 使用 client-go 默认加载规则：--kubeconfig、KUBECONFIG 或 ~/.kube/config。
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	flag.StringVar(&loading.ExplicitPath, "kubeconfig", "", "连接虚拟机集群的 kubeconfig 路径")
	historyPath := flag.String("history-db", ".batchscheduler/history.db", "调度成功记录的本地 BoltDB 文件")
	executionConfig := execution.DefaultConfig()
	flag.IntVar(&executionConfig.MaxInFlight, "max-inflight", executionConfig.MaxInFlight, "调度及等待/正在绑定的最大轮数")
	flag.IntVar(&executionConfig.BindWorkers, "bind-workers", executionConfig.BindWorkers, "后台绑定 worker 数量，不超过 max-inflight")
	flag.DurationVar(&executionConfig.BindTimeout, "bind-timeout", executionConfig.BindTimeout, "一轮绑定的最长等待时间")
	flag.Parse()
	if err := executionConfig.Validate(); err != nil {
		return err
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	groupClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	observed, err := informer.New(client, groupClient)
	if err != nil {
		return err
	}
	resources := resourcecache.New()
	if err := observed.OnResourceChange(resources); err != nil {
		return err
	}
	store, err := history.Open(*historyPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			slog.Error("关闭成功记录数据库失败", "error", err)
		}
	}()
	queues := workqueue.New()
	queues.Views = observed.GroupViews
	executionConfig.Client = client
	executor := execution.NewExecutor(observed.GroupViews, resources, executionConfig)
	center := buffer.NewBuffer(observed.GroupViews, store, newStagingHandlers(queues, observed.Nodes, executor))
	executor.SetGroupContextReader(center)
	executor.Report = center.Report
	queues.Staging = center
	if err := observed.OnRetryChange(center.NotifyRetry); err != nil {
		return err
	}
	// informer 看见组/Pod 变化，就让 Observe 更新记录并通知评估。
	// Active.Pop → Accept 是另一条路径，在缓存就绪后由独立消费循环启动。
	if err := observed.OnGroupChange(func(key group.Key) {
		if err := center.Observe(ctx, staging.Observation{Group: key}); err != nil && ctx.Err() == nil {
			slog.Error("更新调度中心观察失败", "namespace", key.Namespace, "group", key.Name, "error", err)
		}
		// 这里只读缓存、更新观察；Evaluate/Apply 留给下面单独的评估线程。
	}); err != nil {
		return err
	}
	ready := make(chan struct{})
	done := make(chan struct{})
	var informerErr error
	// 这里开一个go routine专门用来ListAndWatch
	go func() {
		defer close(done)
		informerErr = observed.Run(ctx, ready)
	}()
	slog.Info("正在同步 Pod/Node/PodGroup informer 缓存，按 Ctrl+C 退出")

	select {
	case <-done:
		return informerErr
	case <-ready:
	}
	// ready 已包含资源记账回调的初始同步，之后才允许执行调度工作。
	slog.Info("调度资源账初始同步完成", "nodes", len(resources.SnapshotNodeInfos()))
	// 执行器内部串行调度、后台绑定，各轮都通过同一个 Report 回报 Buffer。
	executionDone := make(chan error, 1)
	go func() { executionDone <- executor.Run(ctx) }()
	// 缓存就绪后才启动评估，避免把初始跨资源 List 顺序当作任务异常。
	queueChecksDone := make(chan struct{})
	// 一个定时线程，每秒通知检查 Fail/Retry，让退避时间到后能继续推进。
	go func() {
		defer close(queueChecksDone)
		center.RunQueueChecks(ctx, time.Second)
	}()
	evaluationDone := make(chan error, 1)
	// 这个线程消费组通知。32 是每批最多取多少个组，不是每次调度多少个 Unit。
	go func() {
		evaluationDone <- center.RunEvaluations(ctx, 32, func(ctx context.Context, decisions []staging.Decision) error {
			var failures []error
			for _, decision := range decisions {
				// 依次执行本组的决定；真实执行器需要自己安排耗时工作，别堵住其他组。
				if err := center.Apply(ctx, decision); err != nil {
					switch {
					case errors.Is(err, staging.ErrHandlerUnavailable):
						slog.Debug("动作适配器待接入", "group", decision.Group, "action", decision.Action)
					case errors.Is(err, staging.ErrStaleDecision):
						if err := center.Observe(ctx, staging.Observation{Group: decision.Group}); err != nil {
							failures = append(failures, err)
						}
					default:
						failures = append(failures, err)
					}
				}
			}
			return errors.Join(failures...)
		})
	}()
	activeDone := make(chan struct{})
	var activeErr error
	go func() {
		defer close(activeDone)
		activeErr = runActive(ctx, queues, center)
	}()
	defer func() {
		stop()
		// 先停接收，再等待评估和执行退出；所有后台任务结束后才关闭历史数据库。
		<-activeDone
		<-queueChecksDone
		if err := <-evaluationDone; err != nil {
			slog.Error("评估 worker 退出", "error", err)
		}
		if err := <-executionDone; err != nil {
			slog.Error("执行 worker 退出", "error", err)
		}
		<-done
	}()

	// 从本地缓存读取，只用于验证观察链路；这不是调度器的资源快照。
	nodes, err := observed.Nodes.List(labels.Everything())
	if err != nil {
		return err
	}
	pods, err := observed.Pods.List(labels.Everything())
	if err != nil {
		return err
	}
	groups, err := observed.PodGroups.List(labels.Everything())
	if err != nil {
		return err
	}
	slog.Info("informer 初始缓存已同步", "nodes", len(nodes), "pods", len(pods), "podGroups", len(groups))
	// 先把调用位置放在 main：初始同步后，逐组读取视图并交给 Select。
	// Select 使用默认首次入队策略；后续新成员经 Observe → Evaluate → Apply 再进入 Select。
	for _, object := range groups {
		metadata, err := meta.Accessor(object)
		if err != nil {
			slog.Error("读取组元数据失败", "error", err)
			continue
		}
		view, err := observed.GroupViews.Get(metadata.GetNamespace(), metadata.GetName())
		if err != nil {
			slog.Error("读取组视图失败", "namespace", metadata.GetNamespace(), "name", metadata.GetName(), "error", err)
			continue
		}
		if !utils.ShouldEnqueue(view) {
			continue
		}
		if err := queues.Select(workqueue.SelectRequest{View: view, Nodes: nodes}); err != nil {
			slog.Error("组调度入口处理失败", "namespace", metadata.GetNamespace(), "name", metadata.GetName(), "error", err)
		}
	}
	select {
	case <-done:
		return informerErr
	case <-activeDone:
		return activeErr
	}
}
