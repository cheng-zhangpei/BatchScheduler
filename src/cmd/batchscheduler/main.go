package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"

	"batchscheduler/src/group"
	"batchscheduler/src/informer"
	"batchscheduler/src/staging"
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
	flag.Parse()
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
	queues := workqueue.New()
	queues.Views = observed.GroupViews
	center := staging.NewBuffer(observed.GroupViews)
	queues.Staging = center
	// 观察路径与 Active 出队接收路径并行；先接 Observe，Accept 消费循环后续实现。
	// 回调发生时该 informer 的缓存已更新。Observe 只重查本地 Store 并登记事实，
	// 不等待初始全量同步，也不执行调度或队列迁移；两种资源的后续事件会继续校正。
	if err := observed.OnGroupChange(func(key group.Key) {
		if err := center.Observe(ctx, staging.Observation{Group: key}); err != nil && ctx.Err() == nil {
			slog.Error("更新调度中心观察失败", "namespace", key.Namespace, "group", key.Name, "error", err)
		}
		// Observe 只更新事实并提交去重 Key；评估由下面独立 worker 并发运行，
		// 不在 informer 回调里执行 Evaluate/Apply，避免阻塞观察路径。
	}); err != nil {
		return err
	}
	ready := make(chan struct{})
	done := make(chan error, 1)
	// 这里开一个go routine专门用来ListAndWatch
	go func() { done <- observed.Run(ctx, ready) }()
	slog.Info("正在同步 Pod/Node/PodGroup informer 缓存，按 Ctrl+C 退出")

	select {
	case err := <-done:
		return err
	case <-ready:
	}
	// 缓存就绪后才启动评估，避免把初始跨资源 List 顺序当作任务异常。
	evaluationDone := make(chan error, 1)
	go func() {
		evaluationDone <- center.RunEvaluations(ctx, 32, func(_ context.Context, decisions []staging.Decision) error {
			for _, decision := range decisions {
				if decision.Action != staging.ActionWait {
					slog.Debug("组评估完成，动作执行待接入", "group", decision.Group, "unit", decision.Unit, "action", decision.Action, "reason", decision.Reason)
				}
			}
			// TODO：接 Apply；当前只产出决策，不假装已经调度、回滚或清理。
			return nil
		})
	}()
	defer func() {
		stop()
		if err := <-evaluationDone; err != nil {
			slog.Error("评估 worker 退出", "error", err)
		}
	}()

	// 从本地缓存读取，只用于验证观察链路；这不是调度器的资源快照。
	nodes, err := observed.Nodes.List(labels.Everything())
	if err != nil {
		stop()
		<-done
		return err
	}
	pods, err := observed.Pods.List(labels.Everything())
	if err != nil {
		stop()
		<-done
		return err
	}
	groups, err := observed.PodGroups.List(labels.Everything())
	if err != nil {
		stop()
		<-done
		return err
	}
	slog.Info("informer 初始缓存已同步", "nodes", len(nodes), "pods", len(pods), "podGroups", len(groups))
	// 先把调用位置放在 main：初始同步后，逐组读取视图并交给 Select。
	// Select 使用默认首次入队策略；后续变化的事件回调尚未接入。
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
	return <-done
}
