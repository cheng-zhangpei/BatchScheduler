package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"

	"batchscheduler/src/informer"
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
	// Select 仍是空入口；这里也只处理启动时的组，后续变化的事件回调尚未接入。
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
		queues.Select(view)
	}
	return <-done
}
