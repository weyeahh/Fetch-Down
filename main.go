package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"fetch-down/collector"
	"fetch-down/config"
	"fetch-down/controller"
	"fetch-down/limiter"
	"fetch-down/logger"
	"fetch-down/state"
	"fetch-down/stats"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger.Init(cfg.LogLevel)
	logger.Info("Starting Fetch-Down in %s mode", cfg.Mode)

	coll, err := collector.New(cfg.UplinkInterface)
	if err != nil {
		logger.Error("Failed to initialize collector: %v", err)
		os.Exit(1)
	}

	dlStats := stats.NewDownloadStats()

	var bucket *limiter.TokenBucket
	if cfg.BandwidthLimitMbps > 0 {
		bpsLimit := cfg.BandwidthLimitMbps * 1000 * 1000 / 8
		bucket = limiter.NewTokenBucket(bpsLimit, bpsLimit)
		logger.Info("Bandwidth limit: %.1f Mbps (%.0f bytes/sec)", cfg.BandwidthLimitMbps, bpsLimit)
	} else {
		bucket = limiter.NewTokenBucket(0, 0)
		logger.Info("Bandwidth limit: unlimited")
	}

	logger.Info("Config: max_concurrent=%d, urls=%d, uplink_interface=%s",
		cfg.MaxConcurrent, len(cfg.DownloadURLs), coll.Interface())
	if cfg.Mode == "ratio" {
		logger.Info("Ratio mode: target down/up ratio = %.2f", cfg.Ratio)
	} else {
		logger.Info("Cumulative mode: multiplier=%.2f, window=%s",
			cfg.CumulativeMultiplier, cfg.WindowDuration)
	}

	// 初始化状态管理器
	var stateMgr *state.Manager
	if cfg.StateFile != "" {
		stateMgr = state.NewManager(cfg.StateFile, cfg.StateSaveIntervalParsed())
		logger.Info("State persistence: file=%s, save_interval=%ds", cfg.StateFile, cfg.StateSaveInterval)
	}

	ctrl := controller.New(cfg, coll, dlStats, bucket, stateMgr)

	// 加载持久化状态
	if stateMgr != nil {
		savedState, err := stateMgr.Load()
		if err != nil {
			logger.Warn("加载状态文件失败: %v", err)
		} else if savedState != nil {
			ctrl.RestoreState(savedState)
		} else {
			logger.Info("无持久化状态文件，全新启动")
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("Received signal: %v, shutting down...", sig)

		// 保存状态后退出
		ctrl.SaveAndStop()

		logger.Info("=== Final Statistics ===")
		logger.Info("Total downloaded: %s", dlStats.FormatBytes(dlStats.GetTotalBytes()))
		logger.Info("Total requests: %d (success: %d, failed: %d)",
			dlStats.GetTotalRequests(), dlStats.GetSuccessRequests(), dlStats.GetFailedRequests())
		logger.Info("Uptime: %s", dlStats.Uptime())
		logger.Info("=== Shutdown complete ===")

		os.Exit(0)
	}()

	ctrl.Start()
}
