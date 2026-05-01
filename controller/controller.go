package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"fetch-down/collector"
	"fetch-down/config"
	"fetch-down/downloader"
	"fetch-down/limiter"
	"fetch-down/logger"
	"fetch-down/state"
	"fetch-down/stats"
)

type Controller struct {
	cfg       *config.Config
	collector *collector.Stats
	dlStats   *stats.DownloadStats
	bucket    *limiter.TokenBucket
	stateMgr  *state.Manager

	ctx    context.Context
	cancel context.CancelFunc

	// 累计模式专用
	windowStart       time.Time
	windowTargetBytes atomic.Int64
	windowConsumed    atomic.Int64

	// worker 数量管理
	activeWorkers atomic.Int32
	targetWorkers atomic.Int32

	done chan struct{}
}

func New(cfg *config.Config, coll *collector.Stats, dlStats *stats.DownloadStats, bucket *limiter.TokenBucket, stateMgr *state.Manager) *Controller {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Controller{
		cfg:         cfg,
		collector:   coll,
		dlStats:     dlStats,
		bucket:      bucket,
		stateMgr:    stateMgr,
		ctx:         ctx,
		cancel:      cancel,
		windowStart: time.Now(),
		done:        make(chan struct{}),
	}

	c.targetWorkers.Store(int32(cfg.MaxConcurrent))

	return c
}

// RestoreState 从持久化状态恢复运行现场
func (c *Controller) RestoreState(s *state.PersistedState) {
	if s == nil {
		return
	}

	logger.Info("从持久化状态恢复: saved_at=%s, mode=%s", s.SavedAt.Format("2006-01-02 15:04:05"), s.Mode)

	// 恢复下载统计
	c.dlStats.LoadFromState(s.TotalBytesDown, s.SuccessRequests, s.FailedRequests, s.TotalRequests)

	if s.Mode == "cumulative" {
		// 检查窗口是否仍有效
		windowDur := c.cfg.WindowDurationParsed()
		elapsed := time.Since(s.WindowStart)

		if elapsed < windowDur {
			// 窗口仍然有效，恢复窗口状态
			c.windowStart = s.WindowStart
			logger.Info("窗口仍在有效期内: 已过 %s, 剩余 %s",
				elapsed.Round(time.Second),
				(windowDur - elapsed).Round(time.Second))

			// 恢复 collector 的窗口累计上行字节数
			c.collector.SetWindowTxBytesBase(s.WindowTxBytesAtStart)
		} else {
			// 窗口已过期，重置
			logger.Info("窗口已过期 (过了 %s)，将重新开始", elapsed.Round(time.Second))
			c.windowStart = time.Now()
			c.collector.ResetWindow()
		}
	}
}

// BuildState 构建当前状态快照用于持久化
func (c *Controller) BuildState() *state.PersistedState {
	return &state.PersistedState{
		Mode:                 c.cfg.Mode,
		WindowStart:          c.windowStart,
		TotalBytesDown:       c.dlStats.GetTotalBytes(),
		TotalBytesUp:         c.collector.GetTotalTxBytes(),
		WindowTxBytesAtStart: c.collector.GetWindowTxBytes(),
		SuccessRequests:      c.dlStats.GetSuccessRequests(),
		FailedRequests:       c.dlStats.GetFailedRequests(),
		TotalRequests:        c.dlStats.GetTotalRequests(),
		InterfaceName:        c.collector.Interface(),
		WindowDuration:       c.cfg.WindowDuration,
	}
}

func (c *Controller) Start() {
	// 启动自动保存
	if c.stateMgr != nil {
		c.stateMgr.StartAutoSave(c.BuildState)
	}

	go c.collectorLoop()
	go c.controlLoop()

	switch c.cfg.Mode {
	case "ratio":
		go c.ratioMode()
	case "cumulative":
		go c.cumulativeMode()
	}

	go c.statsReporter()
	go c.downloadWorkerPool()

	<-c.done
}

// SaveAndStop 保存状态后优雅退出
func (c *Controller) SaveAndStop() {
	// 先保存当前状态
	if c.stateMgr != nil {
		if s := c.BuildState(); s != nil {
			if err := c.stateMgr.Save(s); err != nil {
				logger.Warn("退出时保存状态失败: %v", err)
			} else {
				logger.Info("退出状态已保存到 %s", c.stateMgr.FilePath())
			}
			c.stateMgr.Stop()
		}
	}

	c.cancel()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
	}
}

func (c *Controller) Stop() {
	c.cancel()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
	}
}

func (c *Controller) collectorLoop() {
	interval := time.Duration(c.cfg.UplinkSampleInterval) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.collector.Update()
		}
	}
}

func (c *Controller) controlLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			close(c.done)
			return
		case <-ticker.C:
			c.dlStats.UpdateRate()
		}
	}
}

func (c *Controller) statsReporter() {
	interval := time.Duration(c.cfg.StatsIntervalSec) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.printStats()
		}
	}
}

func (c *Controller) printStats() {
	bw := c.collector.GetBandwidth()
	totalRx := c.dlStats.GetTotalBytes()
	totalUplink := c.collector.GetTotalTxBytes()
	rate := c.dlStats.GetRate()
	active := c.dlStats.GetActive()
	uptime := c.dlStats.Uptime()

	switch c.cfg.Mode {
	case "ratio":
		var currentRatio float64
		if bw.TxBps > 0 {
			currentRatio = bw.RxBps / bw.TxBps
		}
		logger.Info(
			"[STATS] mode=ratio | uptime=%s | up=%s down=%s | ratio=%.2f (target=%.2f) | dl_rate=%s | total_down=%s total_up=%s | workers=%d/%d | success=%d failed=%d",
			uptime.Round(time.Second),
			c.dlStats.FormatRate(bw.TxBps),
			c.dlStats.FormatRate(bw.RxBps),
			currentRatio,
			c.cfg.Ratio,
			c.dlStats.FormatRate(rate),
			c.dlStats.FormatBytes(totalRx),
			c.dlStats.FormatBytes(int64(totalUplink)),
			active,
			c.targetWorkers.Load(),
			c.dlStats.GetSuccessRequests(),
			c.dlStats.GetFailedRequests(),
		)

	case "cumulative":
		windowUplink := c.collector.GetWindowTxBytes()
		targetBytes := c.windowTargetBytes.Load()
		remaining := c.windowDurationRemaining()

		var progress float64
		if targetBytes > 0 {
			progress = float64(totalRx) / float64(targetBytes) * 100
		}

		logger.Info(
			"[STATS] mode=cumulative | uptime=%s | window_remaining=%s | window_uplink=%s | target_down=%s (%.1f%%) | dl_rate=%s | total_down=%s | workers=%d/%d | success=%d failed=%d",
			uptime.Round(time.Second),
			remaining.Round(time.Second),
			c.dlStats.FormatBytes(int64(windowUplink)),
			c.dlStats.FormatBytes(targetBytes),
			progress,
			c.dlStats.FormatRate(rate),
			c.dlStats.FormatBytes(totalRx),
			active,
			c.targetWorkers.Load(),
			c.dlStats.GetSuccessRequests(),
			c.dlStats.GetFailedRequests(),
		)
	}
}

func (c *Controller) windowDurationRemaining() time.Duration {
	elapsed := time.Since(c.windowStart)
	remaining := c.cfg.WindowDurationParsed() - elapsed
	if remaining < 0 {
		remaining = 0
	}
	return remaining
}

// ratioMode 实时比例模式：动态调整下载带宽以维持上/下行比例
func (c *Controller) ratioMode() {
	interval := time.Duration(c.cfg.UplinkSampleInterval) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	const smoothingFactor = 0.3

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			bw := c.collector.GetBandwidth()

			targetDownBps := bw.TxBps * c.cfg.Ratio

			if c.cfg.BandwidthLimitMbps > 0 {
				maxBps := c.cfg.BandwidthLimitMbps * 1000 * 1000 / 8
				if targetDownBps > maxBps {
					targetDownBps = maxBps
				}
			}

			currentRate := c.bucket.GetRate()
			if currentRate > 0 {
				newRate := currentRate*(1-smoothingFactor) + targetDownBps*smoothingFactor
				c.bucket.SetRate(newRate)
			} else {
				c.bucket.SetRate(targetDownBps)
			}

			if targetDownBps < 100*1024 {
				c.targetWorkers.Store(1)
			} else if targetDownBps < 1024*1024 {
				c.targetWorkers.Store(min(2, int32(c.cfg.MaxConcurrent)))
			} else {
				c.targetWorkers.Store(int32(c.cfg.MaxConcurrent))
			}

			logger.Debug("Ratio mode: up=%.0f B/s, target_down=%.0f B/s, actual_limit=%.0f B/s, workers=%d",
				bw.TxBps, targetDownBps, c.bucket.GetRate(), c.targetWorkers.Load())
		}
	}
}

// cumulativeMode 累计流量模式：窗口内入网 = 出网 × 倍数
func (c *Controller) cumulativeMode() {
	interval := time.Duration(c.cfg.UplinkSampleInterval) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if c.windowDurationRemaining() <= 0 {
				logger.Info("窗口到期，重置...")
				c.resetWindow()
				continue
			}

			windowUplink := c.collector.GetWindowTxBytes()
			targetBytes := int64(float64(windowUplink) * c.cfg.CumulativeMultiplier)
			c.windowTargetBytes.Store(targetBytes)

			remaining := targetBytes - c.dlStats.GetTotalBytes()
			if remaining <= 0 {
				c.bucket.SetRate(1024)
				c.targetWorkers.Store(1)
				continue
			}

			remainingTime := c.windowDurationRemaining().Seconds()
			if remainingTime < 1 {
				remainingTime = 1
			}

			requiredRate := float64(remaining) / remainingTime

			if c.cfg.BandwidthLimitMbps > 0 {
				maxBps := c.cfg.BandwidthLimitMbps * 1000 * 1000 / 8
				if requiredRate > maxBps {
					requiredRate = maxBps
				}
			}

			c.bucket.SetRate(requiredRate)

			if requiredRate < 100*1024 {
				c.targetWorkers.Store(1)
			} else if requiredRate < 1024*1024 {
				c.targetWorkers.Store(min(2, int32(c.cfg.MaxConcurrent)))
			} else {
				c.targetWorkers.Store(int32(c.cfg.MaxConcurrent))
			}

			logger.Debug("Cumulative: window_uplink=%d, target=%d, remaining=%d, rate=%.0f B/s, time_left=%v",
				windowUplink, targetBytes, remaining, requiredRate, c.windowDurationRemaining().Round(time.Second))
		}
	}
}

func (c *Controller) resetWindow() {
	c.windowStart = time.Now()
	c.collector.ResetWindow()
	c.windowTargetBytes.Store(0)
	c.windowConsumed.Store(0)
	c.dlStats = stats.NewDownloadStats()

	// 重置后立即保存一次状态
	if c.stateMgr != nil {
		if s := c.BuildState(); s != nil {
			if err := c.stateMgr.Save(s); err != nil {
				logger.Warn("窗口重置后保存状态失败: %v", err)
			}
		}
	}

	logger.Info("窗口重置完成")
}

// downloadWorkerPool 管理并发下载 worker 协程池
func (c *Controller) downloadWorkerPool() {
	var wg sync.WaitGroup

	for i := 0; i < c.cfg.MaxConcurrent; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.worker(workerID)
		}(i)
	}

	wg.Wait()
}

func (c *Controller) worker(id int) {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if int32(id) >= c.targetWorkers.Load() {
			time.Sleep(time.Second)
			continue
		}

		dl := downloader.New(c.cfg, c.bucket, c.dlStats)
		result := dl.RunOnce()

		if result.Error != nil && c.ctx.Err() == nil {
			logger.Warn("Worker %d: 下载错误: %v", id, result.Error)
		}

		select {
		case <-c.ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}
