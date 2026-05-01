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
	bucket    *limiter.TokenBucket
	stateMgr  *state.Manager

	dlStatsMu sync.RWMutex
	dlStats   *stats.DownloadStats

	stateMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	windowStart       time.Time
	windowStartBytes  int64
	windowTargetBytes atomic.Int64
	windowConsumed    atomic.Int64

	activeWorkers atomic.Int32
	targetWorkers atomic.Int32

	done chan struct{}
}

func New(cfg *config.Config, coll *collector.Stats, dlStats *stats.DownloadStats, bucket *limiter.TokenBucket, stateMgr *state.Manager) *Controller {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Controller{
		cfg:              cfg,
		collector:        coll,
		dlStats:          dlStats,
		bucket:           bucket,
		stateMgr:         stateMgr,
		ctx:              ctx,
		cancel:           cancel,
		windowStart:      time.Now(),
		windowStartBytes: 0,
		done:             make(chan struct{}),
	}

	c.targetWorkers.Store(int32(cfg.MaxConcurrent))

	return c
}

func (c *Controller) getDlStats() *stats.DownloadStats {
	c.dlStatsMu.RLock()
	defer c.dlStatsMu.RUnlock()
	return c.dlStats
}

func (c *Controller) setDlStats(s *stats.DownloadStats) {
	c.dlStatsMu.Lock()
	defer c.dlStatsMu.Unlock()
	c.dlStats = s
}

func (c *Controller) RestoreState(s *state.PersistedState) {
	if s == nil {
		return
	}

	logger.Info("Restoring from persisted state: saved_at=%s, mode=%s", s.SavedAt.Format("2006-01-02 15:04:05"), s.Mode)

	dl := c.getDlStats()
	dl.LoadFromState(s.TotalBytesDown, s.SuccessRequests, s.FailedRequests, s.TotalRequests)

	c.collector.SetTotalTxBytesBase(s.TotalBytesUp)

	if s.Mode == "traffic" {
		windowDur := c.cfg.WindowDurationParsed()
		elapsed := time.Since(s.WindowStart)

		if elapsed < windowDur {
			c.stateMu.Lock()
			c.windowStart = s.WindowStart
			c.windowStartBytes = s.TotalBytesDown
			c.stateMu.Unlock()

			logger.Info("Window still valid: elapsed %s, remaining %s",
				elapsed.Round(time.Second),
				(windowDur - elapsed).Round(time.Second))

			c.collector.SetWindowTxBytesBase(s.WindowTxBytesAtStart)
		} else {
			logger.Info("Window expired (elapsed %s), starting fresh", elapsed.Round(time.Second))
			c.stateMu.Lock()
			c.windowStart = time.Now()
			c.windowStartBytes = 0
			c.stateMu.Unlock()
			c.collector.ResetWindow()
		}
	}
}

func (c *Controller) BuildState() *state.PersistedState {
	c.stateMu.Lock()
	ws := c.windowStart
	c.stateMu.Unlock()

	dl := c.getDlStats()

	return &state.PersistedState{
		Mode:                 c.cfg.Mode,
		WindowStart:          ws,
		TotalBytesDown:       dl.GetTotalBytes(),
		TotalBytesUp:         c.collector.GetTotalTxBytes(),
		WindowTxBytesAtStart: c.collector.GetWindowTxBytes(),
		SuccessRequests:      dl.GetSuccessRequests(),
		FailedRequests:       dl.GetFailedRequests(),
		TotalRequests:        dl.GetTotalRequests(),
		InterfaceName:        c.collector.Interface(),
		WindowDuration:       c.cfg.WindowDuration,
	}
}

func (c *Controller) Start() {
	if c.stateMgr != nil {
		c.stateMgr.StartAutoSave(c.BuildState)
	}

	go c.collectorLoop()
	go c.controlLoop()

	switch c.cfg.Mode {
	case "bandwidth":
		go c.ratioMode()
	case "traffic":
		go c.cumulativeMode()
	}

	go c.statsReporter()
	go c.downloadWorkerPool()

	<-c.done
}

func (c *Controller) SaveAndStop() {
	if c.stateMgr != nil {
		if s := c.BuildState(); s != nil {
			if err := c.stateMgr.Save(s); err != nil {
				logger.Warn("Failed to save state on exit: %v", err)
			} else {
				logger.Info("Exit state saved to %s", c.stateMgr.FilePath())
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

	defer func() {
		if r := recover(); r != nil {
			logger.Error("controlLoop panic: %v", r)
		}
		close(c.done)
	}()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.getDlStats().UpdateRate()
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
	dl := c.getDlStats()
	totalRx := dl.GetTotalBytes()
	totalUplink := c.collector.GetTotalTxBytes()
	rate := dl.GetRate()
	active := dl.GetActive()
	uptime := dl.Uptime()

	switch c.cfg.Mode {
	case "bandwidth":
		var currentRatio float64
		if bw.TxBps > 0 {
			currentRatio = bw.RxBps / bw.TxBps
		}
		staleIndicator := ""
		if c.collector.IsStale() {
			staleIndicator = " [STALE]"
		}
		logger.Info(
			"[STATS] mode=bandwidth | uptime=%s | up=%s down=%s%s | ratio=%.2f (target=%.2f) | dl_rate=%s | total_down=%s total_up=%s | workers=%d/%d | success=%d failed=%d",
			uptime.Round(time.Second),
			dl.FormatRate(bw.TxBps),
			dl.FormatRate(bw.RxBps),
			staleIndicator,
			currentRatio,
			c.cfg.Ratio,
			dl.FormatRate(rate),
			dl.FormatBytes(totalRx),
			dl.FormatBytes(int64(totalUplink)),
			active,
			c.targetWorkers.Load(),
			dl.GetSuccessRequests(),
			dl.GetFailedRequests(),
		)

	case "traffic":
		windowUplink := c.collector.GetWindowTxBytes()
		targetBytes := c.windowTargetBytes.Load()
		remaining := c.windowDurationRemaining()

		c.stateMu.Lock()
		wsb := c.windowStartBytes
		c.stateMu.Unlock()

		windowDownBytes := totalRx - wsb
		if windowDownBytes < 0 {
			windowDownBytes = 0
		}

		var progress float64
		if targetBytes > 0 {
			progress = float64(windowDownBytes) / float64(targetBytes) * 100
		}

		logger.Info(
			"[STATS] mode=traffic | uptime=%s | window_remaining=%s | window_uplink=%s | target_down=%s (%.1f%%) | dl_rate=%s | window_down=%s total_down=%s | workers=%d/%d | success=%d failed=%d",
			uptime.Round(time.Second),
			remaining.Round(time.Second),
			dl.FormatBytes(int64(windowUplink)),
			dl.FormatBytes(targetBytes),
			progress,
			dl.FormatRate(rate),
			dl.FormatBytes(windowDownBytes),
			dl.FormatBytes(totalRx),
			active,
			c.targetWorkers.Load(),
			dl.GetSuccessRequests(),
			dl.GetFailedRequests(),
		)
	}
}

func (c *Controller) windowDurationRemaining() time.Duration {
	c.stateMu.Lock()
	ws := c.windowStart
	c.stateMu.Unlock()

	elapsed := time.Since(ws)
	remaining := c.cfg.WindowDurationParsed() - elapsed
	if remaining < 0 {
		remaining = 0
	}
	return remaining
}

func (c *Controller) ratioMode() {
	interval := time.Duration(c.cfg.UplinkSampleInterval) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	const smoothingFactor = 0.3

	var smoothedRate float64

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			bw := c.collector.GetBandwidth()

			if c.collector.IsStale() {
				logger.Debug("Ratio mode: collector stale, keeping current rate")
				continue
			}

			targetDownBps := bw.TxBps * c.cfg.Ratio

			if c.cfg.BandwidthLimitMbps > 0 {
				maxBps := c.cfg.BandwidthLimitMbps * 1000 * 1000 / 8
				if targetDownBps > maxBps {
					targetDownBps = maxBps
				}
			}

			if targetDownBps < 1 {
				if smoothedRate > 0 {
					smoothedRate = smoothedRate * (1 - smoothingFactor)
				}
				if smoothedRate < 1 {
					c.bucket.Stop()
					c.targetWorkers.Store(0)
					logger.Debug("Ratio mode: uplink near zero, stopping downloads")
					continue
				}
			} else {
				if smoothedRate > 0 {
					smoothedRate = smoothedRate*(1-smoothingFactor) + targetDownBps*smoothingFactor
				} else {
					smoothedRate = targetDownBps
				}
			}

			c.bucket.SetRate(smoothedRate)

			if smoothedRate < 100*1024 {
				c.targetWorkers.Store(1)
			} else if smoothedRate < 1024*1024 {
				c.targetWorkers.Store(min(2, int32(c.cfg.MaxConcurrent)))
			} else {
				c.targetWorkers.Store(int32(c.cfg.MaxConcurrent))
			}

			logger.Debug("Ratio mode: up=%.0f B/s, target_down=%.0f B/s, smoothed=%.0f B/s, workers=%d",
				bw.TxBps, targetDownBps, smoothedRate, c.targetWorkers.Load())
		}
	}
}

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
				logger.Info("Window expired, resetting...")
				c.resetWindow()
				continue
			}

			windowUplink := c.collector.GetWindowTxBytes()
			targetBytes := int64(float64(windowUplink) * c.cfg.CumulativeMultiplier)
			c.windowTargetBytes.Store(targetBytes)

			dl := c.getDlStats()
			c.stateMu.Lock()
			wsb := c.windowStartBytes
			c.stateMu.Unlock()

			windowDownBytes := dl.GetTotalBytes() - wsb
			if windowDownBytes < 0 {
				windowDownBytes = 0
			}

			remaining := targetBytes - windowDownBytes
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

			logger.Debug("Cumulative: window_uplink=%d, target=%d, window_down=%d, remaining=%d, rate=%.0f B/s, time_left=%v",
				windowUplink, targetBytes, windowDownBytes, remaining, requiredRate, c.windowDurationRemaining().Round(time.Second))
		}
	}
}

func (c *Controller) resetWindow() {
	newDl := stats.NewDownloadStats()

	c.stateMu.Lock()
	c.windowStart = time.Now()
	c.windowStartBytes = 0
	c.stateMu.Unlock()

	c.setDlStats(newDl)
	c.collector.ResetWindow()
	c.windowTargetBytes.Store(0)
	c.windowConsumed.Store(0)

	if c.stateMgr != nil {
		if s := c.BuildState(); s != nil {
			if err := c.stateMgr.Save(s); err != nil {
				logger.Warn("Failed to save state after window reset: %v", err)
			}
		}
	}

	logger.Info("Window reset complete")
}

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
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		dl := downloader.New(c.cfg, c.bucket, c.getDlStats())
		result := dl.RunOnce()

		if result.Error != nil && c.ctx.Err() == nil {
			logger.Warn("Worker %d: download error: %v", id, result.Error)
		}

		select {
		case <-c.ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}
