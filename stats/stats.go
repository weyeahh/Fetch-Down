package stats

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type DownloadStats struct {
	mu sync.RWMutex

	// atomics for hot path
	totalBytesDownloaded atomic.Int64
	activeDownloads      atomic.Int32
	totalRequests        atomic.Int64
	successRequests      atomic.Int64
	failedRequests       atomic.Int64

	// per-download tracking
	completedDownloads int64
	totalDuration      time.Duration

	// session tracking
	startTime time.Time

	// rate calculation
	lastBytesSnapshot int64
	lastRateTime      time.Time
	currentRate       float64 // bytes/sec
}

func NewDownloadStats() *DownloadStats {
	now := time.Now()
	return &DownloadStats{
		startTime:    now,
		lastRateTime: now,
	}
}

// LoadFromState 从持久化状态恢复累计数据
func (ds *DownloadStats) LoadFromState(totalBytesDown int64, successReqs, failedReqs, totalReqs int64) {
	ds.totalBytesDownloaded.Store(totalBytesDown)
	ds.successRequests.Store(successReqs)
	ds.failedRequests.Store(failedReqs)
	ds.totalRequests.Store(totalReqs)
	ds.lastBytesSnapshot = totalBytesDown
	ds.lastRateTime = time.Now()
}

func (ds *DownloadStats) AddBytes(n int64) {
	ds.totalBytesDownloaded.Add(n)
}

func (ds *DownloadStats) IncrActive() {
	ds.activeDownloads.Add(1)
}

func (ds *DownloadStats) DecrActive() {
	ds.activeDownloads.Add(-1)
}

func (ds *DownloadStats) IncrRequests() {
	ds.totalRequests.Add(1)
}

func (ds *DownloadStats) IncrSuccess() {
	ds.successRequests.Add(1)
	ds.mu.Lock()
	ds.completedDownloads++
	ds.mu.Unlock()
}

func (ds *DownloadStats) IncrFailed() {
	ds.failedRequests.Add(1)
}

func (ds *DownloadStats) GetTotalBytes() int64 {
	return ds.totalBytesDownloaded.Load()
}

func (ds *DownloadStats) GetActive() int32 {
	return ds.activeDownloads.Load()
}

func (ds *DownloadStats) GetTotalRequests() int64 {
	return ds.totalRequests.Load()
}

func (ds *DownloadStats) GetSuccessRequests() int64 {
	return ds.successRequests.Load()
}

func (ds *DownloadStats) GetFailedRequests() int64 {
	return ds.failedRequests.Load()
}

func (ds *DownloadStats) UpdateRate() {
	now := time.Now()
	currentBytes := ds.totalBytesDownloaded.Load()

	ds.mu.Lock()
	dt := now.Sub(ds.lastRateTime).Seconds()
	if dt > 0 {
		ds.currentRate = float64(currentBytes-ds.lastBytesSnapshot) / dt
	}
	ds.lastBytesSnapshot = currentBytes
	ds.lastRateTime = now
	ds.mu.Unlock()
}

func (ds *DownloadStats) GetRate() float64 {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.currentRate
}

func (ds *DownloadStats) Uptime() time.Duration {
	return time.Since(ds.startTime)
}

func (ds *DownloadStats) FormatBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case b >= TB:
		return fmt.Sprintf("%.2f TB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func (ds *DownloadStats) FormatRate(bps float64) string {
	const (
		Kbps = 1000.0
		Mbps = Kbps * 1000.0
		Gbps = Mbps * 1000.0
	)

	bitsPerSec := bps * 8

	switch {
	case bitsPerSec >= Gbps:
		return fmt.Sprintf("%.2f Gbps", bitsPerSec/Gbps)
	case bitsPerSec >= Mbps:
		return fmt.Sprintf("%.2f Mbps", bitsPerSec/Mbps)
	case bitsPerSec >= Kbps:
		return fmt.Sprintf("%.2f Kbps", bitsPerSec/Kbps)
	default:
		return fmt.Sprintf("%.0f bps", bitsPerSec)
	}
}
