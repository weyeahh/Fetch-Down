package downloader

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync/atomic"
	"time"

	"fetch-down/config"
	"fetch-down/limiter"
	"fetch-down/logger"
	"fetch-down/stats"
)

type DownloadResult struct {
	Bytes     int64
	Duration  time.Duration
	StatusCode int
	URL       string
	Error     error
}

type Downloader struct {
	cfg       *config.Config
	limiter   *limiter.TokenBucket
	dlStats   *stats.DownloadStats
	urlIndex  atomic.Int64
	client    *http.Client
	ctx       context.Context
	cancel    context.CancelFunc
}

func New(cfg *config.Config, bucket *limiter.TokenBucket, dlStats *stats.DownloadStats) *Downloader {
	ctx, cancel := context.WithCancel(context.Background())

	transport := &http.Transport{
		MaxIdleConns:        cfg.MaxConcurrent * 2,
		MaxIdleConnsPerHost: cfg.MaxConcurrent * 2,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(cfg.ReadTimeoutSec) * time.Second,
	}

	return &Downloader{
		cfg:     cfg,
		limiter: bucket,
		dlStats: dlStats,
		client:  client,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (d *Downloader) Stop() {
	d.cancel()
}

func (d *Downloader) nextURL() string {
	if len(d.cfg.DownloadURLs) == 1 {
		return d.cfg.DownloadURLs[0]
	}
	idx := d.urlIndex.Add(1)
	return d.cfg.DownloadURLs[int(idx)%len(d.cfg.DownloadURLs)]
}

// RunOnce performs a single download with retry logic.
func (d *Downloader) RunOnce() DownloadResult {
	url := d.nextURL()

	var lastErr error
	maxRetries := d.cfg.MaxRetries

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if d.ctx.Err() != nil {
			return DownloadResult{Error: d.ctx.Err(), URL: url}
		}

		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))*float64(time.Second))
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			logger.Debug("Retry %d/%d for %s after %v", attempt, maxRetries, url, backoff)
			select {
			case <-d.ctx.Done():
				return DownloadResult{Error: d.ctx.Err(), URL: url}
			case <-time.After(backoff):
			}
		}

		result := d.doDownload(url)
		if result.Error == nil {
			return result
		}
		lastErr = result.Error
		logger.Warn("Download failed (attempt %d/%d) %s: %v", attempt+1, maxRetries+1, url, lastErr)
	}

	return DownloadResult{
		Error: fmt.Errorf("all %d attempts failed for %s: %w", maxRetries+1, url, lastErr),
		URL:   url,
	}
}

func (d *Downloader) doDownload(url string) DownloadResult {
	d.dlStats.IncrRequests()
	d.dlStats.IncrActive()
	defer d.dlStats.DecrActive()

	start := time.Now()

	ctx, cancel := context.WithTimeout(d.ctx, time.Duration(d.cfg.ReadTimeoutSec)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		d.dlStats.IncrFailed()
		return DownloadResult{Error: fmt.Errorf("create request: %w", err), URL: url}
	}

	req.Header.Set("User-Agent", "FetchDown/1.0")

	resp, err := d.client.Do(req)
	if err != nil {
		d.dlStats.IncrFailed()
		return DownloadResult{Error: fmt.Errorf("execute request: %w", err), URL: url}
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		d.dlStats.IncrFailed()
		return DownloadResult{
			Error:      fmt.Errorf("unexpected status code: %d", resp.StatusCode),
			StatusCode: resp.StatusCode,
			URL:        url,
		}
	}

	// Read with rate limiting, discard content
	var totalBytes int64
	buf := make([]byte, 32*1024) // 32KB buffer

	for {
		select {
		case <-ctx.Done():
			d.dlStats.IncrFailed()
			return DownloadResult{
				Bytes:     totalBytes,
				Duration:  time.Since(start),
				StatusCode: resp.StatusCode,
				Error:     ctx.Err(),
				URL:       url,
			}
		default:
		}

		// Rate limit: wait for tokens before reading
		readSize := len(buf)
		if d.limiter != nil {
			d.limiter.Wait(readSize)
		}

		n, err := resp.Body.Read(buf)
		if n > 0 {
			totalBytes += int64(n)
			d.dlStats.AddBytes(int64(n))
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			// context canceled or deadline exceeded
			if d.ctx.Err() != nil {
				return DownloadResult{
					Bytes:     totalBytes,
					Duration:  time.Since(start),
					StatusCode: resp.StatusCode,
					Error:     d.ctx.Err(),
					URL:       url,
				}
			}
			d.dlStats.IncrFailed()
			return DownloadResult{
				Bytes:     totalBytes,
				Duration:  time.Since(start),
				StatusCode: resp.StatusCode,
				Error:     fmt.Errorf("read body: %w", err),
				URL:       url,
			}
		}
	}

	d.dlStats.IncrSuccess()
	duration := time.Since(start)

	rate := float64(totalBytes) / duration.Seconds()
	logger.Info("Download complete: %s | %s in %v | %s",
		url,
		d.dlStats.FormatBytes(totalBytes),
		duration.Round(time.Millisecond),
		d.dlStats.FormatRate(rate),
	)

	return DownloadResult{
		Bytes:     totalBytes,
		Duration:  duration,
		StatusCode: resp.StatusCode,
		URL:       url,
	}
}
