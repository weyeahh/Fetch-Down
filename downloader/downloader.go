package downloader

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"time"

	"fetch-down/config"
	"fetch-down/limiter"
	"fetch-down/logger"
	"fetch-down/stats"
)

type DownloadResult struct {
	Bytes      int64
	Duration   time.Duration
	StatusCode int
	URL        string
	Error      error
}

type Downloader struct {
	cfg      *config.Config
	limiter  *limiter.TokenBucket
	dlStats  *stats.DownloadStats
	client   *http.Client
	ctx      context.Context
	cancel   context.CancelFunc
	MaxBytes int64
}

func New(cfg *config.Config, bucket *limiter.TokenBucket, dlStats *stats.DownloadStats) *Downloader {
	ctx, cancel := context.WithCancel(context.Background())

	connectTimeout := time.Duration(cfg.ConnectTimeoutSec) * time.Second

	transport := &http.Transport{
		MaxIdleConns:        cfg.MaxConcurrent * 2,
		MaxIdleConnsPerHost: cfg.MaxConcurrent * 2,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
		DialContext: (&net.Dialer{
			Timeout:   connectTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ResponseHeaderTimeout: connectTimeout,
	}

	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("stopped after 5 redirects")
			}
			scheme := req.URL.Scheme
			if scheme != "http" && scheme != "https" {
				return fmt.Errorf("redirect to unsupported scheme %q", scheme)
			}
			return nil
		},
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

func (d *Downloader) RunOnce(url string) DownloadResult {

	var lastErr error
	maxRetries := d.cfg.MaxRetries

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if d.ctx.Err() != nil {
			return DownloadResult{Error: d.ctx.Err(), URL: url}
		}

		if attempt > 0 {
			baseBackoff := math.Pow(2, float64(attempt-1)) * float64(time.Second)
			jitter := rand.Float64() * 0.5 * baseBackoff
			backoff := time.Duration(baseBackoff + jitter)
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

	req, err := http.NewRequestWithContext(d.ctx, http.MethodGet, url, nil)
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
		io.Copy(io.Discard, resp.Body)
		d.dlStats.IncrFailed()
		return DownloadResult{
			Error:      fmt.Errorf("unexpected status code: %d", resp.StatusCode),
			StatusCode: resp.StatusCode,
			URL:        url,
		}
	}

	var totalBytes int64
	buf := make([]byte, 4*1024)

	for {
		if d.ctx.Err() != nil {
			if totalBytes > 0 {
				io.Copy(io.Discard, resp.Body)
			}
			d.dlStats.IncrFailed()
			return DownloadResult{
				Bytes:      totalBytes,
				Duration:   time.Since(start),
				StatusCode: resp.StatusCode,
				Error:      d.ctx.Err(),
				URL:        url,
			}
		}

		if d.limiter != nil {
			d.limiter.Wait(d.ctx, len(buf))
			if d.ctx.Err() != nil {
				d.dlStats.IncrFailed()
				return DownloadResult{
					Bytes:      totalBytes,
					Duration:   time.Since(start),
					StatusCode: resp.StatusCode,
					Error:      d.ctx.Err(),
					URL:        url,
				}
			}
		}

		n, err := resp.Body.Read(buf)
		if n > 0 {
			totalBytes += int64(n)
			d.dlStats.AddBytes(int64(n))
		}
		if d.MaxBytes > 0 && totalBytes >= d.MaxBytes {
			io.Copy(io.Discard, resp.Body)
			break
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			if d.ctx.Err() != nil {
				d.dlStats.IncrFailed()
				return DownloadResult{
					Bytes:      totalBytes,
					Duration:   time.Since(start),
					StatusCode: resp.StatusCode,
					Error:      d.ctx.Err(),
					URL:        url,
				}
			}
			d.dlStats.IncrFailed()
			return DownloadResult{
				Bytes:      totalBytes,
				Duration:   time.Since(start),
				StatusCode: resp.StatusCode,
				Error:      fmt.Errorf("read body: %w", err),
				URL:        url,
			}
		}
	}

	d.dlStats.IncrSuccess()
	duration := time.Since(start)

	if duration.Seconds() > 0 {
		rate := float64(totalBytes) / duration.Seconds()
		logger.Info("Download complete: %s | %s in %v | %s",
			url,
			d.dlStats.FormatBytes(totalBytes),
			duration.Round(time.Millisecond),
			d.dlStats.FormatRate(rate),
		)
	}

	return DownloadResult{
		Bytes:      totalBytes,
		Duration:   duration,
		StatusCode: resp.StatusCode,
		URL:        url,
	}
}
