package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Mode                 string        `yaml:"mode"`
	Ratio                float64       `yaml:"ratio"`
	CumulativeMultiplier float64       `yaml:"cumulative_multiplier"`
	WindowDuration       string        `yaml:"window_duration"`
	DownloadURLs         []string      `yaml:"download_urls"`
	MaxConcurrent        int           `yaml:"max_concurrent"`
	BandwidthLimitMbps   float64       `yaml:"bandwidth_limit_mbps"`
	UplinkInterface      string        `yaml:"uplink_interface"`
	UplinkSampleInterval int           `yaml:"uplink_sample_interval"`
	LogLevel             string        `yaml:"log_level"`
	ConnectTimeoutSec    int           `yaml:"connect_timeout_sec"`
	ReadTimeoutSec       int           `yaml:"read_timeout_sec"`
	MaxRetries           int           `yaml:"max_retries"`
	StatsIntervalSec     int           `yaml:"stats_interval_sec"`

	// 状态持久化
	StateFile         string `yaml:"state_file"`
	StateSaveInterval int    `yaml:"state_save_interval"` // 秒，0 表示仅退出时保存

	windowDurationParsed time.Duration `yaml:"-"`
}

func (c *Config) WindowDurationParsed() time.Duration {
	return c.windowDurationParsed
}

func (c *Config) StateSaveIntervalParsed() time.Duration {
	return time.Duration(c.StateSaveInterval) * time.Second
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	cfg := &Config{
		Mode:                 "ratio",
		Ratio:                2.0,
		CumulativeMultiplier: 1.0,
		WindowDuration:       "24h",
		MaxConcurrent:        4,
		BandwidthLimitMbps:   0,
		UplinkSampleInterval: 1,
		LogLevel:             "info",
		ConnectTimeoutSec:    10,
		ReadTimeoutSec:       30,
		MaxRetries:           3,
		StatsIntervalSec:     10,
		StateFile:            "",
		StateSaveInterval:    60,
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.Mode != "ratio" && c.Mode != "cumulative" {
		return fmt.Errorf("invalid mode %q, must be 'ratio' or 'cumulative'", c.Mode)
	}

	if len(c.DownloadURLs) == 0 {
		return fmt.Errorf("download_urls must not be empty")
	}

	if c.MaxConcurrent < 1 {
		return fmt.Errorf("max_concurrent must be >= 1, got %d", c.MaxConcurrent)
	}

	if c.BandwidthLimitMbps < 0 {
		return fmt.Errorf("bandwidth_limit_mbps must be >= 0, got %f", c.BandwidthLimitMbps)
	}

	if c.UplinkSampleInterval < 1 {
		return fmt.Errorf("uplink_sample_interval must be >= 1, got %d", c.UplinkSampleInterval)
	}

	if c.Mode == "ratio" && c.Ratio <= 0 {
		return fmt.Errorf("ratio must be > 0, got %f", c.Ratio)
	}

	if c.Mode == "cumulative" && c.CumulativeMultiplier <= 0 {
		return fmt.Errorf("cumulative_multiplier must be > 0, got %f", c.CumulativeMultiplier)
	}

	dur, err := time.ParseDuration(c.WindowDuration)
	if err != nil {
		return fmt.Errorf("invalid window_duration %q: %w", c.WindowDuration, err)
	}
	if dur <= 0 {
		return fmt.Errorf("window_duration must be positive")
	}
	c.windowDurationParsed = dur

	if c.ConnectTimeoutSec < 1 {
		c.ConnectTimeoutSec = 10
	}
	if c.ReadTimeoutSec < 1 {
		c.ReadTimeoutSec = 30
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = 3
	}
	if c.StatsIntervalSec < 1 {
		c.StatsIntervalSec = 10
	}

	// 状态持久化：累计模式下如果未设置 state_file，自动使用默认路径
	if c.StateFile == "" && c.Mode == "cumulative" {
		c.StateFile = "fetch-down-state.json"
	}

	return nil
}
