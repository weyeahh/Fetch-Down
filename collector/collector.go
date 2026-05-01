package collector

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"fetch-down/logger"

	"github.com/shirou/gopsutil/v3/net"
)

type Sample struct {
	Timestamp  time.Time
	RxBytes    uint64
	TxBytes    uint64
	RxPackets  uint64
	TxPackets  uint64
}

type Bandwidth struct {
	RxBps float64 // bytes per second (downlink)
	TxBps float64 // bytes per second (uplink)
}

type Stats struct {
	mu sync.RWMutex

	iface string

	// cumulative counters (from system boot)
	lastSample   Sample
	currentSample Sample

	// program-lifetime tracking
	startSample    Sample
	totalRxBytes   uint64 // total downlink since program start
	totalTxBytes   uint64 // total uplink since program start

	// real-time bandwidth
	currentBandwidth Bandwidth

	// window tracking for cumulative mode
	windowStartSample Sample
	windowTxBytes     uint64 // total uplink bytes in current window
}

func New(iface string) (*Stats, error) {
	s, err := sampleInterface(iface)
	if err != nil {
		// fallback: try to auto-detect
		iface, err = detectInterface()
		if err != nil {
			return nil, fmt.Errorf("detect network interface: %w", err)
		}
		s, err = sampleInterface(iface)
		if err != nil {
			return nil, fmt.Errorf("sample interface %q: %w", iface, err)
		}
	}

	logger.Info("Using network interface: %s", iface)

	return &Stats{
		iface:             iface,
		lastSample:        s,
		currentSample:     s,
		startSample:       s,
		windowStartSample: s,
	}, nil
}

func detectInterface() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("list interfaces: %w", err)
	}

	for _, iface := range ifaces {
		if iface.Name == "lo" || iface.Name == "Loopback" {
			continue
		}
		// try to read stats for this interface
		_, err := sampleInterface(iface.Name)
		if err == nil {
			return iface.Name, nil
		}
	}

	// fallback: try reading /proc/net/dev for first non-lo interface
	return detectFromProcNetDev()
}

func detectFromProcNetDev() (string, error) {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		name := strings.TrimSpace(parts[0])
		if name != "lo" {
			return name, nil
		}
	}
	return "", fmt.Errorf("no suitable network interface found")
}

func sampleInterface(iface string) (Sample, error) {
	// Try /proc/net/dev first (Linux)
	s, err := sampleFromProcNetDev(iface)
	if err == nil {
		return s, nil
	}

	// Fallback to gopsutil
	counters, err := net.IOCounters(true)
	if err != nil {
		return Sample{}, fmt.Errorf("gopsutil IOCounters: %w", err)
	}

	for _, c := range counters {
		if c.Name == iface {
			return Sample{
				Timestamp: time.Now(),
				RxBytes:   c.BytesRecv,
				TxBytes:   c.BytesSent,
				RxPackets: c.PacketsRecv,
				TxPackets: c.PacketsSent,
			}, nil
		}
	}

	return Sample{}, fmt.Errorf("interface %q not found", iface)
}

func sampleFromProcNetDev(iface string) (Sample, error) {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return Sample{}, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		name := strings.TrimSpace(parts[0])
		if name != iface {
			continue
		}

		fields := strings.Fields(strings.TrimSpace(parts[1]))
		if len(fields) < 10 {
			return Sample{}, fmt.Errorf("unexpected /proc/net/dev format for %s", iface)
		}

		var s Sample
		s.Timestamp = time.Now()
		fmt.Sscanf(fields[0], "%d", &s.RxBytes)
		fmt.Sscanf(fields[1], "%d", &s.RxPackets)
		fmt.Sscanf(fields[8], "%d", &s.TxBytes)
		fmt.Sscanf(fields[9], "%d", &s.TxPackets)
		return s, nil
	}

	return Sample{}, fmt.Errorf("interface %q not found in /proc/net/dev", iface)
}

// Update samples the interface and updates bandwidth calculations.
func (st *Stats) Update() {
	s, err := sampleInterface(st.iface)
	if err != nil {
		logger.Warn("Failed to sample interface %s: %v", st.iface, err)
		return
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	st.lastSample = st.currentSample
	st.currentSample = s

	// calculate bandwidth (bytes/sec)
	dt := s.Timestamp.Sub(st.lastSample.Timestamp).Seconds()
	if dt > 0 {
		st.currentBandwidth.RxBps = float64(s.RxBytes-st.lastSample.RxBytes) / dt
		st.currentBandwidth.TxBps = float64(s.TxBytes-st.lastSample.TxBytes) / dt
	}

	// update total since program start
	if s.RxBytes >= st.startSample.RxBytes {
		st.totalRxBytes = s.RxBytes - st.startSample.RxBytes
	}
	if s.TxBytes >= st.startSample.TxBytes {
		st.totalTxBytes = s.TxBytes - st.startSample.TxBytes
	}

	// update window tx bytes
	if s.TxBytes >= st.windowStartSample.TxBytes {
		st.windowTxBytes = s.TxBytes - st.windowStartSample.TxBytes
	}
}

func (st *Stats) GetBandwidth() Bandwidth {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.currentBandwidth
}

func (st *Stats) GetTotalRxBytes() uint64 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.totalRxBytes
}

func (st *Stats) GetTotalTxBytes() uint64 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.totalTxBytes
}

func (st *Stats) GetWindowTxBytes() uint64 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.windowTxBytes
}

func (st *Stats) ResetWindow() {
	s, err := sampleInterface(st.iface)
	if err != nil {
		logger.Warn("Failed to sample interface for window reset: %v", err)
		return
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	st.windowStartSample = s
	st.windowTxBytes = 0
}

func (st *Stats) Interface() string {
	return st.iface
}

// SetWindowTxBytesBase 注入持久化的窗口累计上行字节数（重启恢复用）
// 当程序重启时，系统 /proc/net/dev 的计数器不会归零，
// 因此需要记录重启时刻的系统基准值，加上之前窗口内已累计的字节数
func (st *Stats) SetWindowTxBytesBase(persistedWindowTxBytes uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()

	// 记录当前系统 tx 作为新的窗口起点，但将之前累计的值保存下来
	// 这样 windowTxBytes = (当前系统tx - windowStartSample.tx) + persistedWindowTxBytes
	// 通过调整 windowStartSample 来实现：
	// 令 windowStartSample.TxBytes = 当前系统tx - persistedWindowTxBytes
	// 这样 windowTxBytes = 当前系统tx - (当前系统tx - persistedWindowTxBytes) = persistedWindowTxBytes
	currentTx := st.currentSample.TxBytes
	if currentTx >= persistedWindowTxBytes {
		st.windowStartSample.TxBytes = currentTx - persistedWindowTxBytes
	}
	st.windowTxBytes = persistedWindowTxBytes

	logger.Info("恢复窗口累计上行: %d bytes (系统tx基准=%d)", persistedWindowTxBytes, st.windowStartSample.TxBytes)
}

// SetTotalTxBytesBase 注入持久化的程序生命周期累计上行字节数
func (st *Stats) SetTotalTxBytesBase(persistedTotalTxBytes uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()

	currentTx := st.currentSample.TxBytes
	if currentTx >= persistedTotalTxBytes {
		st.startSample.TxBytes = currentTx - persistedTotalTxBytes
	}
	st.totalTxBytes = persistedTotalTxBytes

	logger.Info("恢复累计上行: %d bytes (系统tx基准=%d)", persistedTotalTxBytes, st.startSample.TxBytes)
}
