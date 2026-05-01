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
	Timestamp time.Time
	RxBytes   uint64
	TxBytes   uint64
	RxPackets uint64
	TxPackets uint64
}

type Bandwidth struct {
	RxBps float64
	TxBps float64
}

type Stats struct {
	mu sync.RWMutex

	iface string

	lastSample    Sample
	currentSample Sample

	startSample  Sample
	totalRxBytes uint64
	totalTxBytes uint64

	currentBandwidth Bandwidth
	stale            bool

	windowStartSample Sample
	windowTxBytes     uint64
}

func New(iface string) (*Stats, error) {
	s, err := sampleInterface(iface)
	if err != nil {
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
		_, err := sampleInterface(iface.Name)
		if err == nil {
			return iface.Name, nil
		}
	}

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
	s, err := sampleFromProcNetDev(iface)
	if err == nil {
		return s, nil
	}

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

		if _, err := fmt.Sscanf(fields[0], "%d", &s.RxBytes); err != nil {
			return Sample{}, fmt.Errorf("parse rx_bytes for %s: %w", iface, err)
		}
		if _, err := fmt.Sscanf(fields[1], "%d", &s.RxPackets); err != nil {
			return Sample{}, fmt.Errorf("parse rx_packets for %s: %w", iface, err)
		}
		if _, err := fmt.Sscanf(fields[8], "%d", &s.TxBytes); err != nil {
			return Sample{}, fmt.Errorf("parse tx_bytes for %s: %w", iface, err)
		}
		if _, err := fmt.Sscanf(fields[9], "%d", &s.TxPackets); err != nil {
			return Sample{}, fmt.Errorf("parse tx_packets for %s: %w", iface, err)
		}
		return s, nil
	}

	return Sample{}, fmt.Errorf("interface %q not found in /proc/net/dev", iface)
}

func (st *Stats) Update() {
	s, err := sampleInterface(st.iface)
	if err != nil {
		logger.Warn("Failed to sample interface %s: %v", st.iface, err)
		st.mu.Lock()
		st.stale = true
		st.mu.Unlock()
		return
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	st.stale = false
	st.lastSample = st.currentSample
	st.currentSample = s

	dt := s.Timestamp.Sub(st.lastSample.Timestamp).Seconds()
	if dt > 0 {
		if s.RxBytes >= st.lastSample.RxBytes {
			st.currentBandwidth.RxBps = float64(s.RxBytes-st.lastSample.RxBytes) / dt
		} else {
			st.currentBandwidth.RxBps = 0
		}
		if s.TxBytes >= st.lastSample.TxBytes {
			st.currentBandwidth.TxBps = float64(s.TxBytes-st.lastSample.TxBytes) / dt
		} else {
			st.currentBandwidth.TxBps = 0
			logger.Warn("TX counter wrap detected: current=%d, previous=%d", s.TxBytes, st.lastSample.TxBytes)
		}
	}

	if s.RxBytes >= st.startSample.RxBytes {
		st.totalRxBytes = s.RxBytes - st.startSample.RxBytes
	} else {
		logger.Warn("RX counter wrap detected relative to start, resetting baseline")
		st.startSample.RxBytes = s.RxBytes
		st.totalRxBytes = 0
	}

	if s.TxBytes >= st.startSample.TxBytes {
		st.totalTxBytes = s.TxBytes - st.startSample.TxBytes
	} else {
		logger.Warn("TX counter wrap detected relative to start, resetting baseline")
		st.startSample.TxBytes = s.TxBytes
		st.totalTxBytes = 0
	}

	if s.TxBytes >= st.windowStartSample.TxBytes {
		st.windowTxBytes = s.TxBytes - st.windowStartSample.TxBytes
	} else {
		logger.Warn("TX counter wrap detected relative to window start, resetting window baseline")
		st.windowStartSample.TxBytes = s.TxBytes
		st.windowTxBytes = 0
	}
}

func (st *Stats) IsStale() bool {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.stale
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

func (st *Stats) SetWindowTxBytesBase(persistedWindowTxBytes uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()

	currentTx := st.currentSample.TxBytes
	if currentTx >= persistedWindowTxBytes {
		st.windowStartSample.TxBytes = currentTx - persistedWindowTxBytes
	}
	st.windowTxBytes = persistedWindowTxBytes

	logger.Info("Restored window TX base: %d bytes (system tx baseline=%d)", persistedWindowTxBytes, st.windowStartSample.TxBytes)
}

func (st *Stats) SetTotalTxBytesBase(persistedTotalTxBytes uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()

	currentTx := st.currentSample.TxBytes
	if currentTx >= persistedTotalTxBytes {
		st.startSample.TxBytes = currentTx - persistedTotalTxBytes
	}
	st.totalTxBytes = persistedTotalTxBytes

	logger.Info("Restored total TX base: %d bytes (system tx baseline=%d)", persistedTotalTxBytes, st.startSample.TxBytes)
}
