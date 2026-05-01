package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"fetch-down/logger"
)

type PersistedState struct {
	Mode                 string    `json:"mode"`
	WindowStart          time.Time `json:"window_start"`
	TotalBytesDown       int64     `json:"total_bytes_down"`
	TotalBytesUp         uint64    `json:"total_bytes_up"`
	WindowTxBytesAtStart uint64    `json:"window_tx_bytes_at_start"`
	SuccessRequests      int64     `json:"success_requests"`
	FailedRequests       int64     `json:"failed_requests"`
	TotalRequests        int64     `json:"total_requests"`
	SavedAt              time.Time `json:"saved_at"`
	InterfaceName        string    `json:"interface_name"`
	WindowDuration       string    `json:"window_duration"`
}

type Manager struct {
	filePath  string
	saveEvery time.Duration
	stopCh    chan struct{}
}

func NewManager(filePath string, saveEvery time.Duration) *Manager {
	dir := filepath.Dir(filePath)
	if dir != "" && dir != "." {
		os.MkdirAll(dir, 0755)
	}

	return &Manager{
		filePath:  filePath,
		saveEvery: saveEvery,
		stopCh:    make(chan struct{}),
	}
}

func (m *Manager) StartAutoSave(saveFunc func() *PersistedState) {
	if m.saveEvery <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(m.saveEvery)
		defer ticker.Stop()

		for {
			select {
			case <-m.stopCh:
				return
			case <-ticker.C:
				if s := saveFunc(); s != nil {
					if err := m.Save(s); err != nil {
						logger.Warn("Auto-save state failed: %v", err)
					} else {
						logger.Debug("State auto-saved to %s", m.filePath)
					}
				}
			}
		}
	}()
}

func (m *Manager) Stop() {
	close(m.stopCh)
}

func (m *Manager) Save(s *PersistedState) error {
	s.SavedAt = time.Now()

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("serialize state: %w", err)
	}

	tmpPath := m.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}

	if err := os.Rename(tmpPath, m.filePath); err != nil {
		if runtime.GOOS == "windows" {
			os.Remove(m.filePath)
			if err2 := os.Rename(tmpPath, m.filePath); err2 != nil {
				os.Remove(tmpPath)
				return fmt.Errorf("rename state file (windows fallback): %w", err2)
			}
		} else {
			os.Remove(tmpPath)
			return fmt.Errorf("rename state file: %w", err)
		}
	}

	return nil
}

func (m *Manager) Load() (*PersistedState, error) {
	data, err := os.ReadFile(m.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read state file: %w", err)
	}

	var s PersistedState
	if err := json.Unmarshal(data, &s); err != nil {
		logger.Warn("State file corrupted, ignoring: %v", err)
		return nil, nil
	}

	return &s, nil
}

func (m *Manager) Remove() error {
	err := os.Remove(m.filePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	os.Remove(m.filePath + ".tmp")
	return nil
}

func (m *Manager) FilePath() string {
	return m.filePath
}
