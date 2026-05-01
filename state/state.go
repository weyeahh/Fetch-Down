package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"fetch-down/logger"
)

// PersistedState 是需要持久化到磁盘的运行状态
type PersistedState struct {
	Mode               string    `json:"mode"`
	WindowStart        time.Time `json:"window_start"`
	TotalBytesDown     int64     `json:"total_bytes_down"`
	TotalBytesUp       uint64    `json:"total_bytes_up"`
	WindowTxBytesAtStart uint64  `json:"window_tx_bytes_at_start"`
	SuccessRequests    int64     `json:"success_requests"`
	FailedRequests     int64     `json:"failed_requests"`
	TotalRequests      int64     `json:"total_requests"`
	SavedAt            time.Time `json:"saved_at"`
	InterfaceName      string    `json:"interface_name"`
	WindowDuration     string    `json:"window_duration"`
}

type Manager struct {
	filePath    string
	saveEvery   time.Duration
	stopCh      chan struct{}
}

// NewManager 创建状态管理器
// filePath: 状态文件路径
// saveEvery: 自动保存间隔，0 表示不自动保存（仅在退出时保存）
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

// StartAutoSave 启动自动保存协程，需在另一个 goroutine 中调用
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
						logger.Warn("自动保存状态失败: %v", err)
					} else {
						logger.Debug("状态已自动保存到 %s", m.filePath)
					}
				}
			}
		}
	}()
}

// Stop 停止自动保存
func (m *Manager) Stop() {
	close(m.stopCh)
}

// Save 将状态写入文件（原子写入：先写临时文件再重命名）
func (m *Manager) Save(s *PersistedState) error {
	s.SavedAt = time.Now()

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化状态失败: %w", err)
	}

	tmpPath := m.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("写入临时文件失败: %w", err)
	}

	if err := os.Rename(tmpPath, m.filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("重命名状态文件失败: %w", err)
	}

	return nil
}

// Load 从文件加载状态，文件不存在返回 nil（非错误）
func (m *Manager) Load() (*PersistedState, error) {
	data, err := os.ReadFile(m.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("读取状态文件失败: %w", err)
	}

	var s PersistedState
	if err := json.Unmarshal(data, &s); err != nil {
		logger.Warn("状态文件格式损坏，将忽略: %v", err)
		return nil, nil
	}

	return &s, nil
}

// Remove 删除状态文件（正常退出时可调用）
func (m *Manager) Remove() error {
	err := os.Remove(m.filePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	// 也清理可能残留的临时文件
	os.Remove(m.filePath + ".tmp")
	return nil
}

// FilePath 返回状态文件路径
func (m *Manager) FilePath() string {
	return m.filePath
}
