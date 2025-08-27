package cluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// 辅助函数
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Manager Redis集群管理器
type Manager struct {
	mu sync.RWMutex

	// 配置
	entryPoints []string
	password    string
	username    string
	timeout     time.Duration

	// 节点管理
	nodes   map[string]*Node // nodeID -> Node
	masters map[string]*Node // nodeID -> Master Node
	slaves  map[string]*Node // nodeID -> Slave Node
	slotMap [16384]*Node     // slot -> Node (master)

	// 集群状态
	clusterOK   bool
	lastUpdate  time.Time
	updateCount int64

	// 日志
	logger *zap.Logger

	// 停止信号
	stopCh chan struct{}
	done   chan struct{}
}

// NewManager 创建集群管理器
func NewManager(entryPoints []string, password, username string, timeout time.Duration, logger *zap.Logger) *Manager {
	return &Manager{
		entryPoints: entryPoints,
		password:    password,
		username:    username,
		timeout:     timeout,
		nodes:       make(map[string]*Node),
		masters:     make(map[string]*Node),
		slaves:      make(map[string]*Node),
		logger:      logger,
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// Start 启动集群管理器
func (m *Manager) Start(ctx context.Context) error {
	m.logger.Info("Starting cluster manager", zap.Strings("entry_points", m.entryPoints))

	// 初始化集群拓扑
	if err := m.RefreshTopology(ctx); err != nil {
		return fmt.Errorf("failed to initialize cluster topology: %w", err)
	}

	// 启动后台更新任务
	go m.backgroundUpdate(ctx)

	return nil
}

// Stop 停止集群管理器
func (m *Manager) Stop() error {
	m.logger.Info("Stopping cluster manager")

	close(m.stopCh)

	// 等待后台任务结束
	select {
	case <-m.done:
	case <-time.After(5 * time.Second):
		m.logger.Warn("Timeout waiting for cluster manager to stop")
	}

	// 断开所有节点连接
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range m.nodes {
		if err := node.Disconnect(); err != nil {
			m.logger.Warn("Failed to disconnect node", zap.String("node", node.Addr), zap.Error(err))
		}
	}

	return nil
}

// RefreshTopology 刷新集群拓扑
func (m *Manager) RefreshTopology(ctx context.Context) error {
	m.logger.Debug("Refreshing cluster topology")

	// 尝试从任一入口点获取集群信息
	var clusterInfo string
	var err error

	for _, entryPoint := range m.entryPoints {
		clusterInfo, err = m.getClusterInfo(ctx, entryPoint)
		if err == nil {
			break
		}
		m.logger.Warn("Failed to get cluster info from entry point",
			zap.String("entry_point", entryPoint), zap.Error(err))
	}

	if err != nil {
		return fmt.Errorf("failed to get cluster info from all entry points: %w", err)
	}

	// 解析集群信息
	newNodes, err := m.parseClusterInfo(clusterInfo)
	if err != nil {
		return fmt.Errorf("failed to parse cluster info: %w", err)
	}

	// 更新节点信息
	if err := m.updateNodes(ctx, newNodes); err != nil {
		return fmt.Errorf("failed to update nodes: %w", err)
	}

	// 更新槽位映射
	m.updateSlotMap()

	m.mu.Lock()
	m.lastUpdate = time.Now()
	m.updateCount++
	m.clusterOK = m.validateCluster()
	m.mu.Unlock()

	m.logger.Info("Cluster topology refreshed",
		zap.Int("masters", len(m.masters)),
		zap.Int("slaves", len(m.slaves)),
		zap.Bool("cluster_ok", m.clusterOK))

	return nil
}

// GetNodeBySlot 根据槽位获取节点
func (m *Manager) GetNodeBySlot(slot int) *Node {
	if slot < 0 || slot >= 16384 {
		m.logger.Warn("Invalid slot number", zap.Int("slot", slot))
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	node := m.slotMap[slot]
	if node == nil {
		m.logger.Error("No node found for slot",
			zap.Int("slot", slot),
			zap.Int("total_masters", len(m.masters)),
			zap.Bool("cluster_ok", m.clusterOK))

		// 调试信息：检查附近的slot映射
		for i := max(0, slot-5); i <= min(16383, slot+5); i++ {
			if m.slotMap[i] != nil {
				m.logger.Debug("Nearby slot mapping",
					zap.Int("slot", i),
					zap.String("node", m.slotMap[i].Addr))
			}
		}
	} else {
		m.logger.Debug("Found node for slot",
			zap.Int("slot", slot),
			zap.String("node", node.Addr))
	}

	return node
}

// GetMasterNodes 获取所有主节点
func (m *Manager) GetMasterNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*Node, 0, len(m.masters))
	for _, node := range m.masters {
		nodes = append(nodes, node)
	}

	return nodes
}

// GetSlaveNodes 获取所有从节点
func (m *Manager) GetSlaveNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*Node, 0, len(m.slaves))
	for _, node := range m.slaves {
		nodes = append(nodes, node)
	}

	return nodes
}

// GetAllNodes 获取所有节点
func (m *Manager) GetAllNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// GetNodeByID 根据ID获取节点
func (m *Manager) GetNodeByID(nodeID string) *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.nodes[nodeID]
}

// IsClusterOK 检查集群状态是否正常
func (m *Manager) IsClusterOK() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.clusterOK
}

// GetClusterStats 获取集群统计信息
func (m *Manager) GetClusterStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]interface{}{
		"masters":      len(m.masters),
		"slaves":       len(m.slaves),
		"total_nodes":  len(m.nodes),
		"cluster_ok":   m.clusterOK,
		"last_update":  m.lastUpdate,
		"update_count": m.updateCount,
	}

	// 统计连接状态
	connected := 0
	for _, node := range m.nodes {
		if node.IsConnected() {
			connected++
		}
	}
	stats["connected_nodes"] = connected

	return stats
}

// getClusterInfo 从指定节点获取集群信息
func (m *Manager) getClusterInfo(ctx context.Context, addr string) (string, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     m.password,
		Username:     m.username,
		DB:           0,
		DialTimeout:  m.timeout,
		ReadTimeout:  m.timeout,
		WriteTimeout: m.timeout,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	return client.ClusterNodes(ctx).Result()
}

// parseClusterInfo 解析CLUSTER NODES命令的输出
func (m *Manager) parseClusterInfo(clusterInfo string) (map[string]*Node, error) {
	nodes := make(map[string]*Node)
	lines := strings.Split(strings.TrimSpace(clusterInfo), "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}

		node, err := m.parseNodeLine(line)
		if err != nil {
			m.logger.Warn("Failed to parse node line", zap.String("line", line), zap.Error(err))
			continue
		}

		nodes[node.ID] = node
	}

	return nodes, nil
}

// parseNodeLine 解析单个节点信息行
func (m *Manager) parseNodeLine(line string) (*Node, error) {
	fields := strings.Fields(line)
	if len(fields) < 8 {
		return nil, fmt.Errorf("invalid node line: %s", line)
	}

	nodeID := fields[0]
	addr := fields[1]
	flags := fields[2]
	masterID := fields[3]

	// 解析地址
	if strings.Contains(addr, "@") {
		addr = strings.Split(addr, "@")[0]
	}

	// 确定节点角色
	var role NodeRole
	if strings.Contains(flags, "master") {
		role = RoleMaster
	} else if strings.Contains(flags, "slave") {
		role = RoleSlave
	} else {
		return nil, fmt.Errorf("unknown node role in flags: %s", flags)
	}

	node := NewNode(nodeID, addr, role)

	// 设置主节点ID（对于从节点）
	if role == RoleSlave && masterID != "-" {
		node.mu.Lock()
		node.MasterID = masterID
		node.mu.Unlock()
	}

	// 解析槽位信息（仅对主节点）
	if role == RoleMaster && len(fields) > 8 {
		// 先清空槽位，确保没有旧数据
		node.ClearSlots()
		slots := m.parseSlots(fields[8:])
		node.AddSlots(slots)
	}

	return node, nil
}

// parseSlots 解析槽位信息
func (m *Manager) parseSlots(slotFields []string) []SlotRange {
	var slots []SlotRange

	for _, field := range slotFields {
		// 跳过导入/迁移槽位标记
		if strings.HasPrefix(field, "[") {
			continue
		}

		if strings.Contains(field, "-") {
			// 槽位范围 "0-5460"
			parts := strings.Split(field, "-")
			if len(parts) == 2 {
				start, err1 := strconv.Atoi(parts[0])
				end, err2 := strconv.Atoi(parts[1])
				if err1 == nil && err2 == nil && start <= end {
					slots = append(slots, SlotRange{Start: start, End: end})
				}
			}
		} else {
			// 单个槽位
			slot, err := strconv.Atoi(field)
			if err == nil {
				slots = append(slots, SlotRange{Start: slot, End: slot})
			}
		}
	}

	return slots
}

// updateNodes 更新节点信息
func (m *Manager) updateNodes(ctx context.Context, newNodes map[string]*Node) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 保存旧节点的连接
	oldConnections := make(map[string]*redis.Client)
	for id, node := range m.nodes {
		if client := node.GetClient(); client != nil {
			oldConnections[id] = client
		}
	}

	// 更新节点映射
	m.nodes = newNodes
	m.masters = make(map[string]*Node)
	m.slaves = make(map[string]*Node)

	for id, node := range newNodes {
		if node.IsMaster() {
			m.masters[id] = node
		} else {
			m.slaves[id] = node
		}

		// 尝试重用现有连接
		if client, exists := oldConnections[id]; exists {
			node.mu.Lock()
			node.client = client
			node.SetState(StateConnected)
			node.mu.Unlock()
			delete(oldConnections, id)
		} else {
			// 建立新连接
			if err := node.Connect(ctx, m.password, m.username, m.timeout); err != nil {
				m.logger.Warn("Failed to connect to node",
					zap.String("node", node.Addr), zap.Error(err))
			}
		}
	}

	// 关闭不再使用的连接
	for _, client := range oldConnections {
		client.Close()
	}

	return nil
}

// updateSlotMap 更新槽位映射
func (m *Manager) updateSlotMap() {
	// 创建新的槽位映射，避免在更新过程中出现空槽位
	newSlotMap := [16384]*Node{}

	// 重建槽位映射
	totalSlots := 0
	for _, node := range m.masters {
		// 直接使用节点的slot范围，避免遍历所有16384个slot
		node.mu.RLock()
		nodeSlots := 0
		for _, slotRange := range node.Slots {
			for slot := slotRange.Start; slot <= slotRange.End; slot++ {
				if slot >= 0 && slot < 16384 {
					newSlotMap[slot] = node
					nodeSlots++
					totalSlots++
				}
			}
		}
		node.mu.RUnlock()
		m.logger.Debug("Updated slots for node",
			zap.String("node", node.Addr),
			zap.Int("slots", nodeSlots))
	}

	// 原子性地替换槽位映射
	m.slotMap = newSlotMap

	m.logger.Debug("Slot map updated",
		zap.Int("total_slots", totalSlots),
		zap.Int("masters", len(m.masters)))
}

// validateCluster 验证集群状态
func (m *Manager) validateCluster() bool {
	// 检查是否有足够的主节点
	if len(m.masters) == 0 {
		return false
	}

	// 检查槽位覆盖
	coveredSlots := 0
	for _, node := range m.slotMap {
		if node != nil {
			coveredSlots++
		}
	}

	// 所有槽位都应该被覆盖
	if coveredSlots != 16384 {
		m.logger.Warn("Incomplete slot coverage", zap.Int("covered_slots", coveredSlots))
		return false
	}

	// 检查主节点连接状态
	connectedMasters := 0
	for _, node := range m.masters {
		if node.IsConnected() {
			connectedMasters++
		}
	}

	// 至少一半的主节点应该连接正常
	if connectedMasters < len(m.masters)/2+1 {
		m.logger.Warn("Insufficient connected masters",
			zap.Int("connected", connectedMasters),
			zap.Int("total", len(m.masters)))
		return false
	}

	return true
}

// backgroundUpdate 后台更新任务
func (m *Manager) backgroundUpdate(ctx context.Context) {
	defer close(m.done)

	ticker := time.NewTicker(30 * time.Second) // 每30秒更新一次
	defer ticker.Stop()

	pingTicker := time.NewTicker(5 * time.Second) // 每5秒ping一次
	defer pingTicker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.RefreshTopology(ctx); err != nil {
				m.logger.Error("Failed to refresh topology", zap.Error(err))
			}
		case <-pingTicker.C:
			m.pingNodes(ctx)
		}
	}
}

// pingNodes 对所有节点执行ping
func (m *Manager) pingNodes(ctx context.Context) {
	nodes := m.GetAllNodes()

	for _, node := range nodes {
		go func(n *Node) {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			if err := n.Ping(pingCtx); err != nil {
				m.logger.Debug("Node ping failed",
					zap.String("node", n.Addr), zap.Error(err))
			}
		}(node)
	}
}
