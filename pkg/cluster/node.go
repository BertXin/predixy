package cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// NodeRole 节点角色
type NodeRole string

const (
	RoleMaster NodeRole = "master"
	RoleSlave  NodeRole = "slave"
)

// NodeState 节点状态
type NodeState int32

const (
	StateUnknown NodeState = iota
	StateConnecting
	StateConnected
	StateDisconnected
	StateFailed
)

func (s NodeState) String() string {
	switch s {
	case StateUnknown:
		return "unknown"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateDisconnected:
		return "disconnected"
	case StateFailed:
		return "failed"
	default:
		return "invalid"
	}
}

// Node Redis集群节点
type Node struct {
	mu sync.RWMutex
	
	// 节点基本信息
	ID       string    `json:"id"`
	Addr     string    `json:"addr"`
	Host     string    `json:"host"`
	Port     int       `json:"port"`
	Role     NodeRole  `json:"role"`
	MasterID string    `json:"master_id,omitempty"`
	
	// 槽位信息
	Slots    []SlotRange `json:"slots"`
	SlotSet  map[int]bool `json:"-"` // 快速查找槽位
	
	// 连接状态
	state       int32 // atomic NodeState
	lastPing    int64 // atomic unix timestamp
	lastPong    int64 // atomic unix timestamp
	failCount   int32 // atomic
	lastFailure int64 // atomic unix timestamp
	
	// Redis连接
	client *redis.Client
	
	// 统计信息
	stats NodeStats
}

// SlotRange 槽位范围
type SlotRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// NodeStats 节点统计信息
type NodeStats struct {
	Connections   int64 `json:"connections"`
	Commands      int64 `json:"commands"`
	Errors        int64 `json:"errors"`
	LatencySum    int64 `json:"latency_sum"`
	LatencyCount  int64 `json:"latency_count"`
	LastCommand   int64 `json:"last_command"`
}

// NewNode 创建新节点
func NewNode(id, addr string, role NodeRole) *Node {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		// 如果解析失败，假设是IP地址
		host = addr
		portStr = "6379"
	}
	
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 6379
	}
	
	node := &Node{
		ID:      id,
		Addr:    addr,
		Host:    host,
		Port:    port,
		Role:    role,
		SlotSet: make(map[int]bool),
	}
	
	atomic.StoreInt32(&node.state, int32(StateUnknown))
	atomic.StoreInt64(&node.lastPing, time.Now().Unix())
	
	return node
}

// GetState 获取节点状态
func (n *Node) GetState() NodeState {
	return NodeState(atomic.LoadInt32(&n.state))
}

// SetState 设置节点状态
func (n *Node) SetState(state NodeState) {
	atomic.StoreInt32(&n.state, int32(state))
}

// IsConnected 检查节点是否已连接
func (n *Node) IsConnected() bool {
	return n.GetState() == StateConnected
}

// IsMaster 检查是否为主节点
func (n *Node) IsMaster() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Role == RoleMaster
}

// IsSlave 检查是否为从节点
func (n *Node) IsSlave() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Role == RoleSlave
}

// HasSlot 检查节点是否包含指定槽位
func (n *Node) HasSlot(slot int) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.SlotSet[slot]
}

// AddSlots 添加槽位范围
func (n *Node) AddSlots(slots []SlotRange) {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	n.Slots = append(n.Slots, slots...)
	
	// 更新槽位集合
	for _, slotRange := range slots {
		for slot := slotRange.Start; slot <= slotRange.End; slot++ {
			n.SlotSet[slot] = true
		}
	}
}

// ClearSlots 清空槽位
func (n *Node) ClearSlots() {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	n.Slots = nil
	n.SlotSet = make(map[int]bool)
}

// GetSlotCount 获取槽位数量
func (n *Node) GetSlotCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.SlotSet)
}

// Connect 连接到Redis节点
func (n *Node) Connect(ctx context.Context, password, username string, timeout time.Duration) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.client != nil {
		n.client.Close()
	}
	
	// 创建Redis客户端
	opts := &redis.Options{
		Addr:         n.Addr,
		Password:     password,
		Username:     username,
		DB:           0,
		DialTimeout:  timeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
		PoolSize:     10,
		MinIdleConns: 2,
		MaxRetries:   3,
	}
	
	n.client = redis.NewClient(opts)
	
	// 测试连接
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	
	n.SetState(StateConnecting)
	
	if err := n.client.Ping(ctx).Err(); err != nil {
		n.SetState(StateFailed)
		atomic.AddInt32(&n.failCount, 1)
		atomic.StoreInt64(&n.lastFailure, time.Now().Unix())
		return fmt.Errorf("failed to ping node %s: %w", n.Addr, err)
	}
	
	n.SetState(StateConnected)
	atomic.StoreInt64(&n.lastPong, time.Now().Unix())
	atomic.StoreInt32(&n.failCount, 0)
	
	return nil
}

// Disconnect 断开连接
func (n *Node) Disconnect() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.client != nil {
		err := n.client.Close()
		n.client = nil
		n.SetState(StateDisconnected)
		return err
	}
	
	return nil
}

// Ping 发送PING命令
func (n *Node) Ping(ctx context.Context) error {
	n.mu.RLock()
	client := n.client
	n.mu.RUnlock()
	
	if client == nil {
		return fmt.Errorf("node %s not connected", n.Addr)
	}
	
	atomic.StoreInt64(&n.lastPing, time.Now().Unix())
	
	start := time.Now()
	err := client.Ping(ctx).Err()
	latency := time.Since(start).Nanoseconds()
	
	if err != nil {
		atomic.AddInt32(&n.failCount, 1)
		atomic.StoreInt64(&n.lastFailure, time.Now().Unix())
		atomic.AddInt64(&n.stats.Errors, 1)
		return err
	}
	
	atomic.StoreInt64(&n.lastPong, time.Now().Unix())
	atomic.StoreInt32(&n.failCount, 0)
	atomic.AddInt64(&n.stats.LatencySum, latency)
	atomic.AddInt64(&n.stats.LatencyCount, 1)
	
	return nil
}

// Execute 执行Redis命令
func (n *Node) Execute(ctx context.Context, cmd *redis.Cmd) error {
	n.mu.RLock()
	client := n.client
	n.mu.RUnlock()
	
	if client == nil {
		return fmt.Errorf("node %s not connected", n.Addr)
	}
	
	start := time.Now()
	err := client.Process(ctx, cmd)
	latency := time.Since(start).Nanoseconds()
	
	atomic.AddInt64(&n.stats.Commands, 1)
	atomic.StoreInt64(&n.stats.LastCommand, time.Now().Unix())
	atomic.AddInt64(&n.stats.LatencySum, latency)
	atomic.AddInt64(&n.stats.LatencyCount, 1)
	
	if err != nil {
		atomic.AddInt64(&n.stats.Errors, 1)
		
		// 检查是否为重定向错误
		if isRedirectError(err) {
			return err // 重定向错误不算作节点错误
		}
		
		atomic.AddInt32(&n.failCount, 1)
		atomic.StoreInt64(&n.lastFailure, time.Now().Unix())
	}
	
	return err
}

// GetClient 获取Redis客户端
func (n *Node) GetClient() *redis.Client {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.client
}

// GetStats 获取节点统计信息
func (n *Node) GetStats() NodeStats {
	return NodeStats{
		Connections:  atomic.LoadInt64(&n.stats.Connections),
		Commands:     atomic.LoadInt64(&n.stats.Commands),
		Errors:       atomic.LoadInt64(&n.stats.Errors),
		LatencySum:   atomic.LoadInt64(&n.stats.LatencySum),
		LatencyCount: atomic.LoadInt64(&n.stats.LatencyCount),
		LastCommand:  atomic.LoadInt64(&n.stats.LastCommand),
	}
}

// GetAverageLatency 获取平均延迟（纳秒）
func (n *Node) GetAverageLatency() int64 {
	count := atomic.LoadInt64(&n.stats.LatencyCount)
	if count == 0 {
		return 0
	}
	sum := atomic.LoadInt64(&n.stats.LatencySum)
	return sum / count
}

// GetFailCount 获取失败次数
func (n *Node) GetFailCount() int32 {
	return atomic.LoadInt32(&n.failCount)
}

// GetLastPing 获取最后PING时间
func (n *Node) GetLastPing() time.Time {
	timestamp := atomic.LoadInt64(&n.lastPing)
	return time.Unix(timestamp, 0)
}

// GetLastPong 获取最后PONG时间
func (n *Node) GetLastPong() time.Time {
	timestamp := atomic.LoadInt64(&n.lastPong)
	return time.Unix(timestamp, 0)
}

// GetLastFailure 获取最后失败时间
func (n *Node) GetLastFailure() time.Time {
	timestamp := atomic.LoadInt64(&n.lastFailure)
	if timestamp == 0 {
		return time.Time{}
	}
	return time.Unix(timestamp, 0)
}

// String 返回节点字符串表示
func (n *Node) String() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	return fmt.Sprintf("Node{ID:%s, Addr:%s, Role:%s, State:%s, Slots:%d}",
		n.ID, n.Addr, n.Role, n.GetState(), len(n.SlotSet))
}

// isRedirectError 检查是否为重定向错误
func isRedirectError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	return strings.HasPrefix(errStr, "MOVED") || strings.HasPrefix(errStr, "ASK")
}