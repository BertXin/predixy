package pool

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// ConnectionState 连接状态
type ConnectionState int32

const (
	StateIdle ConnectionState = iota
	StateBusy
	StateClosed
	StateError
)

func (s ConnectionState) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateBusy:
		return "busy"
	case StateClosed:
		return "closed"
	case StateError:
		return "error"
	default:
		return "unknown"
	}
}

// Connection 连接对象
type Connection struct {
	mu sync.RWMutex
	
	// 连接信息
	id       string
	addr     string
	conn     net.Conn
	state    int32 // atomic ConnectionState
	
	// 时间戳
	createdAt  time.Time
	lastUsed   int64 // atomic unix timestamp
	lastActive int64 // atomic unix timestamp
	
	// 统计信息
	useCount    int64 // atomic
	errorCount  int64 // atomic
	bytesRead   int64 // atomic
	bytesWrite  int64 // atomic
	
	// 配置
	idleTimeout time.Duration
	maxLifetime time.Duration
	
	// 日志
	logger *zap.Logger
}

// NewConnection 创建新连接
func NewConnection(id, addr string, conn net.Conn, idleTimeout, maxLifetime time.Duration, logger *zap.Logger) *Connection {
	now := time.Now()
	c := &Connection{
		id:          id,
		addr:        addr,
		conn:        conn,
		createdAt:   now,
		idleTimeout: idleTimeout,
		maxLifetime: maxLifetime,
		logger:      logger,
	}
	
	atomic.StoreInt32(&c.state, int32(StateIdle))
	atomic.StoreInt64(&c.lastUsed, now.Unix())
	atomic.StoreInt64(&c.lastActive, now.Unix())
	
	return c
}

// ID 获取连接ID
func (c *Connection) ID() string {
	return c.id
}

// Addr 获取连接地址
func (c *Connection) Addr() string {
	return c.addr
}

// GetState 获取连接状态
func (c *Connection) GetState() ConnectionState {
	return ConnectionState(atomic.LoadInt32(&c.state))
}

// SetState 设置连接状态
func (c *Connection) SetState(state ConnectionState) {
	atomic.StoreInt32(&c.state, int32(state))
}

// IsIdle 检查连接是否空闲
func (c *Connection) IsIdle() bool {
	return c.GetState() == StateIdle
}

// IsBusy 检查连接是否繁忙
func (c *Connection) IsBusy() bool {
	return c.GetState() == StateBusy
}

// IsClosed 检查连接是否已关闭
func (c *Connection) IsClosed() bool {
	state := c.GetState()
	return state == StateClosed || state == StateError
}

// IsExpired 检查连接是否已过期
func (c *Connection) IsExpired() bool {
	now := time.Now()
	
	// 检查最大生命周期
	if c.maxLifetime > 0 && now.Sub(c.createdAt) > c.maxLifetime {
		return true
	}
	
	// 检查空闲超时
	if c.idleTimeout > 0 && c.IsIdle() {
		lastUsed := time.Unix(atomic.LoadInt64(&c.lastUsed), 0)
		if now.Sub(lastUsed) > c.idleTimeout {
			return true
		}
	}
	
	return false
}

// Acquire 获取连接使用权
func (c *Connection) Acquire() bool {
	// 尝试从空闲状态切换到繁忙状态
	return atomic.CompareAndSwapInt32(&c.state, int32(StateIdle), int32(StateBusy))
}

// Release 释放连接使用权
func (c *Connection) Release() {
	atomic.StoreInt32(&c.state, int32(StateIdle))
	atomic.StoreInt64(&c.lastUsed, time.Now().Unix())
}

// MarkActive 标记连接活跃
func (c *Connection) MarkActive() {
	atomic.StoreInt64(&c.lastActive, time.Now().Unix())
	atomic.AddInt64(&c.useCount, 1)
}

// MarkError 标记连接错误
func (c *Connection) MarkError() {
	atomic.StoreInt32(&c.state, int32(StateError))
	atomic.AddInt64(&c.errorCount, 1)
}

// Read 读取数据
func (c *Connection) Read(b []byte) (int, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	
	if conn == nil {
		return 0, fmt.Errorf("connection is nil")
	}
	
	n, err := conn.Read(b)
	if err != nil {
		c.MarkError()
		c.logger.Debug("Connection read error", zap.String("id", c.id), zap.Error(err))
	} else {
		c.MarkActive()
		atomic.AddInt64(&c.bytesRead, int64(n))
	}
	
	return n, err
}

// Write 写入数据
func (c *Connection) Write(b []byte) (int, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	
	if conn == nil {
		return 0, fmt.Errorf("connection is nil")
	}
	
	n, err := conn.Write(b)
	if err != nil {
		c.MarkError()
		c.logger.Debug("Connection write error", zap.String("id", c.id), zap.Error(err))
	} else {
		c.MarkActive()
		atomic.AddInt64(&c.bytesWrite, int64(n))
	}
	
	return n, err
}

// SetReadDeadline 设置读取超时
func (c *Connection) SetReadDeadline(t time.Time) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}
	
	return conn.SetReadDeadline(t)
}

// SetWriteDeadline 设置写入超时
func (c *Connection) SetWriteDeadline(t time.Time) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}
	
	return conn.SetWriteDeadline(t)
}

// SetDeadline 设置读写超时
func (c *Connection) SetDeadline(t time.Time) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}
	
	return conn.SetDeadline(t)
}

// Close 关闭连接
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.conn == nil {
		return nil
	}
	
	err := c.conn.Close()
	c.conn = nil
	atomic.StoreInt32(&c.state, int32(StateClosed))
	
	c.logger.Debug("Connection closed", zap.String("id", c.id), zap.String("addr", c.addr))
	
	return err
}

// GetStats 获取连接统计信息
func (c *Connection) GetStats() ConnectionStats {
	return ConnectionStats{
		ID:          c.id,
		Addr:        c.addr,
		State:       c.GetState().String(),
		CreatedAt:   c.createdAt,
		LastUsed:    time.Unix(atomic.LoadInt64(&c.lastUsed), 0),
		LastActive:  time.Unix(atomic.LoadInt64(&c.lastActive), 0),
		UseCount:    atomic.LoadInt64(&c.useCount),
		ErrorCount:  atomic.LoadInt64(&c.errorCount),
		BytesRead:   atomic.LoadInt64(&c.bytesRead),
		BytesWrite:  atomic.LoadInt64(&c.bytesWrite),
		Age:         time.Since(c.createdAt),
		IdleTime:    time.Since(time.Unix(atomic.LoadInt64(&c.lastUsed), 0)),
	}
}

// ConnectionStats 连接统计信息
type ConnectionStats struct {
	ID          string        `json:"id"`
	Addr        string        `json:"addr"`
	State       string        `json:"state"`
	CreatedAt   time.Time     `json:"created_at"`
	LastUsed    time.Time     `json:"last_used"`
	LastActive  time.Time     `json:"last_active"`
	UseCount    int64         `json:"use_count"`
	ErrorCount  int64         `json:"error_count"`
	BytesRead   int64         `json:"bytes_read"`
	BytesWrite  int64         `json:"bytes_write"`
	Age         time.Duration `json:"age"`
	IdleTime    time.Duration `json:"idle_time"`
}

// String 返回连接字符串表示
func (c *Connection) String() string {
	return fmt.Sprintf("Connection{ID:%s, Addr:%s, State:%s, UseCount:%d}",
		c.id, c.addr, c.GetState(), atomic.LoadInt64(&c.useCount))
}