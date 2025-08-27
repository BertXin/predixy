package pool

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// PoolConfig 连接池配置
type PoolConfig struct {
	MinSize        int           `json:"min_size"`         // 最小连接数
	MaxSize        int           `json:"max_size"`         // 最大连接数
	IdleTimeout    time.Duration `json:"idle_timeout"`     // 空闲超时
	MaxLifetime    time.Duration `json:"max_lifetime"`     // 最大生命周期
	ConnectTimeout time.Duration `json:"connect_timeout"`  // 连接超时
	ReadTimeout    time.Duration `json:"read_timeout"`     // 读取超时
	WriteTimeout   time.Duration `json:"write_timeout"`    // 写入超时
	CleanupInterval time.Duration `json:"cleanup_interval"` // 清理间隔
}

// DefaultPoolConfig 默认连接池配置
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MinSize:         2,
		MaxSize:         20,
		IdleTimeout:     5 * time.Minute,
		MaxLifetime:     30 * time.Minute,
		ConnectTimeout:  5 * time.Second,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		CleanupInterval: 1 * time.Minute,
	}
}

// Pool 连接池
type Pool struct {
	mu sync.RWMutex
	
	// 配置
	addr   string
	config *PoolConfig
	
	// 连接管理
	connections map[string]*Connection // connID -> Connection
	idleConns   []*Connection          // 空闲连接队列
	busyConns   map[string]*Connection // 繁忙连接映射
	
	// 统计信息
	totalCreated int64 // atomic 总创建数
	totalClosed  int64 // atomic 总关闭数
	totalErrors  int64 // atomic 总错误数
	hitCount     int64 // atomic 命中次数
	missCount    int64 // atomic 未命中次数
	
	// 状态
	closed bool
	
	// 清理任务
	stopCh chan struct{}
	done   chan struct{}
	
	// 日志
	logger *zap.Logger
}

// NewPool 创建连接池
func NewPool(addr string, config *PoolConfig, logger *zap.Logger) *Pool {
	if config == nil {
		config = DefaultPoolConfig()
	}
	
	p := &Pool{
		addr:        addr,
		config:      config,
		connections: make(map[string]*Connection),
		idleConns:   make([]*Connection, 0, config.MaxSize),
		busyConns:   make(map[string]*Connection),
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
		logger:      logger,
	}
	
	// 启动清理任务
	go p.cleanupTask()
	
	return p
}

// Get 获取连接
func (p *Pool) Get(ctx context.Context) (*Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return nil, fmt.Errorf("pool is closed")
	}
	
	// 尝试从空闲连接中获取
	for len(p.idleConns) > 0 {
		conn := p.idleConns[0]
		p.idleConns = p.idleConns[1:]
		
		// 检查连接是否有效
		if conn.IsClosed() || conn.IsExpired() {
			p.removeConnection(conn)
			continue
		}
		
		// 尝试获取连接
		if conn.Acquire() {
			p.busyConns[conn.ID()] = conn
			atomic.AddInt64(&p.hitCount, 1)
			return conn, nil
		}
	}
	
	// 如果没有空闲连接，尝试创建新连接
	if len(p.connections) < p.config.MaxSize {
		conn, err := p.createConnection(ctx)
		if err != nil {
			atomic.AddInt64(&p.missCount, 1)
			return nil, err
		}
		
		if conn.Acquire() {
			p.busyConns[conn.ID()] = conn
			atomic.AddInt64(&p.missCount, 1)
			return conn, nil
		}
	}
	
	atomic.AddInt64(&p.missCount, 1)
	return nil, fmt.Errorf("no available connections")
}

// Put 归还连接
func (p *Pool) Put(conn *Connection) {
	if conn == nil {
		return
	}
	
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		conn.Close()
		return
	}
	
	// 从繁忙连接中移除
	delete(p.busyConns, conn.ID())
	
	// 检查连接状态
	if conn.IsClosed() || conn.IsExpired() {
		p.removeConnection(conn)
		return
	}
	
	// 释放连接并加入空闲队列
	conn.Release()
	p.idleConns = append(p.idleConns, conn)
}

// Close 关闭连接池
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return nil
	}
	
	p.closed = true
	close(p.stopCh)
	
	// 等待清理任务结束
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		p.logger.Warn("Timeout waiting for pool cleanup task to stop")
	}
	
	// 关闭所有连接
	for _, conn := range p.connections {
		conn.Close()
	}
	
	p.connections = make(map[string]*Connection)
	p.idleConns = nil
	p.busyConns = make(map[string]*Connection)
	
	p.logger.Info("Connection pool closed", zap.String("addr", p.addr))
	
	return nil
}

// Stats 获取连接池统计信息
func (p *Pool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	return PoolStats{
		Addr:         p.addr,
		TotalConns:   len(p.connections),
		IdleConns:    len(p.idleConns),
		BusyConns:    len(p.busyConns),
		MaxSize:      p.config.MaxSize,
		MinSize:      p.config.MinSize,
		TotalCreated: atomic.LoadInt64(&p.totalCreated),
		TotalClosed:  atomic.LoadInt64(&p.totalClosed),
		TotalErrors:  atomic.LoadInt64(&p.totalErrors),
		HitCount:     atomic.LoadInt64(&p.hitCount),
		MissCount:    atomic.LoadInt64(&p.missCount),
		HitRate:      p.calculateHitRate(),
	}
}

// GetConnections 获取所有连接信息
func (p *Pool) GetConnections() []ConnectionStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	stats := make([]ConnectionStats, 0, len(p.connections))
	for _, conn := range p.connections {
		stats = append(stats, conn.GetStats())
	}
	
	return stats
}

// createConnection 创建新连接
func (p *Pool) createConnection(ctx context.Context) (*Connection, error) {
	// 设置连接超时
	ctx, cancel := context.WithTimeout(ctx, p.config.ConnectTimeout)
	defer cancel()
	
	// 建立TCP连接
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		atomic.AddInt64(&p.totalErrors, 1)
		return nil, fmt.Errorf("failed to connect to %s: %w", p.addr, err)
	}
	
	// 创建连接对象
	connID := fmt.Sprintf("%s-%d", p.addr, atomic.AddInt64(&p.totalCreated, 1))
	poolConn := NewConnection(
		connID,
		p.addr,
		conn,
		p.config.IdleTimeout,
		p.config.MaxLifetime,
		p.logger,
	)
	
	// 添加到连接映射
	p.connections[connID] = poolConn
	
	p.logger.Debug("Created new connection",
		zap.String("id", connID),
		zap.String("addr", p.addr))
	
	return poolConn, nil
}

// removeConnection 移除连接
func (p *Pool) removeConnection(conn *Connection) {
	if conn == nil {
		return
	}
	
	delete(p.connections, conn.ID())
	delete(p.busyConns, conn.ID())
	
	// 从空闲连接队列中移除
	for i, idleConn := range p.idleConns {
		if idleConn.ID() == conn.ID() {
			p.idleConns = append(p.idleConns[:i], p.idleConns[i+1:]...)
			break
		}
	}
	
	conn.Close()
	atomic.AddInt64(&p.totalClosed, 1)
	
	p.logger.Debug("Removed connection",
		zap.String("id", conn.ID()),
		zap.String("addr", p.addr))
}

// calculateHitRate 计算命中率
func (p *Pool) calculateHitRate() float64 {
	hits := atomic.LoadInt64(&p.hitCount)
	misses := atomic.LoadInt64(&p.missCount)
	total := hits + misses
	
	if total == 0 {
		return 0.0
	}
	
	return float64(hits) / float64(total)
}

// cleanupTask 清理任务
func (p *Pool) cleanupTask() {
	defer close(p.done)
	
	ticker := time.NewTicker(p.config.CleanupInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.cleanup()
		}
	}
}

// cleanup 清理过期连接
func (p *Pool) cleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return
	}
	
	// 清理过期的空闲连接
	validIdle := make([]*Connection, 0, len(p.idleConns))
	for _, conn := range p.idleConns {
		if conn.IsClosed() || conn.IsExpired() {
			p.removeConnection(conn)
		} else {
			validIdle = append(validIdle, conn)
		}
	}
	p.idleConns = validIdle
	
	// 确保最小连接数
	if len(p.connections) < p.config.MinSize {
		needed := p.config.MinSize - len(p.connections)
		for i := 0; i < needed; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), p.config.ConnectTimeout)
			conn, err := p.createConnection(ctx)
			cancel()
			
			if err != nil {
				p.logger.Warn("Failed to create minimum connection",
					zap.String("addr", p.addr), zap.Error(err))
				break
			}
			
			p.idleConns = append(p.idleConns, conn)
		}
	}
	
	p.logger.Debug("Pool cleanup completed",
		zap.String("addr", p.addr),
		zap.Int("total", len(p.connections)),
		zap.Int("idle", len(p.idleConns)),
		zap.Int("busy", len(p.busyConns)))
}

// PoolStats 连接池统计信息
type PoolStats struct {
	Addr         string  `json:"addr"`
	TotalConns   int     `json:"total_conns"`
	IdleConns    int     `json:"idle_conns"`
	BusyConns    int     `json:"busy_conns"`
	MaxSize      int     `json:"max_size"`
	MinSize      int     `json:"min_size"`
	TotalCreated int64   `json:"total_created"`
	TotalClosed  int64   `json:"total_closed"`
	TotalErrors  int64   `json:"total_errors"`
	HitCount     int64   `json:"hit_count"`
	MissCount    int64   `json:"miss_count"`
	HitRate      float64 `json:"hit_rate"`
}