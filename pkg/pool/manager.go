package pool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Manager 连接池管理器
type Manager struct {
	mu sync.RWMutex
	
	// 连接池映射
	pools map[string]*Pool // addr -> Pool
	
	// 配置
	config *PoolConfig
	
	// 日志
	logger *zap.Logger
	
	// 状态
	closed bool
}

// NewManager 创建连接池管理器
func NewManager(config *PoolConfig, logger *zap.Logger) *Manager {
	if config == nil {
		config = DefaultPoolConfig()
	}
	
	return &Manager{
		pools:  make(map[string]*Pool),
		config: config,
		logger: logger,
	}
}

// GetPool 获取指定地址的连接池
func (m *Manager) GetPool(addr string) *Pool {
	m.mu.RLock()
	pool, exists := m.pools[addr]
	m.mu.RUnlock()
	
	if exists {
		return pool
	}
	
	// 创建新的连接池
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// 双重检查
	if pool, exists := m.pools[addr]; exists {
		return pool
	}
	
	if m.closed {
		return nil
	}
	
	pool = NewPool(addr, m.config, m.logger)
	m.pools[addr] = pool
	
	m.logger.Info("Created new connection pool", zap.String("addr", addr))
	
	return pool
}

// GetConnection 获取指定地址的连接
func (m *Manager) GetConnection(ctx context.Context, addr string) (*Connection, error) {
	pool := m.GetPool(addr)
	if pool == nil {
		return nil, fmt.Errorf("failed to get pool for %s", addr)
	}
	
	return pool.Get(ctx)
}

// PutConnection 归还连接
func (m *Manager) PutConnection(conn *Connection) {
	if conn == nil {
		return
	}
	
	m.mu.RLock()
	pool, exists := m.pools[conn.Addr()]
	m.mu.RUnlock()
	
	if exists {
		pool.Put(conn)
	} else {
		// 如果池不存在，直接关闭连接
		conn.Close()
	}
}

// RemovePool 移除指定地址的连接池
func (m *Manager) RemovePool(addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	pool, exists := m.pools[addr]
	if !exists {
		return nil
	}
	
	delete(m.pools, addr)
	
	m.logger.Info("Removing connection pool", zap.String("addr", addr))
	
	return pool.Close()
}

// UpdatePools 更新连接池列表
func (m *Manager) UpdatePools(addrs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.closed {
		return fmt.Errorf("manager is closed")
	}
	
	// 创建地址集合
	addrSet := make(map[string]bool)
	for _, addr := range addrs {
		addrSet[addr] = true
	}
	
	// 移除不再需要的连接池
	for addr, pool := range m.pools {
		if !addrSet[addr] {
			delete(m.pools, addr)
			m.logger.Info("Removing unused connection pool", zap.String("addr", addr))
			go pool.Close() // 异步关闭
		}
	}
	
	// 为新地址创建连接池（延迟创建）
	for addr := range addrSet {
		if _, exists := m.pools[addr]; !exists {
			m.logger.Debug("Will create pool for new address", zap.String("addr", addr))
		}
	}
	
	return nil
}

// GetAllPools 获取所有连接池
func (m *Manager) GetAllPools() map[string]*Pool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	pools := make(map[string]*Pool, len(m.pools))
	for addr, pool := range m.pools {
		pools[addr] = pool
	}
	
	return pools
}

// GetStats 获取所有连接池统计信息
func (m *Manager) GetStats() map[string]PoolStats {
	m.mu.RLock()
	defer m.mu.Unlock()
	
	stats := make(map[string]PoolStats, len(m.pools))
	for addr, pool := range m.pools {
		stats[addr] = pool.Stats()
	}
	
	return stats
}

// GetTotalStats 获取汇总统计信息
func (m *Manager) GetTotalStats() TotalStats {
	m.mu.RLock()
	defer m.mu.Unlock()
	
	stats := TotalStats{
		PoolCount: len(m.pools),
	}
	
	for _, pool := range m.pools {
		poolStats := pool.Stats()
		stats.TotalConns += poolStats.TotalConns
		stats.IdleConns += poolStats.IdleConns
		stats.BusyConns += poolStats.BusyConns
		stats.TotalCreated += poolStats.TotalCreated
		stats.TotalClosed += poolStats.TotalClosed
		stats.TotalErrors += poolStats.TotalErrors
		stats.HitCount += poolStats.HitCount
		stats.MissCount += poolStats.MissCount
	}
	
	// 计算总命中率
	total := stats.HitCount + stats.MissCount
	if total > 0 {
		stats.HitRate = float64(stats.HitCount) / float64(total)
	}
	
	return stats
}

// GetAllConnections 获取所有连接信息
func (m *Manager) GetAllConnections() map[string][]ConnectionStats {
	m.mu.RLock()
	defer m.mu.Unlock()
	
	connections := make(map[string][]ConnectionStats, len(m.pools))
	for addr, pool := range m.pools {
		connections[addr] = pool.GetConnections()
	}
	
	return connections
}

// Ping 对所有连接池执行健康检查
func (m *Manager) Ping(ctx context.Context) map[string]error {
	m.mu.RLock()
	pools := make(map[string]*Pool, len(m.pools))
	for addr, pool := range m.pools {
		pools[addr] = pool
	}
	m.mu.RUnlock()
	
	results := make(map[string]error, len(pools))
	var wg sync.WaitGroup
	var mu sync.Mutex
	
	for addr, pool := range pools {
		wg.Add(1)
		go func(addr string, pool *Pool) {
			defer wg.Done()
			
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			
			conn, err := pool.Get(ctx)
			if err != nil {
				mu.Lock()
				results[addr] = err
				mu.Unlock()
				return
			}
			
			defer pool.Put(conn)
			
			// 简单的连接测试
			if conn.IsClosed() {
				mu.Lock()
				results[addr] = fmt.Errorf("connection is closed")
				mu.Unlock()
				return
			}
			
			mu.Lock()
			results[addr] = nil
			mu.Unlock()
		}(addr, pool)
	}
	
	wg.Wait()
	return results
}

// Close 关闭所有连接池
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.closed {
		return nil
	}
	
	m.closed = true
	
	m.logger.Info("Closing connection pool manager")
	
	// 关闭所有连接池
	var wg sync.WaitGroup
	for addr, pool := range m.pools {
		wg.Add(1)
		go func(addr string, pool *Pool) {
			defer wg.Done()
			if err := pool.Close(); err != nil {
				m.logger.Warn("Failed to close pool",
					zap.String("addr", addr), zap.Error(err))
			}
		}(addr, pool)
	}
	
	wg.Wait()
	m.pools = make(map[string]*Pool)
	
	m.logger.Info("Connection pool manager closed")
	
	return nil
}

// TotalStats 汇总统计信息
type TotalStats struct {
	PoolCount    int     `json:"pool_count"`
	TotalConns   int     `json:"total_conns"`
	IdleConns    int     `json:"idle_conns"`
	BusyConns    int     `json:"busy_conns"`
	TotalCreated int64   `json:"total_created"`
	TotalClosed  int64   `json:"total_closed"`
	TotalErrors  int64   `json:"total_errors"`
	HitCount     int64   `json:"hit_count"`
	MissCount    int64   `json:"miss_count"`
	HitRate      float64 `json:"hit_rate"`
}