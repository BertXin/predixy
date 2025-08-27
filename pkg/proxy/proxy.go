package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/predixy/predixy/pkg/cluster"
	"github.com/predixy/predixy/pkg/pool"
	"github.com/predixy/predixy/pkg/protocol"
	"github.com/predixy/predixy/pkg/router"
	"go.uber.org/zap"
)

// Config 代理配置
type Config struct {
	// 监听配置
	ListenAddr string
	
	// 集群配置
	ClusterNodes []string
	Password     string
	Username     string
	Timeout      time.Duration
	
	// 连接池配置
	PoolMinSize     int
	PoolMaxSize     int
	PoolIdleTimeout time.Duration
	PoolMaxLifetime time.Duration
	
	// 其他配置
	MaxClients      int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	KeepAlive       bool
	KeepAlivePeriod time.Duration
}

// Proxy Redis集群代理
type Proxy struct {
	mu sync.RWMutex
	
	// 配置
	config *Config
	
	// 核心组件
	clusterMgr      *cluster.Manager
	poolMgr         *pool.Manager
	router          *router.Router
	redirectHandler *router.RedirectHandler
	
	// 网络组件
	listener net.Listener
	
	// 客户端连接管理
	clients    map[string]*Client
	clientsMu  sync.RWMutex
	maxClients int32
	
	// 统计信息
	stats *ProxyStats
	
	// 日志
	logger *zap.Logger
	
	// 控制信号
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// ProxyStats 代理统计信息
type ProxyStats struct {
	// 连接统计
	TotalConnections    int64
	ActiveConnections   int64
	RejectedConnections int64
	
	// 命令统计
	TotalCommands    int64
	SuccessCommands  int64
	FailedCommands   int64
	RedirectCommands int64
	
	// 性能统计
	TotalLatency   int64 // 微秒
	MinLatency     int64
	MaxLatency     int64
	AvgLatency     int64
	
	// 错误统计
	ConnectionErrors int64
	TimeoutErrors    int64
	ProtocolErrors   int64
	ClusterErrors    int64
}

// Client 客户端连接
type Client struct {
	id       string
	conn     net.Conn
	parser   *protocol.Parser
	lastUsed time.Time
	proxy    *Proxy
	
	// 统计信息
	commandCount int64
	errorCount   int64
	bytesRead    int64
	bytesWritten int64
}

// NewProxy 创建代理实例
func NewProxy(config *Config, logger *zap.Logger) (*Proxy, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	
	if len(config.ClusterNodes) == 0 {
		return nil, fmt.Errorf("cluster nodes cannot be empty")
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	// 创建集群管理器
	clusterMgr := cluster.NewManager(
		config.ClusterNodes,
		config.Password,
		config.Username,
		config.Timeout,
		logger,
	)
	
	// 创建连接池管理器
	poolConfig := &pool.PoolConfig{
		MinSize:        config.PoolMinSize,
		MaxSize:        config.PoolMaxSize,
		IdleTimeout:    config.PoolIdleTimeout,
		MaxLifetime:    config.PoolMaxLifetime,
		ConnectTimeout: config.Timeout,
		ReadTimeout:    config.ReadTimeout,
		WriteTimeout:   config.WriteTimeout,
		CleanupInterval: 30 * time.Second, // 设置清理间隔为30秒
	}
	poolMgr := pool.NewManager(poolConfig, logger)
	
	// 创建路由器
	routerInstance := router.NewRouter(clusterMgr, logger)
	
	// 创建重定向处理器
	redirectHandler := router.NewRedirectHandler(clusterMgr, routerInstance, logger)
	
	proxy := &Proxy{
		config:          config,
		clusterMgr:      clusterMgr,
		poolMgr:         poolMgr,
		router:          routerInstance,
		redirectHandler: redirectHandler,
		clients:         make(map[string]*Client),
		maxClients:      int32(config.MaxClients),
		stats:           &ProxyStats{},
		logger:          logger,
		ctx:             ctx,
		cancel:          cancel,
		done:            make(chan struct{}),
	}
	
	return proxy, nil
}

// Start 启动代理服务
func (p *Proxy) Start() error {
	p.logger.Info("Starting Redis cluster proxy", zap.String("listen_addr", p.config.ListenAddr))
	
	// 启动集群管理器
	if err := p.clusterMgr.Start(p.ctx); err != nil {
		return fmt.Errorf("failed to start cluster manager: %v", err)
	}
	
	// 连接池管理器不需要显式启动
	
	// 创建监听器
	listener, err := net.Listen("tcp", p.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", p.config.ListenAddr, err)
	}
	p.listener = listener
	
	// 启动接受连接的goroutine
	go p.acceptConnections()
	
	// 启动统计更新goroutine
	go p.updateStats()
	
	p.logger.Info("Redis cluster proxy started successfully")
	return nil
}

// Stop 停止代理服务
func (p *Proxy) Stop() error {
	p.logger.Info("Stopping Redis cluster proxy")
	
	// 取消上下文
	p.cancel()
	
	// 关闭监听器
	if p.listener != nil {
		p.listener.Close()
	}
	
	// 关闭所有客户端连接
	p.clientsMu.Lock()
	for _, client := range p.clients {
		client.conn.Close()
	}
	p.clientsMu.Unlock()
	
	// 停止组件
	if err := p.poolMgr.Close(); err != nil {
		p.logger.Error("Failed to close pool manager", zap.Error(err))
	}
	
	if err := p.clusterMgr.Stop(); err != nil {
		p.logger.Error("Failed to stop cluster manager", zap.Error(err))
	}
	
	// 等待完成
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		p.logger.Warn("Timeout waiting for proxy to stop")
	}
	
	p.logger.Info("Redis cluster proxy stopped")
	return nil
}

// acceptConnections 接受客户端连接
func (p *Proxy) acceptConnections() {
	defer close(p.done)
	
	for {
		select {
	case <-p.ctx.Done():
			return
	default:
		}
		
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.ctx.Done():
				return
			default:
				p.logger.Error("Failed to accept connection", zap.Error(err))
				continue
			}
		}
		
		// 检查连接数限制
		if p.maxClients > 0 && atomic.LoadInt64(&p.stats.ActiveConnections) >= int64(p.maxClients) {
			atomic.AddInt64(&p.stats.RejectedConnections, 1)
			conn.Close()
			p.logger.Warn("Connection rejected due to max clients limit")
			continue
		}
		
		// 创建客户端
		client := p.newClient(conn)
		
		// 启动客户端处理goroutine
		go p.handleClient(client)
	}
}

// newClient 创建新客户端
func (p *Proxy) newClient(conn net.Conn) *Client {
	clientID := fmt.Sprintf("%s-%d", conn.RemoteAddr().String(), time.Now().UnixNano())
	
	// 设置连接选项
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if p.config.KeepAlive {
			tcpConn.SetKeepAlive(true)
			if p.config.KeepAlivePeriod > 0 {
				tcpConn.SetKeepAlivePeriod(p.config.KeepAlivePeriod)
			}
		}
	}
	
	client := &Client{
		id:       clientID,
		conn:     conn,
		parser:   protocol.NewParser(conn),
		lastUsed: time.Now(),
		proxy:    p,
	}
	
	// 添加到客户端列表
	p.clientsMu.Lock()
	p.clients[clientID] = client
	p.clientsMu.Unlock()
	
	// 更新统计
	atomic.AddInt64(&p.stats.TotalConnections, 1)
	atomic.AddInt64(&p.stats.ActiveConnections, 1)
	
	p.logger.Debug("New client connected", zap.String("client_id", clientID))
	return client
}

// handleClient 处理客户端请求
func (p *Proxy) handleClient(client *Client) {
	defer p.removeClient(client)
	
	for {
		select {
	case <-p.ctx.Done():
			return
	default:
		}
		
		// 设置读取超时
		if p.config.ReadTimeout > 0 {
			client.conn.SetReadDeadline(time.Now().Add(p.config.ReadTimeout))
		}
		
		// 解析Redis命令
		cmd, err := client.parser.Parse()
		if err != nil {
			if err == io.EOF {
				p.logger.Debug("Client disconnected", zap.String("client_id", client.id))
				return
			}
			
			atomic.AddInt64(&p.stats.ProtocolErrors, 1)
			client.errorCount++
			p.logger.Error("Failed to parse command", 
				zap.String("client_id", client.id), 
				zap.Error(err))
			
			// 发送错误响应
			p.sendError(client, fmt.Sprintf("ERR Protocol error: %v", err))
			continue
		}
		
		// 处理命令
		if err := p.processCommand(client, cmd); err != nil {
			p.logger.Error("Failed to process command",
				zap.String("client_id", client.id),
				zap.String("command", cmd.GetCommand()),
				zap.Error(err))
		}
		
		client.lastUsed = time.Now()
	}
}

// processCommand 处理Redis命令
func (p *Proxy) processCommand(client *Client, cmd *protocol.RedisValue) error {
	startTime := time.Now()
	defer func() {
		latency := time.Since(startTime).Microseconds()
		atomic.AddInt64(&p.stats.TotalLatency, latency)
		
		// 更新延迟统计
		if p.stats.MinLatency == 0 || latency < p.stats.MinLatency {
			atomic.StoreInt64(&p.stats.MinLatency, latency)
		}
		if latency > p.stats.MaxLatency {
			atomic.StoreInt64(&p.stats.MaxLatency, latency)
		}
	}()
	
	atomic.AddInt64(&p.stats.TotalCommands, 1)
	client.commandCount++
	
	// 路由命令到对应节点
	node, err := p.router.Route(cmd)
	if err != nil {
		atomic.AddInt64(&p.stats.FailedCommands, 1)
		client.errorCount++
		return p.sendError(client, fmt.Sprintf("ERR Routing error: %v", err))
	}
	
	// 获取连接
	ctx := context.Background()
	conn, err := p.poolMgr.GetConnection(ctx, node.Addr)
	if err != nil {
		atomic.AddInt64(&p.stats.ConnectionErrors, 1)
		client.errorCount++
		return p.sendError(client, fmt.Sprintf("ERR Connection error: %v", err))
	}
	defer p.poolMgr.PutConnection(conn)
	
	// 检查是否需要发送ASKING命令
	if p.redirectHandler.NeedsASKCommand(node, 0) { // TODO: 传入正确的slot
		askCmd := p.redirectHandler.CreateASKCommand()
		if err := p.sendCommandToNode(conn, askCmd); err != nil {
			p.logger.Warn("Failed to send ASKING command", zap.Error(err))
		}
		// 读取ASKING响应但不处理
		if _, err := protocol.NewParser(conn).Parse(); err != nil {
			p.logger.Warn("Failed to read ASKING response", zap.Error(err))
		}
	}
	
	// 发送命令到Redis节点
	if err := p.sendCommandToNode(conn, cmd); err != nil {
		atomic.AddInt64(&p.stats.ConnectionErrors, 1)
		client.errorCount++
		return p.sendError(client, fmt.Sprintf("ERR Send error: %v", err))
	}
	
	// 读取响应
	resp, err := protocol.NewParser(conn).Parse()
	if err != nil {
		atomic.AddInt64(&p.stats.ConnectionErrors, 1)
		client.errorCount++
		return p.sendError(client, fmt.Sprintf("ERR Read error: %v", err))
	}
	
	// 检查是否为重定向响应
	if router.IsRedirectError(resp) {
		atomic.AddInt64(&p.stats.RedirectCommands, 1)
		
		// 处理重定向
		newNode, err := p.redirectHandler.HandleRedirect(resp, cmd)
		if err != nil {
			atomic.AddInt64(&p.stats.ClusterErrors, 1)
			client.errorCount++
			return p.sendError(client, fmt.Sprintf("ERR Redirect error: %v", err))
		}
		
		// 重新发送命令到新节点
		return p.retryCommand(client, cmd, newNode)
	}
	
	// 发送响应给客户端
	if err := p.sendResponse(client, resp); err != nil {
		atomic.AddInt64(&p.stats.ConnectionErrors, 1)
		client.errorCount++
		return err
	}
	
	atomic.AddInt64(&p.stats.SuccessCommands, 1)
	return nil
}

// retryCommand 重试命令到新节点
func (p *Proxy) retryCommand(client *Client, cmd *protocol.RedisValue, node *cluster.Node) error {
	// 获取新节点的连接
	ctx := context.Background()
	conn, err := p.poolMgr.GetConnection(ctx, node.Addr)
	if err != nil {
		return p.sendError(client, fmt.Sprintf("ERR Retry connection error: %v", err))
	}
	defer p.poolMgr.PutConnection(conn)
	
	// 发送命令
	if err := p.sendCommandToNode(conn, cmd); err != nil {
		return p.sendError(client, fmt.Sprintf("ERR Retry send error: %v", err))
	}
	
	// 读取响应
	resp, err := protocol.NewParser(conn).Parse()
	if err != nil {
		return p.sendError(client, fmt.Sprintf("ERR Retry read error: %v", err))
	}
	
	// 发送响应给客户端
	return p.sendResponse(client, resp)
}

// sendCommandToNode 发送命令到Redis节点
func (p *Proxy) sendCommandToNode(conn *pool.Connection, cmd *protocol.RedisValue) error {
	data := cmd.Serialize()
	
	if p.config.WriteTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
	}
	
	_, err := conn.Write(data)
	return err
}

// sendResponse 发送响应给客户端
func (p *Proxy) sendResponse(client *Client, resp *protocol.RedisValue) error {
	data := resp.Serialize()
	
	if p.config.WriteTimeout > 0 {
		client.conn.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
	}
	
	n, err := client.conn.Write(data)
	if err == nil {
		client.bytesWritten += int64(n)
	}
	return err
}

// sendError 发送错误响应给客户端
func (p *Proxy) sendError(client *Client, errMsg string) error {
	errorResp := &protocol.RedisValue{
		Type: protocol.TypeError,
		Str:  errMsg,
	}
	return p.sendResponse(client, errorResp)
}

// removeClient 移除客户端
func (p *Proxy) removeClient(client *Client) {
	client.conn.Close()
	
	p.clientsMu.Lock()
	delete(p.clients, client.id)
	p.clientsMu.Unlock()
	
	atomic.AddInt64(&p.stats.ActiveConnections, -1)
	
	p.logger.Debug("Client disconnected",
		zap.String("client_id", client.id),
		zap.Int64("commands", client.commandCount),
		zap.Int64("errors", client.errorCount),
		zap.Int64("bytes_read", client.bytesRead),
		zap.Int64("bytes_written", client.bytesWritten))
}

// updateStats 更新统计信息
func (p *Proxy) updateStats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
	case <-p.ctx.Done():
			return
	case <-ticker.C:
			// 计算平均延迟
			totalCommands := atomic.LoadInt64(&p.stats.TotalCommands)
			if totalCommands > 0 {
				totalLatency := atomic.LoadInt64(&p.stats.TotalLatency)
				avgLatency := totalLatency / totalCommands
				atomic.StoreInt64(&p.stats.AvgLatency, avgLatency)
			}
			
			p.logger.Debug("Proxy stats",
				zap.Int64("active_connections", atomic.LoadInt64(&p.stats.ActiveConnections)),
				zap.Int64("total_commands", totalCommands),
				zap.Int64("success_commands", atomic.LoadInt64(&p.stats.SuccessCommands)),
				zap.Int64("failed_commands", atomic.LoadInt64(&p.stats.FailedCommands)),
				zap.Int64("avg_latency_us", atomic.LoadInt64(&p.stats.AvgLatency)))
		}
	}
}

// GetStats 获取代理统计信息
func (p *Proxy) GetStats() *ProxyStats {
	return &ProxyStats{
		TotalConnections:    atomic.LoadInt64(&p.stats.TotalConnections),
		ActiveConnections:   atomic.LoadInt64(&p.stats.ActiveConnections),
		RejectedConnections: atomic.LoadInt64(&p.stats.RejectedConnections),
		TotalCommands:       atomic.LoadInt64(&p.stats.TotalCommands),
		SuccessCommands:     atomic.LoadInt64(&p.stats.SuccessCommands),
		FailedCommands:      atomic.LoadInt64(&p.stats.FailedCommands),
		RedirectCommands:    atomic.LoadInt64(&p.stats.RedirectCommands),
		TotalLatency:        atomic.LoadInt64(&p.stats.TotalLatency),
		MinLatency:          atomic.LoadInt64(&p.stats.MinLatency),
		MaxLatency:          atomic.LoadInt64(&p.stats.MaxLatency),
		AvgLatency:          atomic.LoadInt64(&p.stats.AvgLatency),
		ConnectionErrors:    atomic.LoadInt64(&p.stats.ConnectionErrors),
		TimeoutErrors:       atomic.LoadInt64(&p.stats.TimeoutErrors),
		ProtocolErrors:      atomic.LoadInt64(&p.stats.ProtocolErrors),
		ClusterErrors:       atomic.LoadInt64(&p.stats.ClusterErrors),
	}
}

// GetClients 获取客户端列表
func (p *Proxy) GetClients() map[string]*Client {
	p.clientsMu.RLock()
	defer p.clientsMu.RUnlock()
	
	clients := make(map[string]*Client)
	for id, client := range p.clients {
		clients[id] = client
	}
	return clients
}