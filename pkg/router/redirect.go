package router

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/predixy/predixy/pkg/cluster"
	"github.com/predixy/predixy/pkg/protocol"
	"go.uber.org/zap"
)

// RedirectType 重定向类型
type RedirectType int

const (
	RedirectTypeMoved RedirectType = iota
	RedirectTypeAsk
)

// RedirectInfo 重定向信息
type RedirectInfo struct {
	Type RedirectType
	Slot int
	Addr string
	Node *cluster.Node
}

// RedirectHandler 重定向处理器
type RedirectHandler struct {
	mu sync.RWMutex
	
	// 集群管理器
	clusterMgr *cluster.Manager
	
	// 路由器
	router *Router
	
	// ASK重定向缓存
	askCache map[string]*RedirectInfo
	
	// 重定向统计
	movedCount int64
	askCount   int64
	
	// 日志
	logger *zap.Logger
}

// NewRedirectHandler 创建重定向处理器
func NewRedirectHandler(clusterMgr *cluster.Manager, router *Router, logger *zap.Logger) *RedirectHandler {
	h := &RedirectHandler{
		clusterMgr: clusterMgr,
		router:     router,
		askCache:   make(map[string]*RedirectInfo),
		logger:     logger,
	}
	
	// 启动ASK缓存清理任务
	go h.cleanupASKCache()
	
	return h
}

// HandleRedirect 处理重定向响应
func (h *RedirectHandler) HandleRedirect(resp *protocol.RedisValue, originalCmd *protocol.RedisValue) (*cluster.Node, error) {
	if resp.Type != protocol.TypeError {
		return nil, fmt.Errorf("not a redirect response")
	}
	
	errorMsg := resp.Str
	
	// 解析重定向信息
	redirectInfo, err := h.parseRedirectError(errorMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redirect error: %v", err)
	}
	
	// 根据重定向类型处理
	switch redirectInfo.Type {
	case RedirectTypeMoved:
		return h.handleMoved(redirectInfo, originalCmd)
	case RedirectTypeAsk:
		return h.handleASK(redirectInfo, originalCmd)
	default:
		return nil, fmt.Errorf("unknown redirect type")
	}
}

// handleMoved 处理MOVED重定向
func (h *RedirectHandler) handleMoved(redirectInfo *RedirectInfo, originalCmd *protocol.RedisValue) (*cluster.Node, error) {
	h.mu.Lock()
	h.movedCount++
	h.mu.Unlock()
	
	h.logger.Debug("handling MOVED redirect",
		zap.Int("slot", redirectInfo.Slot),
		zap.String("addr", redirectInfo.Addr),
		zap.String("command", originalCmd.GetCommand()))
	
	// MOVED表示槽位已经永久迁移，需要更新集群拓扑
	node := h.findNodeByAddr(redirectInfo.Addr)
	if node == nil {
		// 节点不存在，触发集群拓扑刷新
		h.logger.Info("node not found for MOVED redirect, refreshing cluster topology",
			zap.String("addr", redirectInfo.Addr),
			zap.Int("slot", redirectInfo.Slot))
		
		ctx := context.Background()
		if err := h.clusterMgr.RefreshTopology(ctx); err != nil {
			h.logger.Error("failed to refresh cluster topology", zap.Error(err))
			return nil, fmt.Errorf("failed to refresh cluster topology: %v", err)
		}
		
		// 重新获取节点
		node = h.findNodeByAddr(redirectInfo.Addr)
		if node == nil {
			return nil, fmt.Errorf("node still not found after topology refresh: %s", redirectInfo.Addr)
		}
	}
	
	return node, nil
}

// handleASK 处理ASK重定向
func (h *RedirectHandler) handleASK(redirectInfo *RedirectInfo, originalCmd *protocol.RedisValue) (*cluster.Node, error) {
	h.mu.Lock()
	h.askCount++
	h.mu.Unlock()
	
	h.logger.Debug("handling ASK redirect",
		zap.Int("slot", redirectInfo.Slot),
		zap.String("addr", redirectInfo.Addr),
		zap.String("command", originalCmd.GetCommand()))
	
	// ASK表示槽位正在迁移中，这是临时重定向
	node := h.findNodeByAddr(redirectInfo.Addr)
	if node == nil {
		return nil, fmt.Errorf("node not found for ASK redirect: %s", redirectInfo.Addr)
	}
	
	// 缓存ASK重定向信息（短时间内）
	cacheKey := fmt.Sprintf("%d:%s", redirectInfo.Slot, redirectInfo.Addr)
	h.mu.Lock()
	h.askCache[cacheKey] = &RedirectInfo{
		Type: RedirectTypeAsk,
		Slot: redirectInfo.Slot,
		Addr: redirectInfo.Addr,
		Node: node,
	}
	h.mu.Unlock()
	
	return node, nil
}

// CheckASKCache 检查ASK缓存
func (h *RedirectHandler) CheckASKCache(slot int) *cluster.Node {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	for key, info := range h.askCache {
		if info.Slot == slot {
			h.logger.Debug("found ASK cache hit",
				zap.Int("slot", slot),
				zap.String("cache_key", key))
			return info.Node
		}
	}
	
	return nil
}

// parseRedirectError 解析重定向错误信息
func (h *RedirectHandler) parseRedirectError(errorMsg string) (*RedirectInfo, error) {
	// MOVED格式: "MOVED 3999 127.0.0.1:7001"
	// ASK格式: "ASK 3999 127.0.0.1:7001"
	
	parts := strings.Fields(errorMsg)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid redirect error format: %s", errorMsg)
	}
	
	var redirectType RedirectType
	switch parts[0] {
	case "MOVED":
		redirectType = RedirectTypeMoved
	case "ASK":
		redirectType = RedirectTypeAsk
	default:
		return nil, fmt.Errorf("unknown redirect type: %s", parts[0])
	}
	
	// 解析槽位号
	slot, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid slot number: %s", parts[1])
	}
	
	if slot < 0 || slot > 16383 {
		return nil, fmt.Errorf("slot number out of range: %d", slot)
	}
	
	// 解析节点地址
	addr := parts[2]
	if !h.isValidAddress(addr) {
		return nil, fmt.Errorf("invalid node address: %s", addr)
	}
	
	return &RedirectInfo{
		Type: redirectType,
		Slot: slot,
		Addr: addr,
	}, nil
}

// isValidAddress 验证地址格式
func (h *RedirectHandler) isValidAddress(addr string) bool {
	// 简单的地址格式验证: host:port
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return false
	}
	
	// 验证端口号
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return false
	}
	
	return port > 0 && port <= 65535
}

// cleanupASKCache 清理ASK缓存
func (h *RedirectHandler) cleanupASKCache() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		h.mu.Lock()
		// 清空ASK缓存（ASK重定向是临时的）
		if len(h.askCache) > 0 {
			h.logger.Debug("cleaning up ASK cache", zap.Int("entries", len(h.askCache)))
			h.askCache = make(map[string]*RedirectInfo)
		}
		h.mu.Unlock()
	}
}

// GetStats 获取重定向统计信息
func (h *RedirectHandler) GetStats() map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	return map[string]interface{}{
		"moved_count":     h.movedCount,
		"ask_count":       h.askCount,
		"ask_cache_size":  len(h.askCache),
	}
}

// IsRedirectError 检查是否为重定向错误
func IsRedirectError(resp *protocol.RedisValue) bool {
	if resp.Type != protocol.TypeError {
		return false
	}
	
	errorMsg := resp.Str
	return strings.HasPrefix(errorMsg, "MOVED ") || strings.HasPrefix(errorMsg, "ASK ")
}

// NeedsASKCommand 检查是否需要发送ASKING命令
func (h *RedirectHandler) NeedsASKCommand(node *cluster.Node, slot int) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	
	for _, info := range h.askCache {
		if info.Slot == slot && info.Node == node {
			return true
		}
	}
	
	return false
}

// CreateASKCommand 创建ASKING命令
func (h *RedirectHandler) CreateASKCommand() *protocol.RedisValue {
	return &protocol.RedisValue{
		Type: protocol.TypeArray,
		Array: []*protocol.RedisValue{
			{
				Type: protocol.TypeBulkString,
				Bulk: []byte("ASKING"),
			},
		},
	}
}

// findNodeByAddr 根据地址查找节点
func (h *RedirectHandler) findNodeByAddr(addr string) *cluster.Node {
	allNodes := h.clusterMgr.GetAllNodes()
	for _, node := range allNodes {
		if node.Addr == addr {
			return node
		}
	}
	return nil
}