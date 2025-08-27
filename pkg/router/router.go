package router

import (
	"fmt"
	"strings"
	"sync"

	"github.com/predixy/predixy/pkg/cluster"
	"github.com/predixy/predixy/pkg/protocol"
	"go.uber.org/zap"
)

// CommandType 命令类型
type CommandType int

const (
	CommandTypeRead CommandType = iota
	CommandTypeWrite
	CommandTypeAdmin
	CommandTypeTransaction
	CommandTypeBlocking
	CommandTypeScript
)

// CommandInfo 命令信息
type CommandInfo struct {
	Name     string
	Type     CommandType
	KeyCount int // -1表示可变数量的key
	KeyStart int // key开始位置（从1开始）
	KeyStep  int // key之间的步长
	ReadOnly bool
}

// Router 命令路由器
type Router struct {
	mu sync.RWMutex
	
	// 集群管理器
	clusterMgr *cluster.Manager
	
	// 命令映射
	commands map[string]*CommandInfo
	
	// 日志
	logger *zap.Logger
}

// NewRouter 创建路由器
func NewRouter(clusterMgr *cluster.Manager, logger *zap.Logger) *Router {
	r := &Router{
		clusterMgr: clusterMgr,
		commands:   make(map[string]*CommandInfo),
		logger:     logger,
	}
	
	// 初始化命令映射
	r.initCommands()
	
	return r
}

// Route 路由Redis命令到对应节点
func (r *Router) Route(cmd *protocol.RedisValue) (*cluster.Node, error) {
	if !cmd.IsCommand() {
		return nil, fmt.Errorf("not a valid redis command")
	}
	
	cmdName := cmd.GetCommand()
	if cmdName == "" {
		return nil, fmt.Errorf("empty command name")
	}
	
	r.mu.RLock()
	cmdInfo, exists := r.commands[cmdName]
	r.mu.RUnlock()
	
	if !exists {
		// 未知命令，尝试使用第一个参数作为key
		key := cmd.GetKey()
		if key == nil {
			return nil, fmt.Errorf("unknown command and no key found: %s", cmdName)
		}
		
		slot := cluster.HashSlotBytes(key)
		node := r.clusterMgr.GetNodeBySlot(slot)
		if node == nil {
			return nil, fmt.Errorf("no node found for slot %d", slot)
		}
		
		return node, nil
	}
	
	// 根据命令类型进行路由
	switch cmdInfo.Type {
	case CommandTypeAdmin:
		return r.routeAdminCommand(cmd, cmdInfo)
	case CommandTypeTransaction:
		return r.routeTransactionCommand(cmd, cmdInfo)
	case CommandTypeBlocking:
		return r.routeBlockingCommand(cmd, cmdInfo)
	case CommandTypeScript:
		return r.routeScriptCommand(cmd, cmdInfo)
	default:
		return r.routeDataCommand(cmd, cmdInfo)
	}
}

// routeDataCommand 路由数据命令
func (r *Router) routeDataCommand(cmd *protocol.RedisValue, cmdInfo *CommandInfo) (*cluster.Node, error) {
	keys := r.extractKeys(cmd, cmdInfo)
	if len(keys) == 0 {
		return nil, fmt.Errorf("no keys found for command %s", cmdInfo.Name)
	}
	
	// 单key命令
	if len(keys) == 1 {
		slot := cluster.HashSlotBytes(keys[0])
		node := r.clusterMgr.GetNodeBySlot(slot)
		if node == nil {
			return nil, fmt.Errorf("no node found for slot %d", slot)
		}
		return node, nil
	}
	
	// 多key命令，检查是否在同一个槽位
	firstSlot := cluster.HashSlotBytes(keys[0])
	for i := 1; i < len(keys); i++ {
		slot := cluster.HashSlotBytes(keys[i])
		if slot != firstSlot {
			return nil, fmt.Errorf("multi-key command %s with keys in different slots", cmdInfo.Name)
		}
	}
	
	node := r.clusterMgr.GetNodeBySlot(firstSlot)
	if node == nil {
		return nil, fmt.Errorf("no node found for slot %d", firstSlot)
	}
	
	return node, nil
}

// routeAdminCommand 路由管理命令
func (r *Router) routeAdminCommand(cmd *protocol.RedisValue, cmdInfo *CommandInfo) (*cluster.Node, error) {
	// 管理命令通常发送到任意一个主节点
	masters := r.clusterMgr.GetMasterNodes()
	if len(masters) == 0 {
		return nil, fmt.Errorf("no master nodes available")
	}
	
	// 选择第一个连接正常的主节点
	for _, node := range masters {
		if node.IsConnected() {
			return node, nil
		}
	}
	
	return nil, fmt.Errorf("no connected master nodes available")
}

// routeTransactionCommand 路由事务命令
func (r *Router) routeTransactionCommand(cmd *protocol.RedisValue, cmdInfo *CommandInfo) (*cluster.Node, error) {
	// 事务命令需要特殊处理，暂时不支持跨槽位事务
	return nil, fmt.Errorf("transaction commands not supported in cluster mode: %s", cmdInfo.Name)
}

// routeBlockingCommand 路由阻塞命令
func (r *Router) routeBlockingCommand(cmd *protocol.RedisValue, cmdInfo *CommandInfo) (*cluster.Node, error) {
	// 阻塞命令按照普通数据命令处理
	return r.routeDataCommand(cmd, cmdInfo)
}

// routeScriptCommand 路由脚本命令
func (r *Router) routeScriptCommand(cmd *protocol.RedisValue, cmdInfo *CommandInfo) (*cluster.Node, error) {
	cmdName := cmd.GetCommand()
	args := cmd.GetArgs()
	
	switch cmdName {
	case "EVAL", "EVALSHA":
		// EVAL script numkeys key1 key2 ... arg1 arg2 ...
		if len(args) < 2 {
			return nil, fmt.Errorf("invalid %s command format", cmdName)
		}
		
		// 解析key数量
		numKeysStr := string(args[1])
		numKeys := 0
		for _, c := range numKeysStr {
			if c >= '0' && c <= '9' {
				numKeys = numKeys*10 + int(c-'0')
			} else {
				return nil, fmt.Errorf("invalid numkeys in %s command: %s", cmdName, numKeysStr)
			}
		}
		
		if numKeys == 0 {
			// 没有key，发送到任意主节点
			return r.routeAdminCommand(cmd, cmdInfo)
		}
		
		if len(args) < 2+numKeys {
			return nil, fmt.Errorf("insufficient keys in %s command", cmdName)
		}
		
		// 提取所有key
		keys := make([][]byte, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = args[2+i]
		}
		
		// 检查所有key是否在同一个槽位
		if len(keys) > 0 {
			firstSlot := cluster.HashSlotBytes(keys[0])
			for i := 1; i < len(keys); i++ {
				slot := cluster.HashSlotBytes(keys[i])
				if slot != firstSlot {
					return nil, fmt.Errorf("script keys in different slots")
				}
			}
			
			node := r.clusterMgr.GetNodeBySlot(firstSlot)
			if node == nil {
				return nil, fmt.Errorf("no node found for slot %d", firstSlot)
			}
			return node, nil
		}
		
		return r.routeAdminCommand(cmd, cmdInfo)
	
	default:
		// 其他脚本命令发送到任意主节点
		return r.routeAdminCommand(cmd, cmdInfo)
	}
}

// extractKeys 从命令中提取key
func (r *Router) extractKeys(cmd *protocol.RedisValue, cmdInfo *CommandInfo) [][]byte {
	args := cmd.GetArgs()
	
	if cmdInfo.KeyCount == 0 {
		return nil
	}
	
	if cmdInfo.KeyCount == 1 {
		// 单key命令
		if len(args) >= cmdInfo.KeyStart {
			return [][]byte{args[cmdInfo.KeyStart-1]}
		}
		return nil
	}
	
	if cmdInfo.KeyCount > 0 {
		// 固定数量的key
		keys := make([][]byte, 0, cmdInfo.KeyCount)
		for i := 0; i < cmdInfo.KeyCount; i++ {
			idx := cmdInfo.KeyStart - 1 + i*cmdInfo.KeyStep
			if idx < len(args) {
				keys = append(keys, args[idx])
			}
		}
		return keys
	}
	
	// 可变数量的key
	switch cmd.GetCommand() {
	case "MGET":
		// MGET key1 key2 key3 ...
		return args
	case "MSET", "MSETNX":
		// MSET key1 value1 key2 value2 ...
		keys := make([][]byte, 0, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			keys = append(keys, args[i])
		}
		return keys
	case "DEL", "EXISTS", "UNLINK":
		// DEL key1 key2 key3 ...
		return args
	default:
		// 默认返回第一个参数
		if len(args) > 0 {
			return [][]byte{args[0]}
		}
	}
	
	return nil
}

// GetCommandInfo 获取命令信息
func (r *Router) GetCommandInfo(cmdName string) *CommandInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	return r.commands[strings.ToUpper(cmdName)]
}

// IsReadOnlyCommand 检查是否为只读命令
func (r *Router) IsReadOnlyCommand(cmdName string) bool {
	cmdInfo := r.GetCommandInfo(cmdName)
	return cmdInfo != nil && cmdInfo.ReadOnly
}

// CanUseSlaveNode 检查命令是否可以使用从节点
func (r *Router) CanUseSlaveNode(cmd *protocol.RedisValue) bool {
	cmdName := cmd.GetCommand()
	return r.IsReadOnlyCommand(cmdName)
}

// initCommands 初始化命令映射
func (r *Router) initCommands() {
	// 字符串命令
	r.addCommand("GET", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("SET", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("SETNX", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("SETEX", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("PSETEX", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("GETSET", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("STRLEN", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("APPEND", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("INCR", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("DECR", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("INCRBY", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("DECRBY", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("INCRBYFLOAT", CommandTypeWrite, 1, 1, 1, false)
	
	// 多key字符串命令
	r.addCommand("MGET", CommandTypeRead, -1, 1, 1, true)
	r.addCommand("MSET", CommandTypeWrite, -1, 1, 2, false)
	r.addCommand("MSETNX", CommandTypeWrite, -1, 1, 2, false)
	
	// 通用命令
	r.addCommand("DEL", CommandTypeWrite, -1, 1, 1, false)
	r.addCommand("EXISTS", CommandTypeRead, -1, 1, 1, true)
	r.addCommand("EXPIRE", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("EXPIREAT", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("PEXPIRE", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("PEXPIREAT", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("TTL", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("PTTL", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("PERSIST", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("TYPE", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("RENAME", CommandTypeWrite, 2, 1, 1, false)
	r.addCommand("RENAMENX", CommandTypeWrite, 2, 1, 1, false)
	
	// 哈希命令
	r.addCommand("HGET", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("HSET", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("HSETNX", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("HMGET", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("HMSET", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("HGETALL", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("HKEYS", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("HVALS", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("HLEN", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("HEXISTS", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("HDEL", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("HINCRBY", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("HINCRBYFLOAT", CommandTypeWrite, 1, 1, 1, false)
	
	// 列表命令
	r.addCommand("LPUSH", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("RPUSH", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("LPOP", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("RPOP", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("LLEN", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("LINDEX", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("LRANGE", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("LSET", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("LTRIM", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("LREM", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("LINSERT", CommandTypeWrite, 1, 1, 1, false)
	
	// 集合命令
	r.addCommand("SADD", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("SREM", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("SCARD", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("SISMEMBER", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("SMEMBERS", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("SPOP", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("SRANDMEMBER", CommandTypeRead, 1, 1, 1, true)
	
	// 有序集合命令
	r.addCommand("ZADD", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("ZREM", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("ZCARD", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("ZSCORE", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("ZRANK", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("ZREVRANK", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("ZRANGE", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("ZREVRANGE", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("ZRANGEBYSCORE", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("ZREVRANGEBYSCORE", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("ZCOUNT", CommandTypeRead, 1, 1, 1, true)
	r.addCommand("ZINCRBY", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("ZREMRANGEBYRANK", CommandTypeWrite, 1, 1, 1, false)
	r.addCommand("ZREMRANGEBYSCORE", CommandTypeWrite, 1, 1, 1, false)
	
	// 事务命令
	r.addCommand("MULTI", CommandTypeTransaction, 0, 0, 0, false)
	r.addCommand("EXEC", CommandTypeTransaction, 0, 0, 0, false)
	r.addCommand("DISCARD", CommandTypeTransaction, 0, 0, 0, false)
	r.addCommand("WATCH", CommandTypeTransaction, -1, 1, 1, false)
	r.addCommand("UNWATCH", CommandTypeTransaction, 0, 0, 0, false)
	
	// 脚本命令
	r.addCommand("EVAL", CommandTypeScript, -1, 3, 1, false)
	r.addCommand("EVALSHA", CommandTypeScript, -1, 3, 1, false)
	r.addCommand("SCRIPT", CommandTypeScript, 0, 0, 0, false)
	
	// 阻塞命令
	r.addCommand("BLPOP", CommandTypeBlocking, -1, 1, 1, false)
	r.addCommand("BRPOP", CommandTypeBlocking, -1, 1, 1, false)
	r.addCommand("BRPOPLPUSH", CommandTypeBlocking, 2, 1, 1, false)
	
	// 管理命令
	r.addCommand("PING", CommandTypeAdmin, 0, 0, 0, true)
	r.addCommand("INFO", CommandTypeAdmin, 0, 0, 0, true)
	r.addCommand("TIME", CommandTypeAdmin, 0, 0, 0, true)
	r.addCommand("DBSIZE", CommandTypeAdmin, 0, 0, 0, true)
	r.addCommand("FLUSHDB", CommandTypeAdmin, 0, 0, 0, false)
	r.addCommand("FLUSHALL", CommandTypeAdmin, 0, 0, 0, false)
	r.addCommand("KEYS", CommandTypeAdmin, 0, 0, 0, true)
	r.addCommand("SCAN", CommandTypeAdmin, 0, 0, 0, true)
}

// addCommand 添加命令信息
func (r *Router) addCommand(name string, cmdType CommandType, keyCount, keyStart, keyStep int, readOnly bool) {
	r.commands[name] = &CommandInfo{
		Name:     name,
		Type:     cmdType,
		KeyCount: keyCount,
		KeyStart: keyStart,
		KeyStep:  keyStep,
		ReadOnly: readOnly,
	}
}