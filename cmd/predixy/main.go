package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/predixy/predixy/pkg/config"
	"github.com/predixy/predixy/pkg/proxy"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// 版本信息
var (
	Version   = "1.0.0"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

// 命令行参数
var (
	configFile = flag.String("config", "predixy.conf", "配置文件路径")
	logLevel   = flag.String("log-level", "info", "日志级别 (debug, info, warn, error)")
	version    = flag.Bool("version", false, "显示版本信息")
	daemon     = flag.Bool("daemon", false, "以守护进程模式运行")
	pidFile    = flag.String("pid-file", "", "PID文件路径")
)

func main() {
	flag.Parse()

	// 显示版本信息
	if *version {
		fmt.Printf("Predixy Redis Cluster Proxy\n")
		fmt.Printf("Version: %s\n", Version)
		fmt.Printf("Build Time: %s\n", BuildTime)
		fmt.Printf("Git Commit: %s\n", GitCommit)
		os.Exit(0)
	}

	// 加载配置
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 验证配置
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config: %v\n", err)
		os.Exit(1)
	}

	// 如果命令行指定了日志级别，覆盖配置文件中的设置
	if *logLevel != "info" {
		cfg.Log.Level = *logLevel
	}

	// 初始化日志
	logger, err := initLogger(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting Predixy Redis Cluster Proxy",
		zap.String("version", Version),
		zap.String("build_time", BuildTime),
		zap.String("git_commit", GitCommit))

	// 转换为proxy配置
	proxyConfig, err := convertToProxyConfig(cfg)
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	// 写入PID文件
	if *pidFile != "" {
		if err := writePidFile(*pidFile); err != nil {
			logger.Fatal("Failed to write PID file", zap.Error(err))
		}
		defer removePidFile(*pidFile)
	}

	// 创建代理服务器
	proxy, err := proxy.NewProxy(proxyConfig, logger)
	if err != nil {
		logger.Fatal("Failed to create proxy", zap.Error(err))
	}

	// 设置信号处理
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// 启动代理服务器
	go func() {
		if err := proxy.Start(); err != nil {
			logger.Error("Proxy server error", zap.Error(err))
			cancel()
		}
	}()

	logger.Info("Proxy server started",
		zap.String("listen_addr", proxyConfig.ListenAddr),
		zap.Strings("cluster_nodes", proxyConfig.ClusterNodes))

	// 等待信号
	select {
	case sig := <-sigChan:
		logger.Info("Received signal", zap.String("signal", sig.String()))
		if sig == syscall.SIGHUP {
			// 重新加载配置
			logger.Info("Reloading configuration...")
			newCfg, err := config.LoadConfig(*configFile)
			if err != nil {
				logger.Error("Failed to reload config", zap.Error(err))
			} else {
				newProxyConfig, err := convertToProxyConfig(newCfg)
				if err != nil {
					logger.Error("Failed to convert config", zap.Error(err))
				} else {
					// TODO: 实现配置热重载
					_ = newProxyConfig
				}
				logger.Info("Configuration reloaded", zap.Any("config", newCfg))
			}
		} else {
			// 优雅关闭
			cancel()
		}
	case <-ctx.Done():
		logger.Info("Context cancelled")
	}

	// 停止代理服务器
	logger.Info("Shutting down proxy server...")

	if err := proxy.Stop(); err != nil {
		logger.Error("Failed to stop proxy gracefully", zap.Error(err))
	} else {
		logger.Info("Proxy server stopped gracefully")
	}
}

// initLogger 初始化日志记录器
func initLogger(cfg *config.Config) (*zap.Logger, error) {
	var zapLevel zapcore.Level
	switch cfg.Log.Level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		return nil, fmt.Errorf("invalid log level: %s", cfg.Log.Level)
	}

	// 配置编码器
	var encoder zapcore.Encoder
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.CallerKey = "caller"
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

	if cfg.Log.Format == "console" {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	}

	// 配置输出目标
	var writeSyncer zapcore.WriteSyncer
	switch cfg.Log.Output {
	case "stdout":
		writeSyncer = zapcore.AddSync(os.Stdout)
	case "stderr":
		writeSyncer = zapcore.AddSync(os.Stderr)
	case "file":
		// 使用lumberjack进行日志轮转
		lumberjackLogger := &lumberjack.Logger{
			Filename:   cfg.Log.Filename,
			MaxSize:    cfg.Log.MaxSize, // MB
			MaxAge:     cfg.Log.MaxAge,  // days
			MaxBackups: cfg.Log.MaxBackups,
			Compress:   cfg.Log.Compress,
		}
		writeSyncer = zapcore.AddSync(lumberjackLogger)
	default:
		return nil, fmt.Errorf("invalid log output: %s", cfg.Log.Output)
	}

	// 创建核心
	core := zapcore.NewCore(encoder, writeSyncer, zapLevel)

	// 创建logger
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return logger, nil
}

// loadConfig 加载配置文件
// convertToProxyConfig 将config.Config转换为proxy.Config
func convertToProxyConfig(cfg *config.Config) (*proxy.Config, error) {
	// 转换为proxy.Config
	proxyConfig := &proxy.Config{
		ListenAddr:        fmt.Sprintf("%s:%d", cfg.Server.Bind, cfg.Server.Port),
		ClusterNodes:      cfg.Cluster.EntryPoints,
		MaxClients:        cfg.Server.MaxClients,
		Timeout:           cfg.Cluster.ConnectTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		PoolMinSize:       cfg.Pool.MinIdle,
		PoolMaxSize:       cfg.Pool.MaxActive,
		PoolIdleTimeout:   cfg.Pool.IdleTimeout,
		PoolMaxLifetime:   cfg.Pool.WaitTimeout,
		KeepAlive:         cfg.Server.TCPKeepAlive > 0,
		KeepAlivePeriod:   cfg.Server.TCPKeepAlive,
	}

	return proxyConfig, nil
}

func loadConfig(configFile string) (*proxy.Config, error) {
	// 加载配置文件
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// 验证配置
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// 转换为proxy.Config
	proxyConfig := &proxy.Config{
		ListenAddr:        fmt.Sprintf("%s:%d", cfg.Server.Bind, cfg.Server.Port),
		ClusterNodes:      cfg.Cluster.EntryPoints,
		MaxClients:        cfg.Server.MaxClients,
		Timeout:           cfg.Cluster.ConnectTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		PoolMinSize:       cfg.Pool.MinIdle,
		PoolMaxSize:       cfg.Pool.MaxActive,
		PoolIdleTimeout:   cfg.Pool.IdleTimeout,
		PoolMaxLifetime:   cfg.Pool.WaitTimeout,
		KeepAlive:         cfg.Server.TCPKeepAlive > 0,
		KeepAlivePeriod:   cfg.Server.TCPKeepAlive,
	}

	return proxyConfig, nil
}

// writePidFile 写入PID文件
func writePidFile(pidFile string) error {
	pid := os.Getpid()
	return os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", pid)), 0644)
}

// removePidFile 删除PID文件
func removePidFile(pidFile string) {
	if err := os.Remove(pidFile); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to remove PID file: %v\n", err)
	}
}