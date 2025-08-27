package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config 代理服务配置
type Config struct {
	Server    ServerConfig    `yaml:"server" mapstructure:"server"`
	Cluster   ClusterConfig   `yaml:"cluster" mapstructure:"cluster"`
	Pool      PoolConfig      `yaml:"pool" mapstructure:"pool"`
	Log       LogConfig       `yaml:"log" mapstructure:"log"`
	Metrics   MetricsConfig   `yaml:"metrics" mapstructure:"metrics"`
	Security  SecurityConfig  `yaml:"security" mapstructure:"security"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Port           int           `yaml:"port" mapstructure:"port"`
	Bind           string        `yaml:"bind" mapstructure:"bind"`
	MaxClients     int           `yaml:"max_clients" mapstructure:"max_clients"`
	WorkerThreads  int           `yaml:"worker_threads" mapstructure:"worker_threads"`
	ReadTimeout    time.Duration `yaml:"read_timeout" mapstructure:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout" mapstructure:"write_timeout"`
	IdleTimeout    time.Duration `yaml:"idle_timeout" mapstructure:"idle_timeout"`
	BufferSize     int           `yaml:"buffer_size" mapstructure:"buffer_size"`
	TCPKeepAlive   time.Duration `yaml:"tcp_keepalive" mapstructure:"tcp_keepalive"`
	TCPNoDelay     bool          `yaml:"tcp_nodelay" mapstructure:"tcp_nodelay"`
}

// ClusterConfig 集群配置
type ClusterConfig struct {
	EntryPoints          []string      `yaml:"entry_points" mapstructure:"entry_points"`
	Password             string        `yaml:"password" mapstructure:"password"`
	Username             string        `yaml:"username" mapstructure:"username"`
	ConnectTimeout       time.Duration `yaml:"connect_timeout" mapstructure:"connect_timeout"`
	ReadTimeout          time.Duration `yaml:"read_timeout" mapstructure:"read_timeout"`
	WriteTimeout         time.Duration `yaml:"write_timeout" mapstructure:"write_timeout"`
	HealthCheckInterval  time.Duration `yaml:"health_check_interval" mapstructure:"health_check_interval"`
	RefreshInterval      time.Duration `yaml:"refresh_interval" mapstructure:"refresh_interval"`
	MaxRetries           int           `yaml:"max_retries" mapstructure:"max_retries"`
	RetryDelay           time.Duration `yaml:"retry_delay" mapstructure:"retry_delay"`
	EnableCrossSlot      bool          `yaml:"enable_cross_slot" mapstructure:"enable_cross_slot"`
	ReadOnlyReplicas     bool          `yaml:"readonly_replicas" mapstructure:"readonly_replicas"`
}

// PoolConfig 连接池配置
type PoolConfig struct {
	Size                int           `yaml:"size" mapstructure:"size"`
	MinIdle             int           `yaml:"min_idle" mapstructure:"min_idle"`
	MaxIdle             int           `yaml:"max_idle" mapstructure:"max_idle"`
	MaxActive           int           `yaml:"max_active" mapstructure:"max_active"`
	IdleTimeout         time.Duration `yaml:"idle_timeout" mapstructure:"idle_timeout"`
	Wait                bool          `yaml:"wait" mapstructure:"wait"`
	WaitTimeout         time.Duration `yaml:"wait_timeout" mapstructure:"wait_timeout"`
	TestOnBorrow        bool          `yaml:"test_on_borrow" mapstructure:"test_on_borrow"`
	TestOnReturn        bool          `yaml:"test_on_return" mapstructure:"test_on_return"`
	TestWhileIdle       bool          `yaml:"test_while_idle" mapstructure:"test_while_idle"`
	TimeBetweenEviction time.Duration `yaml:"time_between_eviction" mapstructure:"time_between_eviction"`
}

// LogConfig 日志配置
type LogConfig struct {
	Level      string `yaml:"level" mapstructure:"level"`
	Format     string `yaml:"format" mapstructure:"format"`
	Output     string `yaml:"output" mapstructure:"output"`
	Filename   string `yaml:"filename" mapstructure:"filename"`
	MaxSize    int    `yaml:"max_size" mapstructure:"max_size"`
	MaxAge     int    `yaml:"max_age" mapstructure:"max_age"`
	MaxBackups int    `yaml:"max_backups" mapstructure:"max_backups"`
	Compress   bool   `yaml:"compress" mapstructure:"compress"`
}

// MetricsConfig 监控配置
type MetricsConfig struct {
	Enabled    bool   `yaml:"enabled" mapstructure:"enabled"`
	Port       int    `yaml:"port" mapstructure:"port"`
	Path       string `yaml:"path" mapstructure:"path"`
	Namespace  string `yaml:"namespace" mapstructure:"namespace"`
	Subsystem  string `yaml:"subsystem" mapstructure:"subsystem"`
}

// SecurityConfig 安全配置
type SecurityConfig struct {
	RequireAuth bool   `yaml:"require_auth" mapstructure:"require_auth"`
	Password    string `yaml:"password" mapstructure:"password"`
	Username    string `yaml:"username" mapstructure:"username"`
	TLSEnabled  bool   `yaml:"tls_enabled" mapstructure:"tls_enabled"`
	TLSCertFile string `yaml:"tls_cert_file" mapstructure:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file" mapstructure:"tls_key_file"`
	TLSCAFile   string `yaml:"tls_ca_file" mapstructure:"tls_ca_file"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port:          6379,
			Bind:          "0.0.0.0",
			MaxClients:    10000,
			WorkerThreads: 8,
			ReadTimeout:   30 * time.Second,
			WriteTimeout:  30 * time.Second,
			IdleTimeout:   300 * time.Second,
			BufferSize:    4096,
			TCPKeepAlive:  300 * time.Second,
			TCPNoDelay:    true,
		},
		Cluster: ClusterConfig{
			EntryPoints:         []string{"127.0.0.1:6379"},
			ConnectTimeout:      5 * time.Second,
			ReadTimeout:         3 * time.Second,
			WriteTimeout:        3 * time.Second,
			HealthCheckInterval: 30 * time.Second,
			RefreshInterval:     60 * time.Second,
			MaxRetries:          3,
			RetryDelay:          100 * time.Millisecond,
			EnableCrossSlot:     false,
			ReadOnlyReplicas:    true,
		},
		Pool: PoolConfig{
			Size:                100,
			MinIdle:             10,
			MaxIdle:             50,
			MaxActive:           200,
			IdleTimeout:         300 * time.Second,
			Wait:                true,
			WaitTimeout:         5 * time.Second,
			TestOnBorrow:        true,
			TestOnReturn:        false,
			TestWhileIdle:       true,
			TimeBetweenEviction: 60 * time.Second,
		},
		Log: LogConfig{
			Level:      "info",
			Format:     "json",
			Output:     "stdout",
			MaxSize:    100,
			MaxAge:     7,
			MaxBackups: 3,
			Compress:   true,
		},
		Metrics: MetricsConfig{
			Enabled:   true,
			Port:      9090,
			Path:      "/metrics",
			Namespace: "predixy",
			Subsystem: "proxy",
		},
		Security: SecurityConfig{
			RequireAuth: false,
			TLSEnabled:  false,
		},
	}
}

// LoadConfig 从文件加载配置
func LoadConfig(configFile string) (*Config, error) {
	config := DefaultConfig()
	
	if configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("./configs")
		viper.AddConfigPath("/etc/predixy")
	}
	
	// 设置环境变量前缀
	viper.SetEnvPrefix("PREDIXY")
	viper.AutomaticEnv()
	
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}
	
	if err := viper.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	
	return config, nil
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}
	
	if c.Server.MaxClients <= 0 {
		return fmt.Errorf("max_clients must be positive: %d", c.Server.MaxClients)
	}
	
	if c.Server.WorkerThreads <= 0 {
		return fmt.Errorf("worker_threads must be positive: %d", c.Server.WorkerThreads)
	}
	
	if len(c.Cluster.EntryPoints) == 0 {
		return fmt.Errorf("cluster entry_points cannot be empty")
	}
	
	if c.Pool.Size <= 0 {
		return fmt.Errorf("pool size must be positive: %d", c.Pool.Size)
	}
	
	if c.Pool.MinIdle < 0 || c.Pool.MinIdle > c.Pool.Size {
		return fmt.Errorf("invalid pool min_idle: %d", c.Pool.MinIdle)
	}
	
	if c.Pool.MaxIdle < c.Pool.MinIdle || c.Pool.MaxIdle > c.Pool.Size {
		return fmt.Errorf("invalid pool max_idle: %d", c.Pool.MaxIdle)
	}
	
	return nil
}