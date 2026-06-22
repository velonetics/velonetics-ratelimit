package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pucora/lura/v2/config"
	"github.com/pucora/lura/v2/logging"
	"github.com/redis/go-redis/v9"
)

const (
	namespaceRedis  = "redis"
	namespacePrefix = "qos/ratelimit/router/redis"
)

type Config struct {
	Addr            string  `json:"addr"`
	Password        string  `json:"password"`
	DB              int     `json:"db"`
	MaxRate         float64 `json:"max_rate"`
	Capacity        uint64  `json:"capacity"`
	Strategy        string  `json:"strategy"`
	Key             string  `json:"key"`
	ConnectionName  string  `json:"connection_name"`
	OnFailureAllow  bool    `json:"on_failure_allow"`
	Every           string  `json:"every"`
}

type ConnectionPoolConfig struct {
	Name            string        `json:"name"`
	Address         string        `json:"address"`
	Password        string        `json:"password"`
	DB              int           `json:"db"`
	PoolSize        int           `json:"pool_size"`
	MinIdleConns    int           `json:"min_idle_conns"`
	MaxIdleConns    int           `json:"max_idle_conns"`
	MaxActiveConns  int           `json:"max_active_conns"`
	ConnMaxIdleTime time.Duration `json:"conn_max_idle_time"`
	ConnMaxLifeTime time.Duration `json:"conn_max_life_time"`
	DialTimeout     time.Duration `json:"dial_timeout"`
	PoolTimeout     time.Duration `json:"pool_timeout"`
	MaxRetries      int           `json:"max_retries"`
	MinRetryBackoff time.Duration `json:"min_retry_backoff"`
	MaxRetryBackoff time.Duration `json:"max_retry_backoff"`
	TLS             *tls.Config   `json:"tls"`
}

type ClusterConfig struct {
	Name            string        `json:"name"`
	Addresses       []string      `json:"addresses"`
	Password        string        `json:"password"`
	PoolSize        int           `json:"pool_size"`
	MinIdleConns    int           `json:"min_idle_conns"`
	MaxRetries      int           `json:"max_retries"`
	MinRetryBackoff time.Duration `json:"min_retry_backoff"`
	MaxRetryBackoff time.Duration `json:"max_retry_backoff"`
	TLS             *tls.Config   `json:"tls"`
}

var ZeroCfg = Config{}

var (
	ErrNoExtraCfg    = fmt.Errorf("no extra config for namespace %s", namespacePrefix)
	ErrWrongExtraCfg  = fmt.Errorf("wrong extra config for namespace %s", namespacePrefix)
	ErrPoolNotFound  = fmt.Errorf("redis connection pool not found: %s")
)

var (
	poolManager  *PoolManager
	mu           sync.RWMutex
	tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'last_time')
local tokens = tonumber(bucket[1])
local last_time = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    last_time = now
end

local elapsed = (now - last_time) / 1000.0
local new_tokens = math.min(capacity, tokens + (elapsed * rate))

if new_tokens >= requested then
    new_tokens = new_tokens - requested
    redis.call('HMSET', key, 'tokens', new_tokens, 'last_time', now)
    redis.call('EXPIRE', key, math.ceil(capacity / rate) + 1)
    return 1
else
    redis.call('HMSET', key, 'tokens', new_tokens, 'last_time', now)
    redis.call('EXPIRE', key, math.ceil(capacity / rate) + 1)
    return 0
end
`)
)

type PoolManager struct {
	pools    map[string]*redis.Client
	clusters map[string]*redis.ClusterClient
	mu       sync.RWMutex
}

func NewPoolManager() *PoolManager {
	return &PoolManager{
		pools:    make(map[string]*redis.Client),
		clusters: make(map[string]*redis.ClusterClient),
	}
}

func (pm *PoolManager) RegisterPool(cfg ConnectionPoolConfig) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.pools[cfg.Name]; exists {
		return nil
	}

	opts := &redis.Options{
		Addr:            cfg.Address,
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		MaxActiveConns:  cfg.MaxActiveConns,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		ConnMaxLifetime: cfg.ConnMaxLifeTime,
		DialTimeout:     cfg.DialTimeout,
		PoolTimeout:     cfg.PoolTimeout,
		MaxRetries:      cfg.MaxRetries,
		MinRetryBackoff: cfg.MinRetryBackoff,
		MaxRetryBackoff: cfg.MaxRetryBackoff,
		TLSConfig:       cfg.TLS,
	}

	if opts.PoolSize == 0 {
		opts.PoolSize = 10
	}

	client := redis.NewClient(opts)
	pm.pools[cfg.Name] = client
	return nil
}

func (pm *PoolManager) RegisterCluster(cfg ClusterConfig) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.clusters[cfg.Name]; exists {
		return nil
	}

	opts := &redis.ClusterOptions{
		Addrs:           cfg.Addresses,
		Password:        cfg.Password,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		MaxRetries:      cfg.MaxRetries,
		MinRetryBackoff: cfg.MinRetryBackoff,
		MaxRetryBackoff: cfg.MaxRetryBackoff,
		TLSConfig:       cfg.TLS,
	}

	if opts.PoolSize == 0 {
		opts.PoolSize = 10
	}

	client := redis.NewClusterClient(opts)
	pm.clusters[cfg.Name] = client
	return nil
}

func (pm *PoolManager) GetPool(name string) (*redis.Client, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	client, ok := pm.pools[name]
	return client, ok
}

func (pm *PoolManager) GetCluster(name string) (*redis.ClusterClient, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	client, ok := pm.clusters[name]
	return client, ok
}

func SetPoolManager(pm *PoolManager) {
	mu.Lock()
	poolManager = pm
	mu.Unlock()
}

func GetPoolManager() *PoolManager {
	mu.RLock()
	defer mu.RUnlock()
	return poolManager
}

func ConfigGetter(e config.ExtraConfig) (Config, error) {
	v, ok := e[namespacePrefix]
	if !ok {
		return ZeroCfg, ErrNoExtraCfg
	}
	tmp, ok := v.(map[string]interface{})
	if !ok {
		return ZeroCfg, ErrWrongExtraCfg
	}

	cfg := Config{}

	if v, ok := tmp["addr"]; ok {
		cfg.Addr = fmt.Sprintf("%v", v)
	}
	if v, ok := tmp["password"]; ok {
		cfg.Password = fmt.Sprintf("%v", v)
	}
	if v, ok := tmp["db"]; ok {
		switch val := v.(type) {
		case int64:
			cfg.DB = int(val)
		case int:
			cfg.DB = val
		case float64:
			cfg.DB = int(val)
		}
	}
	if v, ok := tmp["max_rate"]; ok {
		switch val := v.(type) {
		case int64:
			cfg.MaxRate = float64(val)
		case int:
			cfg.MaxRate = float64(val)
		case float64:
			cfg.MaxRate = val
		}
	}
	if v, ok := tmp["capacity"]; ok {
		switch val := v.(type) {
		case int64:
			cfg.Capacity = uint64(val)
		case int:
			cfg.Capacity = uint64(val)
		case float64:
			cfg.Capacity = uint64(val)
		}
	}
	if v, ok := tmp["strategy"]; ok {
		cfg.Strategy = fmt.Sprintf("%v", v)
	}
	if v, ok := tmp["key"]; ok {
		cfg.Key = fmt.Sprintf("%v", v)
	}
	if v, ok := tmp["connection_name"]; ok {
		cfg.ConnectionName = fmt.Sprintf("%v", v)
	}
	if v, ok := tmp["on_failure_allow"]; ok {
		cfg.OnFailureAllow, _ = v.(bool)
	}
	if v, ok := tmp["every"]; ok {
		cfg.Every = fmt.Sprintf("%v", v)
	}

	return cfg, nil
}

func ParseRedisConfig(e config.ExtraConfig) (map[string]ConnectionPoolConfig, map[string]ClusterConfig, error) {
	pools := make(map[string]ConnectionPoolConfig)
	clusters := make(map[string]ClusterConfig)

	v, ok := e[namespaceRedis]
	if !ok {
		return pools, clusters, nil
	}

	tmp, ok := v.(map[string]interface{})
	if !ok {
		return pools, clusters, nil
	}

	if connectionPools, ok := tmp["connection_pools"].([]interface{}); ok {
		for _, item := range connectionPools {
			if itemMap, ok := item.(map[string]interface{}); ok {
				pool := parseConnectionPool(itemMap)
				if pool.Name != "" && pool.Address != "" {
					pools[pool.Name] = pool
				}
			}
		}
	}

	if clusterList, ok := tmp["clusters"].([]interface{}); ok {
		for _, item := range clusterList {
			if itemMap, ok := item.(map[string]interface{}); ok {
				cluster := parseClusterConfig(itemMap)
				if cluster.Name != "" && len(cluster.Addresses) > 0 {
					clusters[cluster.Name] = cluster
				}
			}
		}
	}

	return pools, clusters, nil
}

func parseConnectionPool(m map[string]interface{}) ConnectionPoolConfig {
	cfg := ConnectionPoolConfig{
		PoolSize:     10,
		MinIdleConns: 0,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
	}

	if v, ok := m["name"].(string); ok {
		cfg.Name = v
	}
	if v, ok := m["address"].(string); ok {
		cfg.Address = v
	}
	if v, ok := m["password"].(string); ok {
		cfg.Password = v
	}
	if v, ok := m["db"].(float64); ok {
		cfg.DB = int(v)
	}
	if v, ok := m["pool_size"].(float64); ok {
		cfg.PoolSize = int(v)
	}
	if v, ok := m["min_idle_conns"].(float64); ok {
		cfg.MinIdleConns = int(v)
	}
	if v, ok := m["max_idle_conns"].(float64); ok {
		cfg.MaxIdleConns = int(v)
	}
	if v, ok := m["max_active_conns"].(float64); ok {
		cfg.MaxActiveConns = int(v)
	}
	if v, ok := m["conn_max_idle_time"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.ConnMaxIdleTime = d
		}
	}
	if v, ok := m["conn_max_life_time"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.ConnMaxLifeTime = d
		}
	}
	if v, ok := m["dial_timeout"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.DialTimeout = d
		}
	}
	if v, ok := m["pool_timeout"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.PoolTimeout = d
		}
	}
	if v, ok := m["max_retries"].(float64); ok {
		cfg.MaxRetries = int(v)
	}
	if v, ok := m["min_retry_backoff"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.MinRetryBackoff = d
		}
	}
	if v, ok := m["max_retry_backoff"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.MaxRetryBackoff = d
		}
	}

	return cfg
}

func parseClusterConfig(m map[string]interface{}) ClusterConfig {
	cfg := ClusterConfig{
		PoolSize:     10,
		MinIdleConns: 0,
		MaxRetries:   3,
	}

	if v, ok := m["name"].(string); ok {
		cfg.Name = v
	}
	if v, ok := m["addresses"].([]interface{}); ok {
		cfg.Addresses = make([]string, len(v))
		for i, addr := range v {
			if s, ok := addr.(string); ok {
				cfg.Addresses[i] = s
			}
		}
	}
	if v, ok := m["password"].(string); ok {
		cfg.Password = v
	}
	if v, ok := m["pool_size"].(float64); ok {
		cfg.PoolSize = int(v)
	}
	if v, ok := m["min_idle_conns"].(float64); ok {
		cfg.MinIdleConns = int(v)
	}
	if v, ok := m["max_retries"].(float64); ok {
		cfg.MaxRetries = int(v)
	}
	if v, ok := m["min_retry_backoff"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.MinRetryBackoff = d
		}
	}
	if v, ok := m["max_retry_backoff"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.MaxRetryBackoff = d
		}
	}

	return cfg
}

func Middleware(cfg Config, logger logging.Logger) gin.HandlerFunc {
	return MiddlewareWithClient(cfg, nil, logger)
}

func MiddlewareWithClient(cfg Config, client *redis.Client, logger logging.Logger) gin.HandlerFunc {
	if cfg.MaxRate <= 0 {
		return func(c *gin.Context) { c.Next() }
	}

	capacity := cfg.Capacity
	if capacity == 0 {
		if cfg.MaxRate < 1 {
			capacity = 1
		} else {
			capacity = uint64(cfg.MaxRate)
		}
	}

	keyFunc := buildKeyFunc(cfg)

	var poolManager *PoolManager
	if cfg.ConnectionName != "" {
		poolManager = GetPoolManager()
	}

	logger.Debug(fmt.Sprintf(
		"[Redis] rate limit enabled. connection=%s strategy=%s maxRate=%f capacity=%d",
		cfg.ConnectionName, cfg.Strategy, cfg.MaxRate, capacity,
	))

	return func(c *gin.Context) {
		var redisClient *redis.Client

		if cfg.Addr != "" {
			redisClient = getInlineClient(cfg)
		} else if cfg.ConnectionName != "" && poolManager != nil {
			redisClient, _ = poolManager.GetPool(cfg.ConnectionName)
			if redisClient == nil {
				logger.Error("[Redis] pool not found: " + cfg.ConnectionName)
				if !cfg.OnFailureAllow {
					c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "rate limit service unavailable"})
					return
				}
				c.Next()
				return
			}
		} else {
			logger.Error("[Redis] no redis configuration found")
			if !cfg.OnFailureAllow {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "rate limit service unavailable"})
				return
			}
			c.Next()
			return
		}

		key := keyFunc(c)
		if key == "" {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}

		allowed, err := allow(redisClient, key, int64(capacity), cfg.MaxRate, logger)
		if err != nil {
			logger.Error("[Redis] allow error: " + err.Error())
			if !cfg.OnFailureAllow {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "rate limit service error"})
				return
			}
		}

		if !allowed {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}

		c.Next()
	}
}

func getInlineClient(cfg Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
}

func allow(client *redis.Client, key string, capacity int64, rate float64, logger logging.Logger) (bool, error) {
	ctx := context.Background()

	result, err := tokenBucketScript.Run(ctx, client, []string{key},
		capacity,
		rate,
		time.Now().UnixMilli(),
		1,
	).Int()

	if err != nil {
		return false, err
	}

	return result == 1, nil
}

func buildKeyFunc(cfg Config) func(*gin.Context) string {
	switch strings.ToLower(cfg.Strategy) {
	case "header":
		header := cfg.Key
		return func(c *gin.Context) string {
			return c.Request.Header.Get(header)
		}
	case "param":
		param := cfg.Key
		return func(c *gin.Context) string {
			return c.Param(param)
		}
	case "jwt":
		return func(c *gin.Context) string {
			return c.Request.Header.Get("Authorization")
		}
	default:
		return func(c *gin.Context) string {
			return c.ClientIP()
		}
	}
}