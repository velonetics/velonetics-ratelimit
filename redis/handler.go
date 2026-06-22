package redis

import (
	"github.com/gin-gonic/gin"
	"github.com/pucora/lura/v2/config"
	"github.com/pucora/lura/v2/logging"
	"github.com/pucora/lura/v2/proxy"
	router "github.com/pucora/lura/v2/router/gin"
)

func HandlerFactory(logger logging.Logger, next router.HandlerFactory, serviceCfg *config.ServiceConfig) router.HandlerFactory {
	if serviceCfg != nil {
		pools, clusters, err := ParseRedisConfig(serviceCfg.ExtraConfig)
		if err != nil {
			logger.Error("[Redis] failed to parse redis config: " + err.Error())
		} else {
			pm := NewPoolManager()
			for _, cfg := range pools {
				if err := pm.RegisterPool(cfg); err != nil {
					logger.Error("[Redis] failed to register pool "+cfg.Name+": " + err.Error())
				}
			}
			for _, cfg := range clusters {
				if err := pm.RegisterCluster(cfg); err != nil {
					logger.Error("[Redis] failed to register cluster "+cfg.Name+": " + err.Error())
				}
			}
			if len(pools) > 0 || len(clusters) > 0 {
				SetPoolManager(pm)
				logger.Info("[Redis] initialized " + itoa(len(pools)) + " pools and " + itoa(len(clusters)) + " clusters")
			}
		}
	}

	return func(cfg *config.EndpointConfig, p proxy.Proxy) gin.HandlerFunc {
		redisCfg, err := ConfigGetter(cfg.ExtraConfig)
		if err != nil {
			return next(cfg, p)
		}
		mw := MiddlewareWithClient(redisCfg, nil, logger)
		h := next(cfg, p)
		return func(c *gin.Context) {
			mw(c)
			if !c.IsAborted() {
				h(c)
			}
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	result := ""
	for i > 0 {
		result = string(rune('0'+i%10)) + result
		i /= 10
	}
	return result
}