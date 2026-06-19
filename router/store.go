package router

import (
	"context"

	pucorarate "github.com/pucora/pucora-ratelimit/v3"
)

func StoreFromCfg(cfg Config) pucorarate.LimiterStore {
	ctx := context.Background()
	var storeBackend pucorarate.Backend
	if cfg.NumShards > 1 {
		storeBackend = pucorarate.NewShardedBackend(
			ctx,
			cfg.NumShards,
			cfg.TTL,
			cfg.CleanUpPeriod,
			1,
			pucorarate.PseudoFNV64a,
			pucorarate.MemoryBackendBuilder,
		)
	} else {
		storeBackend = pucorarate.MemoryBackendBuilder(ctx, cfg.TTL, cfg.CleanUpPeriod, 1, 1)[0]
	}

	return pucorarate.NewLimiterStore(cfg.ClientMaxRate, int(cfg.ClientCapacity),
		storeBackend)
}
