package router

import (
	"context"

	veloneticsrate "github.com/velonetics/velonetics-ratelimit/v3"
)

func StoreFromCfg(cfg Config) veloneticsrate.LimiterStore {
	ctx := context.Background()
	var storeBackend veloneticsrate.Backend
	if cfg.NumShards > 1 {
		storeBackend = veloneticsrate.NewShardedBackend(
			ctx,
			cfg.NumShards,
			cfg.TTL,
			cfg.CleanUpPeriod,
			1,
			veloneticsrate.PseudoFNV64a,
			veloneticsrate.MemoryBackendBuilder,
		)
	} else {
		storeBackend = veloneticsrate.MemoryBackendBuilder(ctx, cfg.TTL, cfg.CleanUpPeriod, 1, 1)[0]
	}

	return veloneticsrate.NewLimiterStore(cfg.ClientMaxRate, int(cfg.ClientCapacity),
		storeBackend)
}
