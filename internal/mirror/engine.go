package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gomm2/gomm2/internal/admin"
	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/metrics"
	"github.com/gomm2/gomm2/internal/offset"
	"github.com/gomm2/gomm2/internal/policy"
)

// Engine orchestrates all replication flows defined in the configuration.
// It manages Source, Heartbeat, Checkpoint, TopicDiscovery, and offset-sync
// persistence components for each enabled replication.
type Engine struct {
	cfg     *config.Config
	metrics *metrics.Metrics
	logger  *slog.Logger

	sources          []*Source
	sourcesByFlow    map[string]*Source              // keyed by "source->target"
	heartbeats       []*Heartbeat
	checkpoints      []*Checkpoint
	topicDiscoveries []*TopicDiscovery
	syncStores       map[string]*offset.SyncStore   // keyed by "source->target"
	syncReaders      []*offset.SyncReader
	syncWriters      []*offset.SyncWriter
	dlqs             []*DLQ

	httpServer *http.Server

	mu      sync.Mutex
	running bool
}

// NewEngine creates a new replication engine from config.
func NewEngine(cfg *config.Config) (*Engine, error) {
	// Setup logger
	var handler slog.Handler
	level := parseLogLevel(cfg.Logging.Level)
	opts := &slog.HandlerOptions{Level: level}
	if cfg.Logging.Format == "console" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}
	logger := slog.New(handler)

	m := metrics.New()

	e := &Engine{
		cfg:           cfg,
		metrics:       m,
		logger:        logger,
		syncStores:    make(map[string]*offset.SyncStore),
		sourcesByFlow: make(map[string]*Source),
	}

	// Initialize components for each enabled replication
	for _, r := range cfg.Replications {
		if !r.Enabled {
			continue
		}
		srcCfg, ok := cfg.Clusters[r.Source]
		if !ok {
			return nil, fmt.Errorf("source cluster %q not defined", r.Source)
		}
		tgtCfg, ok := cfg.Clusters[r.Target]
		if !ok {
			return nil, fmt.Errorf("target cluster %q not defined", r.Target)
		}

		flowKey := r.Source + "->" + r.Target
		syncStore := offset.NewSyncStore()
		e.syncStores[flowKey] = syncStore

		// Source replicator
		src, err := NewSource(r, srcCfg, tgtCfg, m, syncStore, logger)
		if err != nil {
			return nil, fmt.Errorf("create source for %s: %w", flowKey, err)
		}
		e.sources = append(e.sources, src)
		e.sourcesByFlow[flowKey] = src

		// DLQ
		dlq, err := NewDLQ(r, tgtCfg, m, logger)
		if err != nil {
			return nil, fmt.Errorf("create DLQ for %s: %w", flowKey, err)
		}
		if dlq != nil {
			src.SetDLQ(dlq)
			e.dlqs = append(e.dlqs, dlq)
		}

		// Heartbeat emitter
		hb, err := NewHeartbeat(r, tgtCfg, m, logger)
		if err != nil {
			return nil, fmt.Errorf("create heartbeat for %s: %w", flowKey, err)
		}
		e.heartbeats = append(e.heartbeats, hb)

		// Checkpoint emitter
		cp, err := NewCheckpoint(r, srcCfg, tgtCfg, syncStore, m, logger)
		if err != nil {
			return nil, fmt.Errorf("create checkpoint for %s: %w", flowKey, err)
		}
		e.checkpoints = append(e.checkpoints, cp)

		// Topic discovery
		td, err := NewTopicDiscovery(r, srcCfg, tgtCfg, m, src, logger)
		if err != nil {
			return nil, fmt.Errorf("create topic discovery for %s: %w", flowKey, err)
		}
		e.topicDiscoveries = append(e.topicDiscoveries, td)
	}

	return e, nil
}

// Run starts all components and blocks until shutdown signal is received.
func (e *Engine) Run(ctx context.Context) error {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return fmt.Errorf("engine already running")
	}
	e.running = true
	e.mu.Unlock()

	e.logger.Info("gomm2 engine starting",
		"replications", len(e.sources),
		"metrics_address", e.cfg.Metrics.Address,
	)

	// Start metrics server
	if e.cfg.Metrics.Enabled {
		e.startMetricsServer()
	}

	// Phase 1: Create offset-syncs internal topics and bootstrap sync stores
	if err := e.bootstrapOffsetSyncs(ctx); err != nil {
		return fmt.Errorf("bootstrap offset syncs: %w", err)
	}

	// Phase 2: Parallel catch-up for high-lag partitions (before normal source starts)
	for _, r := range e.cfg.Replications {
		if !r.Enabled {
			continue
		}
		srcCfg := e.cfg.Clusters[r.Source]
		tgtCfg := e.cfg.Clusters[r.Target]
		group := "gomm2-source-" + r.Source + "-" + r.Target

		// Check lag for all known topics (via a quick admin list)
		adminClient, err := admin.NewClient(ctx, srcCfg)
		if err != nil {
			e.logger.Warn("skip parallel catch-up: admin client error", "err", err)
			continue
		}
		topics, err := adminClient.ListTopics(ctx)
		adminClient.Close()
		if err != nil {
			e.logger.Warn("skip parallel catch-up: list topics error", "err", err)
			continue
		}

		const parallelThreshold = 100000 // only parallelize if lag > 100K records

		// Smart worker allocation:
		// - Total memory budget for fetch buffers: ~2GB
		// - Each worker uses ~5MB FetchMaxPartitionBytes + overhead ≈ 10MB
		// - 3 brokers × 188 MB/s = 564 MB/s source limit
		// - With 30ms RTT and 5MB fetch per worker: ~166 MB/s theoretical per conn
		// - Need ~564 / 166 ≈ 4 workers minimum, use 6 for headroom
		// - 6 workers × 8 partitions = 48 total workers ≈ 480MB fetch buffers
		const workersPerPartition = 6

		for _, topic := range topics {
			// Skip internal topics
			if len(topic) > 0 && (topic[0] == '_' || topic[0] == '.') {
				continue
			}

			// Get partition count for this topic
			topicAdm, err := admin.NewClient(ctx, srcCfg)
			if err != nil {
				continue
			}
			partitions, err := topicAdm.ListTopicPartitions(ctx, topic)
			topicAdm.Close()
			if err != nil {
				partitions = []int32{0}
			}

			// Collect lags for all partitions
			partitionLags := make(map[int32][2]int64) // partition -> [committed, hwm]
			for _, part := range partitions {
				committed, hwm, err := GetPartitionLag(ctx, srcCfg, group, topic, part)
				if err != nil {
					continue
				}
				lag := hwm - committed
				if committed <= 0 {
					lag = hwm
					committed = 0
				}
				if lag > parallelThreshold {
					partitionLags[part] = [2]int64{committed, hwm}
					e.logger.Info("partition has high lag",
						"topic", topic, "partition", part,
						"committed", committed, "hwm", hwm, "lag", lag,
					)
				}
			}

			if len(partitionLags) > 0 {
				ps, err := NewParallelSource(r, srcCfg, tgtCfg, e.metrics, workersPerPartition, e.logger)
				if err != nil {
					e.logger.Error("failed to create parallel source", "err", err)
					continue
				}
				// All partitions catch up simultaneously
				if err := ps.CatchUpMultiPartition(ctx, partitionLags, topic); err != nil {
					e.logger.Error("parallel catch-up failed", "topic", topic, "err", err)
				}
			}
		}
	}

	// Phase 3: Start all replication sources (creates consumer clients)
	for _, src := range e.sources {
		if err := src.Start(ctx); err != nil {
			return fmt.Errorf("start source: %w", err)
		}
	}

	// Phase 3: Start topic discovery (discovers topics and subscribes the consumer)
	for _, td := range e.topicDiscoveries {
		if err := td.Start(ctx); err != nil {
			return fmt.Errorf("start topic discovery: %w", err)
		}
	}

	// Phase 4: Start heartbeats
	for _, hb := range e.heartbeats {
		if err := hb.Start(ctx); err != nil {
			return fmt.Errorf("start heartbeat: %w", err)
		}
	}

	// Phase 5: Start checkpoints (sync stores should be ready by now)
	for _, cp := range e.checkpoints {
		if err := cp.Start(ctx); err != nil {
			return fmt.Errorf("start checkpoint: %w", err)
		}
	}

	e.logger.Info("gomm2 engine started — all components running")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		e.logger.Info("received signal, shutting down", "signal", sig)
	case <-ctx.Done():
		e.logger.Info("context cancelled, shutting down")
	}

	e.Shutdown()
	return nil
}

// bootstrapOffsetSyncs creates offset-syncs internal topics if needed,
// reads existing offset sync data to populate the sync stores, then
// starts background followers for real-time updates.
func (e *Engine) bootstrapOffsetSyncs(ctx context.Context) error {
	for _, r := range e.cfg.Replications {
		if !r.Enabled || !config.BoolDefault(r.EmitOffsetSyncs, true) {
			continue
		}

		srcCfg := e.cfg.Clusters[r.Source]
		flowKey := r.Source + "->" + r.Target
		syncStore := e.syncStores[flowKey]

		// Determine the offset-syncs topic name using the replication policy
		pol, err := policy.NewPolicy(r.ReplicationPolicy, r.Separator)
		if err != nil {
			return fmt.Errorf("create policy for %s: %w", flowKey, err)
		}
		syncTopic := pol.OffsetSyncsTopic(r.Source)

		// Create the offset-syncs topic if it doesn't exist
		e.logger.Info("ensuring offset-syncs topic exists",
			"topic", syncTopic, "flow", flowKey)
		srcAdmin, err := admin.NewClient(ctx, srcCfg)
		if err != nil {
			return fmt.Errorf("create admin for offset-syncs topic %s: %w", flowKey, err)
		}
		_ = srcAdmin.CreateCompactedTopic(ctx, syncTopic, r.ReplicationFactor)
		srcAdmin.Close()

		// Read the offset-syncs topic to populate the sync store
		reader, err := offset.NewSyncReader(ctx, srcCfg, syncTopic, syncStore, e.logger)
		if err != nil {
			return fmt.Errorf("create sync reader for %s: %w", flowKey, err)
		}

		readTimeout := 30 * time.Second
		if err := reader.ReadToEnd(ctx, readTimeout); err != nil {
			reader.Close()
			return fmt.Errorf("read offset-syncs to end for %s: %w", flowKey, err)
		}

		// Start background follower
		go reader.Follow(ctx)
		e.syncReaders = append(e.syncReaders, reader)

		// Create a sync writer for this flow
		writer, err := offset.NewSyncWriter(ctx, srcCfg, syncTopic, e.logger)
		if err != nil {
			return fmt.Errorf("create sync writer for %s: %w", flowKey, err)
		}
		e.syncWriters = append(e.syncWriters, writer)

		// Wire the sync writer into the source so it can persist offset syncs
		if src, ok := e.sourcesByFlow[flowKey]; ok {
			src.SetSyncWriter(writer)
		}

	}

	return nil
}

// Shutdown gracefully stops all components.
func (e *Engine) Shutdown() {
	e.logger.Info("gomm2 engine shutting down")

	// Stop in reverse order: checkpoints, heartbeats, topic discovery, sources
	for _, cp := range e.checkpoints {
		cp.Stop()
	}
	for _, hb := range e.heartbeats {
		hb.Stop()
	}
	for _, td := range e.topicDiscoveries {
		td.Stop()
	}
	for _, src := range e.sources {
		src.Stop()
	}

	// Close offset-sync readers and writers
	for _, sr := range e.syncReaders {
		sr.Close()
	}
	for _, sw := range e.syncWriters {
		sw.Close()
	}

	// Close DLQ producers
	for _, d := range e.dlqs {
		d.Close()
	}

	if e.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.httpServer.Shutdown(ctx)
	}

	e.mu.Lock()
	e.running = false
	e.mu.Unlock()

	e.logger.Info("gomm2 engine stopped")
}

func (e *Engine) startMetricsServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		// Check if all sync stores are ready
		ready := true
		for _, ss := range e.syncStores {
			if !ss.IsReady() {
				ready = false
				break
			}
		}
		if ready {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("not ready"))
		}
	})

	e.httpServer = &http.Server{
		Addr:    e.cfg.Metrics.Address,
		Handler: mux,
	}
	go func() {
		e.logger.Info("metrics server starting", "address", e.cfg.Metrics.Address)
		if err := e.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			e.logger.Error("metrics server error", "err", err)
		}
	}()
}

func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
