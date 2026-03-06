package mirror

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gomm2/gomm2/internal/config"
	"github.com/gomm2/gomm2/internal/kafka"
	"github.com/gomm2/gomm2/internal/metrics"
	"github.com/gomm2/gomm2/internal/policy"
	"github.com/gomm2/gomm2/pkg/types"
)

// Heartbeat emits heartbeat records to the target cluster at regular intervals.
// Corresponds to Java MM2's MirrorHeartbeatConnector + MirrorHeartbeatTask.
type Heartbeat struct {
	cfg    config.ReplicationConfig
	tgtCfg config.ClusterConfig
	policy policy.ReplicationPolicy
	m      *metrics.Metrics
	logger *slog.Logger

	producer *kgo.Client

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewHeartbeat creates a new Heartbeat emitter.
func NewHeartbeat(
	cfg config.ReplicationConfig,
	tgtCfg config.ClusterConfig,
	m *metrics.Metrics,
	logger *slog.Logger,
) (*Heartbeat, error) {
	pol, err := policy.NewPolicy(cfg.ReplicationPolicy, cfg.Separator)
	if err != nil {
		return nil, fmt.Errorf("create replication policy: %w", err)
	}
	return &Heartbeat{
		cfg:    cfg,
		tgtCfg: tgtCfg,
		policy: pol,
		m:      m,
		logger: logger.With("component", "heartbeat", "flow", cfg.Source+"->"+cfg.Target),
	}, nil
}

// Start begins emitting heartbeats.
func (h *Heartbeat) Start(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.running {
		return fmt.Errorf("heartbeat already running")
	}
	if !config.BoolDefault(h.cfg.EmitHeartbeats, true) {
		h.logger.Info("heartbeat emission disabled")
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel

	// Build target producer using shared builder
	opts, err := kafka.BuildClientOpts(h.tgtCfg)
	if err != nil {
		cancel()
		return fmt.Errorf("build target client opts: %w", err)
	}
	opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))

	producer, err := kgo.NewClient(opts...)
	if err != nil {
		cancel()
		return fmt.Errorf("create heartbeat producer: %w", err)
	}
	h.producer = producer
	h.running = true

	h.wg.Add(1)
	go h.emitLoop(ctx)

	h.logger.Info("heartbeat emission started", "interval", h.cfg.HeartbeatInterval.Duration)
	return nil
}

// Stop shuts down the heartbeat emitter.
func (h *Heartbeat) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.running {
		return
	}
	h.cancel()
	h.wg.Wait()
	if h.producer != nil {
		h.producer.Close()
	}
	h.running = false
	h.logger.Info("heartbeat emission stopped")
}

func (h *Heartbeat) emitLoop(ctx context.Context) {
	defer h.wg.Done()
	ticker := time.NewTicker(h.cfg.HeartbeatInterval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.emitHeartbeat(ctx)
		}
	}
}

func (h *Heartbeat) emitHeartbeat(ctx context.Context) {
	now := time.Now()
	hb := types.Heartbeat{
		SourceCluster: h.cfg.Source,
		TargetCluster: h.cfg.Target,
		Timestamp:     now,
	}

	record := &kgo.Record{
		Topic: h.policy.HeartbeatsTopic(),
		Key:   hb.SerializeKey(),
		Value: hb.SerializeValue(),
	}

	h.producer.Produce(ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			h.logger.Error("failed to emit heartbeat", "err", err)
			return
		}
		latency := time.Since(now)
		h.m.HeartbeatLatency.WithLabelValues(h.cfg.Source, h.cfg.Target).Observe(latency.Seconds())
		h.logger.Debug("heartbeat emitted", "latency", latency)
	})
}
