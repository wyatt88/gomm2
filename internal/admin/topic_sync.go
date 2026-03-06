// Package admin — topic_sync.go implements topic configuration synchronization.
package admin

import (
	"context"
	"fmt"
	"log/slog"
)

// TopicConfigDiff represents the difference between source and target topic configs.
type TopicConfigDiff struct {
	Topic   string
	ToSet   map[string]*string // configs to set or update
	ToReset []string           // configs to delete (reset to default)
}

// SyncTopicConfigs reads config entries from the source topic and compares them
// with the target topic. It returns a diff that can be applied to bring the
// target in line with the source, filtering through allowedProps (if non-nil).
func SyncTopicConfigs(
	ctx context.Context,
	srcAdmin *Client,
	tgtAdmin *Client,
	srcTopic, tgtTopic string,
	allowedProps map[string]bool,
	logger *slog.Logger,
) error {
	srcConfigs, err := srcAdmin.DescribeTopicConfigs(ctx, srcTopic)
	if err != nil {
		return fmt.Errorf("describe source topic %s configs: %w", srcTopic, err)
	}
	tgtConfigs, err := tgtAdmin.DescribeTopicConfigs(ctx, tgtTopic)
	if err != nil {
		return fmt.Errorf("describe target topic %s configs: %w", tgtTopic, err)
	}

	toApply := make(map[string]*string)
	for k, v := range srcConfigs {
		if allowedProps != nil && !allowedProps[k] {
			continue
		}
		tgtVal, exists := tgtConfigs[k]
		if !exists || tgtVal != v {
			val := v // copy
			toApply[k] = &val
		}
	}

	if len(toApply) == 0 {
		return nil
	}

	logger.Info("syncing topic configs",
		"source_topic", srcTopic,
		"target_topic", tgtTopic,
		"configs_count", len(toApply),
	)

	if err := tgtAdmin.AlterTopicConfigs(ctx, tgtTopic, toApply); err != nil {
		return fmt.Errorf("alter target topic %s configs: %w", tgtTopic, err)
	}

	return nil
}
