#!/bin/bash
# gomm2 migration test script
# Deploys gomm2 binary + config, then runs catch-up migration test

set -e

SOURCE_BROKERS="b-1.gomm2testsource.8pb7he.c23.kafka.us-east-1.amazonaws.com:9092,b-3.gomm2testsource.8pb7he.c23.kafka.us-east-1.amazonaws.com:9092,b-2.gomm2testsource.8pb7he.c23.kafka.us-east-1.amazonaws.com:9092"
TARGET_BROKERS="b-1.gomm2testtarget.vqtsux.c23.kafka.us-east-1.amazonaws.com:9092,b-3.gomm2testtarget.vqtsux.c23.kafka.us-east-1.amazonaws.com:9092,b-2.gomm2testtarget.vqtsux.c23.kafka.us-east-1.amazonaws.com:9092"

# Create gomm2 config
mkdir -p /opt/gomm2
cat > /opt/gomm2/config.yaml << EOF
clusters:
  source:
    bootstrap_servers:
      - "b-1.gomm2testsource.8pb7he.c23.kafka.us-east-1.amazonaws.com:9092"
      - "b-2.gomm2testsource.8pb7he.c23.kafka.us-east-1.amazonaws.com:9092"
      - "b-3.gomm2testsource.8pb7he.c23.kafka.us-east-1.amazonaws.com:9092"
  target:
    bootstrap_servers:
      - "b-1.gomm2testtarget.vqtsux.c23.kafka.us-east-1.amazonaws.com:9092"
      - "b-2.gomm2testtarget.vqtsux.c23.kafka.us-east-1.amazonaws.com:9092"
      - "b-3.gomm2testtarget.vqtsux.c23.kafka.us-east-1.amazonaws.com:9092"

replications:
  - source: source
    target: target
    enabled: true
    topic_filter:
      whitelist:
        - "perf-test-8p"
    replication_policy: "default"
    replication_factor: 2
    emit_heartbeats: true
    emit_checkpoints: true
    emit_offset_syncs: true
    refresh_topics_interval: "30s"
    refresh_groups_interval: "30s"
    heartbeat_interval: "1s"
    checkpoint_interval: "30s"
    producer_batch_size: 131072
    producer_linger_ms: 5
    consumer_poll_timeout: "1s"
    max_poll_records: 5000
    compression: "lz4"
    read_committed: false

metrics:
  enabled: true
  address: ":9090"

logging:
  level: "info"
  format: "json"
EOF

echo "Config written to /opt/gomm2/config.yaml"

# Verify source data before migration
echo ""
echo "=== PRE-MIGRATION: Source Topic Status ==="
/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list $SOURCE_BROKERS \
  --topic perf-test-8p --time -1 2>/dev/null | sort

echo ""
echo "=== Starting gomm2 catch-up migration ==="
echo "Start time: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"

# Run gomm2 with timestamps for performance measurement
/opt/gomm2/gomm2 --config /opt/gomm2/config.yaml 2>&1 | tee /tmp/gomm2-migration.log &
GOMM2_PID=$!
echo "gomm2 PID: $GOMM2_PID"

# Monitor loop: check target topic offsets every 30s
echo ""
echo "=== Migration Progress Monitor ==="
TOTAL_SOURCE_RECORDS=$(/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list $SOURCE_BROKERS --topic perf-test-8p --time -1 2>/dev/null | \
  awk -F: '{sum+=$3} END {print sum}')
echo "Total source records: $TOTAL_SOURCE_RECORDS"

START_TS=$(date +%s)
while kill -0 $GOMM2_PID 2>/dev/null; do
    sleep 30
    NOW_TS=$(date +%s)
    ELAPSED=$((NOW_TS - START_TS))

    # Get target topic offsets (replicated topic name: source.perf-test-8p)
    TARGET_RECORDS=$(/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
      --broker-list $TARGET_BROKERS --topic source.perf-test-8p --time -1 2>/dev/null | \
      awk -F: '{sum+=$3} END {print sum}' 2>/dev/null || echo "0")

    if [ -z "$TARGET_RECORDS" ] || [ "$TARGET_RECORDS" = "" ]; then
        TARGET_RECORDS=0
    fi

    PCT=0
    if [ "$TOTAL_SOURCE_RECORDS" -gt 0 ]; then
        PCT=$((TARGET_RECORDS * 100 / TOTAL_SOURCE_RECORDS))
    fi

    RATE=0
    if [ "$ELAPSED" -gt 0 ]; then
        RATE=$((TARGET_RECORDS / ELAPSED))
    fi

    echo "[${ELAPSED}s] Replicated: $TARGET_RECORDS / $TOTAL_SOURCE_RECORDS ($PCT%) | Rate: $RATE records/sec"

    # Check if migration is complete
    if [ "$TARGET_RECORDS" -ge "$TOTAL_SOURCE_RECORDS" ] && [ "$TARGET_RECORDS" -gt 0 ]; then
        END_TS=$(date +%s)
        TOTAL_TIME=$((END_TS - START_TS))
        echo ""
        echo "=== MIGRATION COMPLETE ==="
        echo "Total time: ${TOTAL_TIME}s ($(( TOTAL_TIME / 60 ))m $(( TOTAL_TIME % 60 ))s)"
        echo "Total records: $TARGET_RECORDS"
        echo "Avg rate: $((TARGET_RECORDS / TOTAL_TIME)) records/sec"
        echo "Avg throughput: $(( TARGET_RECORDS * 3750 / TOTAL_TIME / 1048576 )) MB/s"
        echo "End time: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"

        # Check metrics endpoint
        echo ""
        echo "=== gomm2 Metrics ==="
        curl -s http://localhost:9090/metrics 2>/dev/null | grep -E "^gomm2_(records|bytes|replication)" | head -20

        # Stop gomm2
        kill $GOMM2_PID 2>/dev/null
        wait $GOMM2_PID 2>/dev/null
        break
    fi
done

echo ""
echo "=== POST-MIGRATION: Verification ==="
echo "Source offsets:"
/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list $SOURCE_BROKERS --topic perf-test-8p --time -1 2>/dev/null | sort
echo ""
echo "Target offsets:"
/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list $TARGET_BROKERS --topic source.perf-test-8p --time -1 2>/dev/null | sort
