package types

import (
	"math"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// TopicPartition
// ---------------------------------------------------------------------------

func TestTopicPartitionString(t *testing.T) {
	tests := []struct {
		tp   TopicPartition
		want string
	}{
		{TopicPartition{Topic: "orders", Partition: 0}, "orders-0"},
		{TopicPartition{Topic: "events", Partition: 42}, "events-42"},
		{TopicPartition{Topic: "", Partition: 0}, "-0"},
		{TopicPartition{Topic: "a.b.c", Partition: 99}, "a.b.c-99"},
		{TopicPartition{Topic: "topic", Partition: math.MaxInt32}, "topic-2147483647"},
	}
	for _, tt := range tests {
		got := tt.tp.String()
		if got != tt.want {
			t.Errorf("TopicPartition%+v.String() = %q, want %q", tt.tp, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// OffsetSync serialization round-trip
// ---------------------------------------------------------------------------

func TestOffsetSyncRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		os   OffsetSync
	}{
		{
			name: "normal",
			os: OffsetSync{
				TopicPartition:   TopicPartition{Topic: "orders", Partition: 5},
				UpstreamOffset:   1000,
				DownstreamOffset: 500,
			},
		},
		{
			name: "zero offsets",
			os: OffsetSync{
				TopicPartition:   TopicPartition{Topic: "test", Partition: 0},
				UpstreamOffset:   0,
				DownstreamOffset: 0,
			},
		},
		{
			name: "max int64 offsets",
			os: OffsetSync{
				TopicPartition:   TopicPartition{Topic: "big", Partition: 1},
				UpstreamOffset:   math.MaxInt64,
				DownstreamOffset: math.MaxInt64,
			},
		},
		{
			name: "empty topic",
			os: OffsetSync{
				TopicPartition:   TopicPartition{Topic: "", Partition: 0},
				UpstreamOffset:   42,
				DownstreamOffset: 21,
			},
		},
		{
			name: "long topic name",
			os: OffsetSync{
				TopicPartition:   TopicPartition{Topic: strings.Repeat("a", 500), Partition: 3},
				UpstreamOffset:   99,
				DownstreamOffset: 88,
			},
		},
		{
			name: "max partition",
			os: OffsetSync{
				TopicPartition:   TopicPartition{Topic: "t", Partition: math.MaxInt32},
				UpstreamOffset:   10,
				DownstreamOffset: 5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := tt.os.SerializeKey()
			value := tt.os.SerializeValue()

			got, err := DeserializeOffsetSync(key, value)
			if err != nil {
				t.Fatalf("DeserializeOffsetSync: %v", err)
			}
			if got.TopicPartition != tt.os.TopicPartition {
				t.Errorf("TopicPartition = %v, want %v", got.TopicPartition, tt.os.TopicPartition)
			}
			if got.UpstreamOffset != tt.os.UpstreamOffset {
				t.Errorf("UpstreamOffset = %d, want %d", got.UpstreamOffset, tt.os.UpstreamOffset)
			}
			if got.DownstreamOffset != tt.os.DownstreamOffset {
				t.Errorf("DownstreamOffset = %d, want %d", got.DownstreamOffset, tt.os.DownstreamOffset)
			}
		})
	}
}

func TestOffsetSyncString(t *testing.T) {
	os := OffsetSync{
		TopicPartition:   TopicPartition{Topic: "orders", Partition: 0},
		UpstreamOffset:   100,
		DownstreamOffset: 50,
	}
	s := os.String()
	if !strings.Contains(s, "orders-0") {
		t.Errorf("String() = %q, should contain topic-partition", s)
	}
	if !strings.Contains(s, "upstream=100") {
		t.Errorf("String() = %q, should contain upstream offset", s)
	}
}

func TestDeserializeOffsetSyncErrors(t *testing.T) {
	tests := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{"key too short", []byte{0, 1, 2}, make([]byte, 16)},
		{"key length mismatch", []byte{0, 100, 'a', 'b', 0, 0}, make([]byte, 16)},
		{"value too short", []byte{0, 1, 'a', 0, 0, 0, 0}, make([]byte, 8)},
		{"empty key", []byte{}, make([]byte, 16)},
		{"nil key", nil, make([]byte, 16)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DeserializeOffsetSync(tt.key, tt.value)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Heartbeat serialization
// ---------------------------------------------------------------------------

func TestHeartbeatSerializeKey(t *testing.T) {
	hb := Heartbeat{
		SourceCluster: "us-west",
		TargetCluster: "eu-central",
		Timestamp:     time.Now(),
	}
	key := hb.SerializeKey()
	// key format: 2-byte len + src + 2-byte len + tgt
	expectedLen := 2 + len("us-west") + 2 + len("eu-central")
	if len(key) != expectedLen {
		t.Errorf("key length = %d, want %d", len(key), expectedLen)
	}
}

func TestHeartbeatSerializeValue(t *testing.T) {
	hb := Heartbeat{
		SourceCluster: "a",
		TargetCluster: "b",
		Timestamp:     time.Now(),
	}
	value := hb.SerializeValue()
	// value format: version (2 bytes) + timestamp (8 bytes)
	if len(value) != 10 {
		t.Errorf("value length = %d, want 10", len(value))
	}
}

func TestHeartbeatString(t *testing.T) {
	hb := Heartbeat{
		SourceCluster: "src",
		TargetCluster: "tgt",
		Timestamp:     time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC),
	}
	s := hb.String()
	if !strings.Contains(s, "src") || !strings.Contains(s, "tgt") {
		t.Errorf("String() = %q, missing cluster names", s)
	}
	if !strings.Contains(s, "2025") {
		t.Errorf("String() = %q, missing timestamp", s)
	}
}

func TestHeartbeatEmptyClusters(t *testing.T) {
	hb := Heartbeat{
		SourceCluster: "",
		TargetCluster: "",
		Timestamp:     time.Unix(0, 0),
	}
	key := hb.SerializeKey()
	// Should serialize successfully with empty strings
	if len(key) != 4 { // 2+0+2+0
		t.Errorf("empty cluster key length = %d, want 4", len(key))
	}
}

func TestHeartbeatZeroTimestamp(t *testing.T) {
	hb := Heartbeat{
		SourceCluster: "a",
		TargetCluster: "b",
		Timestamp:     time.Time{}, // zero value
	}
	value := hb.SerializeValue()
	if len(value) != 10 {
		t.Errorf("value length = %d, want 10", len(value))
	}
}

// ---------------------------------------------------------------------------
// Checkpoint serialization round-trip
// ---------------------------------------------------------------------------

func TestCheckpointSerializeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		cp   Checkpoint
	}{
		{
			name: "normal",
			cp: Checkpoint{
				ConsumerGroupID:  "my-group",
				TopicPartition:   TopicPartition{Topic: "orders", Partition: 3},
				UpstreamOffset:   1000,
				DownstreamOffset: 500,
				Metadata:         "some-metadata",
			},
		},
		{
			name: "empty group and metadata",
			cp: Checkpoint{
				ConsumerGroupID:  "",
				TopicPartition:   TopicPartition{Topic: "", Partition: 0},
				UpstreamOffset:   0,
				DownstreamOffset: 0,
				Metadata:         "",
			},
		},
		{
			name: "max offsets",
			cp: Checkpoint{
				ConsumerGroupID:  "group",
				TopicPartition:   TopicPartition{Topic: "t", Partition: 1},
				UpstreamOffset:   math.MaxInt64,
				DownstreamOffset: math.MaxInt64,
				Metadata:         "",
			},
		},
		{
			name: "long metadata",
			cp: Checkpoint{
				ConsumerGroupID:  "g",
				TopicPartition:   TopicPartition{Topic: "t", Partition: 0},
				UpstreamOffset:   1,
				DownstreamOffset: 1,
				Metadata:         strings.Repeat("x", 1000),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := tt.cp.SerializeKey()
			value := tt.cp.SerializeValue()

			// Verify key has correct length: 2+len(group)+2+len(topic)+4
			expectedKeyLen := 2 + len(tt.cp.ConsumerGroupID) + 2 + len(tt.cp.TopicPartition.Topic) + 4
			if len(key) != expectedKeyLen {
				t.Errorf("key length = %d, want %d", len(key), expectedKeyLen)
			}

			// Verify value has correct length: 2+8+8+2+len(metadata)
			expectedValueLen := 2 + 8 + 8 + 2 + len(tt.cp.Metadata)
			if len(value) != expectedValueLen {
				t.Errorf("value length = %d, want %d", len(value), expectedValueLen)
			}
		})
	}
}

func TestCheckpointString(t *testing.T) {
	cp := Checkpoint{
		ConsumerGroupID:  "test-group",
		TopicPartition:   TopicPartition{Topic: "events", Partition: 7},
		UpstreamOffset:   100,
		DownstreamOffset: 50,
	}
	s := cp.String()
	if !strings.Contains(s, "test-group") {
		t.Errorf("String() = %q, missing group", s)
	}
	if !strings.Contains(s, "events-7") {
		t.Errorf("String() = %q, missing tp", s)
	}
}

// ---------------------------------------------------------------------------
// SourceAndTarget
// ---------------------------------------------------------------------------

func TestSourceAndTargetString(t *testing.T) {
	st := SourceAndTarget{Source: "dc1", Target: "dc2"}
	got := st.String()
	want := "dc1->dc2"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestSourceAndTargetEmptyFields(t *testing.T) {
	st := SourceAndTarget{Source: "", Target: ""}
	got := st.String()
	want := "->"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
