package offset

import (
	"testing"

	"github.com/gomm2/gomm2/pkg/types"
)

// TestSyncWriterSerializationRoundTrip verifies that offset sync records
// can be serialized and deserialized correctly — the core invariant that
// SyncWriter and SyncReader depend on.
func TestSyncWriterSerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		os       types.OffsetSync
	}{
		{
			name: "simple",
			os: types.OffsetSync{
				TopicPartition:   types.TopicPartition{Topic: "orders", Partition: 0},
				UpstreamOffset:   1000,
				DownstreamOffset: 500,
			},
		},
		{
			name: "high_offsets",
			os: types.OffsetSync{
				TopicPartition:   types.TopicPartition{Topic: "events", Partition: 42},
				UpstreamOffset:   1<<40 + 123,
				DownstreamOffset: 1<<40 + 99,
			},
		},
		{
			name: "zero_offsets",
			os: types.OffsetSync{
				TopicPartition:   types.TopicPartition{Topic: "test", Partition: 0},
				UpstreamOffset:   0,
				DownstreamOffset: 0,
			},
		},
		{
			name: "long_topic_name",
			os: types.OffsetSync{
				TopicPartition:   types.TopicPartition{Topic: "my.very.long.topic.name.with.dots.and-dashes", Partition: 99},
				UpstreamOffset:   42,
				DownstreamOffset: 24,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := tt.os.SerializeKey()
			value := tt.os.SerializeValue()

			got, err := types.DeserializeOffsetSync(key, value)
			if err != nil {
				t.Fatalf("DeserializeOffsetSync: %v", err)
			}
			if got.TopicPartition.Topic != tt.os.TopicPartition.Topic {
				t.Errorf("topic = %q, want %q", got.TopicPartition.Topic, tt.os.TopicPartition.Topic)
			}
			if got.TopicPartition.Partition != tt.os.TopicPartition.Partition {
				t.Errorf("partition = %d, want %d", got.TopicPartition.Partition, tt.os.TopicPartition.Partition)
			}
			if got.UpstreamOffset != tt.os.UpstreamOffset {
				t.Errorf("upstream = %d, want %d", got.UpstreamOffset, tt.os.UpstreamOffset)
			}
			if got.DownstreamOffset != tt.os.DownstreamOffset {
				t.Errorf("downstream = %d, want %d", got.DownstreamOffset, tt.os.DownstreamOffset)
			}
		})
	}
}

// TestSyncReaderStoreIntegration verifies that deserialized offset syncs
// are correctly handled by the SyncStore, simulating what SyncReader does.
func TestSyncReaderStoreIntegration(t *testing.T) {
	store := NewSyncStore()

	// Simulate reading several offset-sync records from the topic
	records := []types.OffsetSync{
		{TopicPartition: types.TopicPartition{Topic: "t1", Partition: 0}, UpstreamOffset: 100, DownstreamOffset: 50},
		{TopicPartition: types.TopicPartition{Topic: "t1", Partition: 0}, UpstreamOffset: 200, DownstreamOffset: 150},
		{TopicPartition: types.TopicPartition{Topic: "t1", Partition: 1}, UpstreamOffset: 50, DownstreamOffset: 25},
	}

	for _, os := range records {
		// Simulate the serialize→deserialize round-trip
		key := os.SerializeKey()
		value := os.SerializeValue()
		got, err := types.DeserializeOffsetSync(key, value)
		if err != nil {
			t.Fatalf("deserialize: %v", err)
		}
		store.HandleSync(got)
	}

	// Before marking read-to-end, translations should be unavailable
	_, ok := store.TranslateDownstream(types.TopicPartition{Topic: "t1", Partition: 0}, 150)
	if ok {
		t.Error("expected translation to be unavailable before MarkReadToEnd")
	}

	// Mark as read-to-end (simulating ReadToEnd completion)
	store.MarkReadToEnd()

	// Now translations should work
	got, ok := store.TranslateDownstream(types.TopicPartition{Topic: "t1", Partition: 0}, 200)
	if !ok {
		t.Fatal("expected translation to succeed after MarkReadToEnd")
	}
	if got != 150 {
		t.Errorf("expected 150, got %d", got)
	}

	got, ok = store.TranslateDownstream(types.TopicPartition{Topic: "t1", Partition: 1}, 50)
	if !ok {
		t.Fatal("expected translation to succeed for t1-1")
	}
	if got != 25 {
		t.Errorf("expected 25, got %d", got)
	}
}

// TestDeserializeOffsetSyncErrors verifies error handling for malformed data.
func TestDeserializeOffsetSyncErrors(t *testing.T) {
	tests := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{"nil_key", nil, make([]byte, 16)},
		{"short_key", []byte{0, 1}, make([]byte, 16)},
		{"short_value", []byte{0, 1, 'a', 0, 0, 0, 0}, make([]byte, 8)},
		{"key_length_mismatch", []byte{0, 10, 'a', 0, 0, 0, 0}, make([]byte, 16)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := types.DeserializeOffsetSync(tt.key, tt.value)
			if err == nil {
				t.Error("expected error for malformed data")
			}
		})
	}
}
