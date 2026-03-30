package offset

import (
	"testing"

	"github.com/gomm2/gomm2/pkg/types"
)

func tp(topic string, partition int32) types.TopicPartition {
	return types.TopicPartition{Topic: topic, Partition: partition}
}

func makeSync(topic string, partition int32, up, down int64) types.OffsetSync {
	return types.OffsetSync{
		TopicPartition:   tp(topic, partition),
		UpstreamOffset:   up,
		DownstreamOffset: down,
	}
}

func TestTranslateUnavailableBeforeReadToEnd(t *testing.T) {
	store := NewSyncStore()
	store.HandleSync(makeSync("t", 0, 100, 50))

	_, ok := store.TranslateDownstream(tp("t", 0), 100)
	if ok {
		t.Error("expected translation to be unavailable before read-to-end")
	}
}

func TestTranslateExactMatch(t *testing.T) {
	store := NewSyncStore()
	store.HandleSync(makeSync("t", 0, 100, 50))
	store.MarkReadToEnd()

	got, ok := store.TranslateDownstream(tp("t", 0), 100)
	if !ok {
		t.Fatal("expected translation to succeed")
	}
	if got != 50 {
		t.Errorf("expected 50, got %d", got)
	}
}

func TestTranslateAheadOfSync(t *testing.T) {
	store := NewSyncStore()
	store.HandleSync(makeSync("t", 0, 100, 50))
	store.MarkReadToEnd()

	got, ok := store.TranslateDownstream(tp("t", 0), 150)
	if !ok {
		t.Fatal("expected translation to succeed")
	}
	// Should return downstream + 1 (because upstream is ahead of sync)
	if got != 51 {
		t.Errorf("expected 51, got %d", got)
	}
}

func TestTranslateUnknownPartition(t *testing.T) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	_, ok := store.TranslateDownstream(tp("unknown", 0), 100)
	if ok {
		t.Error("expected translation to be unavailable for unknown partition")
	}
}

func TestMultipleSyncsPreservesLatest(t *testing.T) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	store.HandleSync(makeSync("t", 0, 100, 50))
	store.HandleSync(makeSync("t", 0, 200, 150))
	store.HandleSync(makeSync("t", 0, 300, 250))

	// Query for offset 250 — with exponential spacing, slots 0-8 hold sync (300,250)
	// and slots 9+ hold (100,50). Since 250 < 300, we skip to slot 9 (upstream=100),
	// and translate as downstream=50+1=51 (step=1 because 100 < 250).
	got, ok := store.TranslateDownstream(tp("t", 0), 250)
	if !ok {
		t.Fatal("expected translation to succeed")
	}
	if got != 51 {
		t.Errorf("expected 51, got %d", got)
	}

	// Query for exact latest sync
	got, ok = store.TranslateDownstream(tp("t", 0), 300)
	if !ok {
		t.Fatal("expected translation to succeed")
	}
	if got != 250 {
		t.Errorf("expected 250, got %d", got)
	}
}

func TestUpstreamRewindClearsStore(t *testing.T) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	store.HandleSync(makeSync("t", 0, 200, 150))
	// Rewind: new sync has lower upstream offset
	store.HandleSync(makeSync("t", 0, 50, 30))

	got, ok := store.TranslateDownstream(tp("t", 0), 50)
	if !ok {
		t.Fatal("expected translation to succeed")
	}
	if got != 30 {
		t.Errorf("expected 30, got %d", got)
	}
}
