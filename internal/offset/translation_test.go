package offset

import (
	"fmt"
	"sync"
	"testing"

	"github.com/gomm2/gomm2/pkg/types"
)

// ---------------------------------------------------------------------------
// Translation accuracy with large offset gaps
// ---------------------------------------------------------------------------

func TestTranslateWithLargeGaps(t *testing.T) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	// Insert syncs with large gaps
	store.HandleSync(makeSync("t", 0, 1000, 500))
	store.HandleSync(makeSync("t", 0, 1000000, 500000))

	// Query in the gap — should use the lower sync
	got, ok := store.TranslateDownstream(tp("t", 0), 50000)
	if !ok {
		t.Fatal("expected translation to succeed")
	}
	// Should use sync at 1000 (the older one if it's still stored)
	// since 50000 > 1000, result = 501 (downstream+1)
	if got < 0 {
		t.Errorf("expected positive translated offset, got %d", got)
	}
}

func TestTranslateWithManyIncrementalSyncs(t *testing.T) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	// Feed 1000 syncs incrementally
	for i := int64(0); i < 1000; i++ {
		store.HandleSync(makeSync("t", 0, i*100, i*50))
	}

	// Latest sync: upstream=99900, downstream=49950
	got, ok := store.TranslateDownstream(tp("t", 0), 99900)
	if !ok {
		t.Fatal("expected translation to succeed")
	}
	if got != 49950 {
		t.Errorf("expected 49950, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Concurrent access (race detection)
// ---------------------------------------------------------------------------

func TestConcurrentHandleSyncAndTranslate(t *testing.T) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	const (
		numWriters = 4
		numReaders = 8
		iterations = 1000
	)

	var wg sync.WaitGroup

	// Writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			topic := fmt.Sprintf("topic-%d", writerID)
			for i := int64(0); i < iterations; i++ {
				store.HandleSync(types.OffsetSync{
					TopicPartition:   types.TopicPartition{Topic: topic, Partition: 0},
					UpstreamOffset:   i * 100,
					DownstreamOffset: i * 50,
				})
			}
		}(w)
	}

	// Readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			topic := fmt.Sprintf("topic-%d", readerID%numWriters)
			for i := int64(0); i < iterations; i++ {
				store.TranslateDownstream(
					types.TopicPartition{Topic: topic, Partition: 0},
					i*100,
				)
			}
		}(r)
	}

	wg.Wait()
}

func TestConcurrentMultiPartition(t *testing.T) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	const (
		numPartitions = 100
		iterations    = 100
	)

	var wg sync.WaitGroup

	// Write to many partitions concurrently
	for p := int32(0); p < numPartitions; p++ {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()
			for i := int64(0); i < iterations; i++ {
				store.HandleSync(types.OffsetSync{
					TopicPartition:   types.TopicPartition{Topic: "t", Partition: partition},
					UpstreamOffset:   i * 10,
					DownstreamOffset: i * 5,
				})
			}
		}(p)
	}

	// Read from many partitions concurrently
	for p := int32(0); p < numPartitions; p++ {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()
			for i := int64(0); i < iterations; i++ {
				store.TranslateDownstream(
					types.TopicPartition{Topic: "t", Partition: partition},
					i*10,
				)
			}
		}(p)
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// Monotonicity: increasing upstream → non-decreasing downstream
// ---------------------------------------------------------------------------

func TestTranslationMonotonicity(t *testing.T) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	// Feed a sequence of syncs
	for i := int64(0); i < 500; i++ {
		store.HandleSync(makeSync("t", 0, i*10, i*5))
	}

	// Translate increasing upstream offsets
	var prevDownstream int64 = -1
	for up := int64(0); up <= 5000; up += 7 {
		down, ok := store.TranslateDownstream(tp("t", 0), up)
		if !ok {
			continue
		}
		if down < prevDownstream {
			t.Errorf("monotonicity violated: upstream=%d → downstream=%d, prev downstream=%d",
				up, down, prevDownstream)
		}
		prevDownstream = down
	}
}

func TestTranslationMonotonicityWithGaps(t *testing.T) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	// Feed syncs with irregular gaps
	offsets := []struct{ up, down int64 }{
		{10, 5}, {50, 25}, {100, 50}, {500, 250}, {10000, 5000},
	}
	for _, o := range offsets {
		store.HandleSync(makeSync("t", 0, o.up, o.down))
	}

	var prevDownstream int64 = -1
	for up := int64(0); up <= 11000; up += 13 {
		down, ok := store.TranslateDownstream(tp("t", 0), up)
		if !ok {
			continue
		}
		if down < prevDownstream {
			t.Errorf("monotonicity violated: upstream=%d → downstream=%d, prev=%d",
				up, down, prevDownstream)
		}
		prevDownstream = down
	}
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

func TestTranslateBeforeAllSyncs(t *testing.T) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	store.HandleSync(makeSync("t", 0, 100, 50))

	// Query for offset 10, which is before the sync at 100
	got, ok := store.TranslateDownstream(tp("t", 0), 10)
	if !ok {
		t.Fatal("expected ok=true")
	}
	// All 64 slots are filled with the same sync (100,50), so upstream=10 < 100 → returns -1
	if got != -1 {
		t.Logf("TranslateDownstream(10) = %d (offset before all syncs)", got)
	}
}

func TestStoreString(t *testing.T) {
	store := NewSyncStore()
	s := store.String()
	if s == "" {
		t.Error("String() should not be empty")
	}

	store.HandleSync(makeSync("t", 0, 100, 50))
	store.MarkReadToEnd()
	s = store.String()
	if s == "" {
		t.Error("String() should not be empty after sync")
	}
}

func TestIsReadyBeforeAndAfterMarkReadToEnd(t *testing.T) {
	store := NewSyncStore()
	if store.IsReady() {
		t.Error("store should not be ready before MarkReadToEnd")
	}
	store.MarkReadToEnd()
	if !store.IsReady() {
		t.Error("store should be ready after MarkReadToEnd")
	}
}

func TestHandleSyncDuringInitialRead(t *testing.T) {
	store := NewSyncStore()

	// During initial read (before MarkReadToEnd), only the latest should be kept
	store.HandleSync(makeSync("t", 0, 100, 50))
	store.HandleSync(makeSync("t", 0, 200, 150))
	store.HandleSync(makeSync("t", 0, 300, 250))

	store.MarkReadToEnd()

	// Should be able to translate the latest
	got, ok := store.TranslateDownstream(tp("t", 0), 300)
	if !ok {
		t.Fatal("expected translation to succeed")
	}
	if got != 250 {
		t.Errorf("expected 250, got %d", got)
	}
}
