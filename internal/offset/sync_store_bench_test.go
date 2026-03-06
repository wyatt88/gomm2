package offset

import (
	"fmt"
	"testing"

	"github.com/gomm2/gomm2/pkg/types"
)

// ---------------------------------------------------------------------------
// Benchmarks: TranslateDownstream with varying store sizes
// ---------------------------------------------------------------------------

func BenchmarkTranslateDownstream_1Partition(b *testing.B) {
	benchTranslate(b, 1, 100)
}

func BenchmarkTranslateDownstream_10Partitions(b *testing.B) {
	benchTranslate(b, 10, 100)
}

func BenchmarkTranslateDownstream_100Partitions(b *testing.B) {
	benchTranslate(b, 100, 100)
}

func BenchmarkTranslateDownstream_1000Partitions(b *testing.B) {
	benchTranslate(b, 1000, 100)
}

func BenchmarkTranslateDownstream_10000Partitions(b *testing.B) {
	benchTranslate(b, 10000, 100)
}

func benchTranslate(b *testing.B, partitions int, syncsPerPart int) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	for p := 0; p < partitions; p++ {
		for s := 0; s < syncsPerPart; s++ {
			store.HandleSync(types.OffsetSync{
				TopicPartition:   types.TopicPartition{Topic: "topic", Partition: int32(p)},
				UpstreamOffset:   int64(s * 100),
				DownstreamOffset: int64(s * 50),
			})
		}
	}

	target := types.TopicPartition{Topic: "topic", Partition: 0}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.TranslateDownstream(target, int64(i%10000))
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: HandleSync throughput
// ---------------------------------------------------------------------------

func BenchmarkHandleSync_SinglePartition(b *testing.B) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	tp := types.TopicPartition{Topic: "topic", Partition: 0}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.HandleSync(types.OffsetSync{
			TopicPartition:   tp,
			UpstreamOffset:   int64(i),
			DownstreamOffset: int64(i / 2),
		})
	}
}

func BenchmarkHandleSync_MultiPartition(b *testing.B) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	partitions := make([]types.TopicPartition, 100)
	for i := range partitions {
		partitions[i] = types.TopicPartition{Topic: "topic", Partition: int32(i)}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tp := partitions[i%100]
		store.HandleSync(types.OffsetSync{
			TopicPartition:   tp,
			UpstreamOffset:   int64(i),
			DownstreamOffset: int64(i / 2),
		})
	}
}

func BenchmarkHandleSync_InitialLoad(b *testing.B) {
	// Benchmark performance during initial load (before MarkReadToEnd)
	store := NewSyncStore()

	tp := types.TopicPartition{Topic: "topic", Partition: 0}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.HandleSync(types.OffsetSync{
			TopicPartition:   tp,
			UpstreamOffset:   int64(i),
			DownstreamOffset: int64(i / 2),
		})
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: Sync store operations under various partition counts
// ---------------------------------------------------------------------------

func BenchmarkSyncStoreOperations_10Partitions(b *testing.B) {
	benchSyncStoreOps(b, 10)
}

func BenchmarkSyncStoreOperations_100Partitions(b *testing.B) {
	benchSyncStoreOps(b, 100)
}

func BenchmarkSyncStoreOperations_1000Partitions(b *testing.B) {
	benchSyncStoreOps(b, 1000)
}

func benchSyncStoreOps(b *testing.B, partitions int) {
	store := NewSyncStore()
	store.MarkReadToEnd()

	// Pre-populate
	for p := 0; p < partitions; p++ {
		for s := 0; s < 50; s++ {
			store.HandleSync(types.OffsetSync{
				TopicPartition:   types.TopicPartition{Topic: fmt.Sprintf("topic-%d", p/10), Partition: int32(p % 10)},
				UpstreamOffset:   int64(s * 100),
				DownstreamOffset: int64(s * 50),
			})
		}
	}

	tps := make([]types.TopicPartition, partitions)
	for i := 0; i < partitions; i++ {
		tps[i] = types.TopicPartition{Topic: fmt.Sprintf("topic-%d", i/10), Partition: int32(i % 10)}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Mix of reads and writes (80% reads, 20% writes)
		tp := tps[i%partitions]
		if i%5 == 0 {
			store.HandleSync(types.OffsetSync{
				TopicPartition:   tp,
				UpstreamOffset:   int64(50*100 + i),
				DownstreamOffset: int64(50*50 + i/2),
			})
		} else {
			store.TranslateDownstream(tp, int64(i%5000))
		}
	}
}
