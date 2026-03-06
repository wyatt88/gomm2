// Package offset implements the offset sync store and offset translation.
package offset

import (
	"fmt"
	gosync "sync"

	"github.com/gomm2/gomm2/pkg/types"
)

const syncsPerPartition = 64 // matches Java MM2: Long.SIZE = 64

// SyncStore stores offset syncs per topic-partition and translates offsets
// between source and target clusters.
//
// Implements the same invariant-based storage as Java MM2's OffsetSyncStore:
//   - syncs[0] is the latest offset sync
//   - Exponential spacing between syncs for O(1) lookup
//   - Linear-time updates
type SyncStore struct {
	mu          gosync.RWMutex
	syncs       map[types.TopicPartition][syncsPerPartition]types.OffsetSync
	initialized bool
	readToEnd   bool
}

// NewSyncStore creates an empty offset sync store.
func NewSyncStore() *SyncStore {
	return &SyncStore{
		syncs: make(map[types.TopicPartition][syncsPerPartition]types.OffsetSync),
	}
}

// MarkReadToEnd marks the initial read-to-end of the sync topic as complete.
// Translation is unavailable until this is called.
func (s *SyncStore) MarkReadToEnd() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readToEnd = true
	s.initialized = true
}

// IsReady returns true if the store has completed its initial read.
func (s *SyncStore) IsReady() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readToEnd
}

// HandleSync processes an incoming offset sync record.
func (s *SyncStore) HandleSync(sync types.OffsetSync) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tp := sync.TopicPartition
	existing, ok := s.syncs[tp]
	if !ok {
		// First sync for this partition — fill all slots
		var arr [syncsPerPartition]types.OffsetSync
		for i := range arr {
			arr[i] = sync
		}
		s.syncs[tp] = arr
		return
	}

	// Check for upstream rewind
	if sync.UpstreamOffset < existing[0].UpstreamOffset {
		var arr [syncsPerPartition]types.OffsetSync
		for i := range arr {
			arr[i] = sync
		}
		s.syncs[tp] = arr
		return
	}

	// During initial read, only keep the latest
	if !s.readToEnd {
		var arr [syncsPerPartition]types.OffsetSync
		for i := range arr {
			arr[i] = sync
		}
		s.syncs[tp] = arr
		return
	}

	// Normal update: maintain exponentially-spaced syncs
	updated := existing // copy (value type array)
	updated[0] = sync

	for i := 1; i < syncsPerPartition; i++ {
		prev := i - 1
		// Check invariant B: syncs[prev].upstream <= syncs[i].upstream + 2^i - 2^prev
		bound := updated[i].UpstreamOffset + (1 << i) - (1 << prev)
		if bound >= 0 && updated[prev].UpstreamOffset <= bound {
			// Invariant holds for this and all later — stop
			break
		}
		// Repair: propagate the replacement
		updated[i] = updated[prev]
	}

	s.syncs[tp] = updated
}

// TranslateDownstream translates an upstream offset to a downstream offset.
// Returns the translated offset and true, or -1 and false if translation is unavailable.
func (s *SyncStore) TranslateDownstream(tp types.TopicPartition, upstreamOffset int64) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.readToEnd {
		return -1, false
	}

	syncs, ok := s.syncs[tp]
	if !ok {
		return -1, false
	}

	// Find the latest sync that precedes the upstream offset
	for i := 0; i < syncsPerPartition; i++ {
		if syncs[i].UpstreamOffset <= upstreamOffset {
			step := int64(0)
			if syncs[i].UpstreamOffset < upstreamOffset {
				step = 1
			}
			return syncs[i].DownstreamOffset + step, true
		}
	}

	// Offset is before all known syncs — too far in the past
	return -1, true
}

func (s *SyncStore) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("SyncStore{partitions=%d, ready=%v}", len(s.syncs), s.readToEnd)
}
