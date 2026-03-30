// Package mirror — ring_buffer.go implements an offset-ordered concurrent
// buffer that decouples parallel fetchers from a single in-order producer.
//
// Design goals:
//   - O(1) insert by offset (direct slot mapping)
//   - O(1) drain in offset order (sequential scan from drainOffset)
//   - Bounded memory: fixed number of slots, backpressure via blocking insert
//   - Lock-free hot path using atomic state per slot + condition variable for signaling
//
// Memory model:
//   With 8192 slots × 39KB avg record = ~320MB max buffered data.
//   The buffer does NOT copy record bytes — it holds pointers to franz-go's
//   fetch response buffers. Records are released to GC after produce completes.
package mirror

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
)

// slotState constants for the atomic state machine per slot.
const (
	slotEmpty  int32 = 0 // slot available for writing
	slotFilled int32 = 1 // slot contains a record, ready for drain
	slotTaken  int32 = 2 // slot being drained (transitional)
)

// ringSlot holds a single record in the ring buffer.
type ringSlot struct {
	state  atomic.Int32
	record *kgo.Record // source record (zero-copy slice into fetch buffer)
	offset int64       // source offset for ordering verification
}

// OrderedRingBuffer is a concurrent, offset-indexed ring buffer.
//
// Fetchers call Put(offset, record) which maps to a slot and blocks if
// the slot is not yet drained. The single drainer calls Drain() which
// yields records in strict offset order.
type OrderedRingBuffer struct {
	slots    []ringSlot
	capacity int64

	// drainOffset is the next offset the drainer expects.
	// Only the drain goroutine writes to this; fetchers read it for backpressure.
	drainOffset atomic.Int64

	// mu + cond protects wakeups between fetchers and drainer.
	mu   sync.Mutex
	cond *sync.Cond

	closed atomic.Bool
}

// NewOrderedRingBuffer creates a ring buffer with the given capacity (number of slots).
// startOffset is the first offset the drainer will expect.
func NewOrderedRingBuffer(capacity int, startOffset int64) *OrderedRingBuffer {
	rb := &OrderedRingBuffer{
		slots:    make([]ringSlot, capacity),
		capacity: int64(capacity),
	}
	rb.drainOffset.Store(startOffset)
	rb.cond = sync.NewCond(&rb.mu)
	return rb
}

// slotIndex maps an offset to a slot index via modulo.
func (rb *OrderedRingBuffer) slotIndex(offset int64) int {
	idx := offset % rb.capacity
	if idx < 0 {
		idx += rb.capacity
	}
	return int(idx)
}

// Put inserts a record at the given offset. It blocks if the target slot
// is still occupied (i.e., the drainer hasn't caught up yet — backpressure).
// Returns false if the buffer is closed or context is cancelled.
func (rb *OrderedRingBuffer) Put(ctx context.Context, offset int64, record *kgo.Record) bool {
	idx := rb.slotIndex(offset)
	slot := &rb.slots[idx]

	// Fast path: slot is already empty — use CAS to avoid data race
	if slot.state.Load() == slotEmpty {
		slot.record = record
		slot.offset = offset
		if slot.state.CompareAndSwap(slotEmpty, slotFilled) {
			rb.cond.Signal()
			return true
		}
		// CAS failed, fall through to slow path
	}

	// Slow path: slot is occupied — wait for drainer to free it.
	// This happens when the drainer is behind by >= capacity records.
	rb.mu.Lock()
	for slot.state.Load() != slotEmpty && !rb.closed.Load() && ctx.Err() == nil {
		rb.cond.Wait()
	}
	rb.mu.Unlock()

	if rb.closed.Load() || ctx.Err() != nil {
		return false
	}

	// Write the record into the slot
	slot.record = record
	slot.offset = offset
	slot.state.Store(slotFilled)

	// Wake the drainer
	rb.cond.Signal()

	return true
}

// DrainBatch drains up to maxBatch records in strict offset order.
// It blocks until at least one record is available or the context is cancelled.
// Returns the drained records (caller owns them) and whether the buffer is still open.
func (rb *OrderedRingBuffer) DrainBatch(ctx context.Context, maxBatch int) ([]*kgo.Record, bool) {
	if maxBatch <= 0 {
		maxBatch = 256
	}

	// Wait for at least the first record
	nextOffset := rb.drainOffset.Load()
	idx := rb.slotIndex(nextOffset)
	slot := &rb.slots[idx]

	// Block until the next expected offset is filled
	rb.mu.Lock()
	for slot.state.Load() != slotFilled && !rb.closed.Load() && ctx.Err() == nil {
		rb.cond.Wait()
	}
	rb.mu.Unlock()

	if rb.closed.Load() && slot.state.Load() != slotFilled {
		return nil, false
	}
	if ctx.Err() != nil {
		return nil, false
	}

	// Drain as many consecutive records as available, up to maxBatch
	batch := make([]*kgo.Record, 0, maxBatch)
	for len(batch) < maxBatch {
		off := nextOffset + int64(len(batch))
		i := rb.slotIndex(off)
		s := &rb.slots[i]

		if s.state.Load() != slotFilled {
			break // gap or not yet filled
		}

		// Take the record
		rec := s.record
		s.record = nil
		s.state.Store(slotEmpty)

		batch = append(batch, rec)
	}

	// Advance drain offset
	rb.drainOffset.Add(int64(len(batch)))

	// Wake any blocked fetchers (slots freed)
	rb.cond.Broadcast()

	return batch, true
}

// Close signals all waiters to stop.
func (rb *OrderedRingBuffer) Close() {
	rb.closed.Store(true)
	rb.cond.Broadcast()
}

// DrainOffset returns the current drain position.
func (rb *OrderedRingBuffer) DrainOffset() int64 {
	return rb.drainOffset.Load()
}

// Capacity returns the buffer capacity.
func (rb *OrderedRingBuffer) Capacity() int {
	return int(rb.capacity)
}
