// Package types defines shared data types for gomm2 mirror replication.
package types

import (
	"encoding/binary"
	"fmt"
	"time"
)

// TopicPartition identifies a topic and partition pair.
type TopicPartition struct {
	Topic     string `json:"topic" yaml:"topic"`
	Partition int32  `json:"partition" yaml:"partition"`
}

func (tp TopicPartition) String() string {
	return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition)
}

// OffsetSync records a mapping between upstream and downstream offsets for a topic-partition.
type OffsetSync struct {
	TopicPartition   TopicPartition
	UpstreamOffset   int64
	DownstreamOffset int64
}

func (os OffsetSync) String() string {
	return fmt.Sprintf("OffsetSync{tp=%s, upstream=%d, downstream=%d}",
		os.TopicPartition, os.UpstreamOffset, os.DownstreamOffset)
}

// SerializeKey serializes the OffsetSync key: topic (2-byte len + bytes) + partition (int32 big-endian).
func (os OffsetSync) SerializeKey() []byte {
	topicBytes := []byte(os.TopicPartition.Topic)
	buf := make([]byte, 2+len(topicBytes)+4)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(topicBytes)))
	copy(buf[2:2+len(topicBytes)], topicBytes)
	binary.BigEndian.PutUint32(buf[2+len(topicBytes):], uint32(os.TopicPartition.Partition))
	return buf
}

// SerializeValue serializes the OffsetSync value: upstreamOffset (int64) + downstreamOffset (int64).
func (os OffsetSync) SerializeValue() []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(os.UpstreamOffset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(os.DownstreamOffset))
	return buf
}

// DeserializeOffsetSync reads an OffsetSync from key+value byte slices.
func DeserializeOffsetSync(key, value []byte) (OffsetSync, error) {
	if len(key) < 6 {
		return OffsetSync{}, fmt.Errorf("offset sync key too short: %d bytes", len(key))
	}
	topicLen := binary.BigEndian.Uint16(key[0:2])
	if int(topicLen)+6 > len(key) {
		return OffsetSync{}, fmt.Errorf("offset sync key topic length mismatch")
	}
	topic := string(key[2 : 2+topicLen])
	partition := int32(binary.BigEndian.Uint32(key[2+topicLen:]))

	if len(value) < 16 {
		return OffsetSync{}, fmt.Errorf("offset sync value too short: %d bytes", len(value))
	}
	upstream := int64(binary.BigEndian.Uint64(value[0:8]))
	downstream := int64(binary.BigEndian.Uint64(value[8:16]))

	return OffsetSync{
		TopicPartition:   TopicPartition{Topic: topic, Partition: partition},
		UpstreamOffset:   upstream,
		DownstreamOffset: downstream,
	}, nil
}

// Heartbeat records a heartbeat emitted between clusters.
type Heartbeat struct {
	SourceCluster string
	TargetCluster string
	Timestamp     time.Time
}

func (h Heartbeat) String() string {
	return fmt.Sprintf("Heartbeat{source=%s, target=%s, ts=%s}",
		h.SourceCluster, h.TargetCluster, h.Timestamp.Format(time.RFC3339))
}

// SerializeKey serializes the Heartbeat key: sourceCluster + targetCluster as length-prefixed strings.
func (h Heartbeat) SerializeKey() []byte {
	src := []byte(h.SourceCluster)
	tgt := []byte(h.TargetCluster)
	buf := make([]byte, 2+len(src)+2+len(tgt))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(src)))
	copy(buf[2:2+len(src)], src)
	off := 2 + len(src)
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(tgt)))
	copy(buf[off+2:], tgt)
	return buf
}

// SerializeValue serializes the Heartbeat value: version (int16) + timestamp (int64 millis).
func (h Heartbeat) SerializeValue() []byte {
	buf := make([]byte, 2+8)
	binary.BigEndian.PutUint16(buf[0:2], 0) // version
	binary.BigEndian.PutUint64(buf[2:10], uint64(h.Timestamp.UnixMilli()))
	return buf
}

// Checkpoint records a consumer group offset translation between clusters.
type Checkpoint struct {
	ConsumerGroupID string
	TopicPartition  TopicPartition
	UpstreamOffset  int64
	DownstreamOffset int64
	Metadata        string
}

func (c Checkpoint) String() string {
	return fmt.Sprintf("Checkpoint{group=%s, tp=%s, upstream=%d, downstream=%d}",
		c.ConsumerGroupID, c.TopicPartition, c.UpstreamOffset, c.DownstreamOffset)
}

// SerializeKey serializes the Checkpoint key: group + topic (length-prefixed) + partition (int32).
func (c Checkpoint) SerializeKey() []byte {
	group := []byte(c.ConsumerGroupID)
	topic := []byte(c.TopicPartition.Topic)
	buf := make([]byte, 2+len(group)+2+len(topic)+4)
	off := 0
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(group)))
	off += 2
	copy(buf[off:off+len(group)], group)
	off += len(group)
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(topic)))
	off += 2
	copy(buf[off:off+len(topic)], topic)
	off += len(topic)
	binary.BigEndian.PutUint32(buf[off:], uint32(c.TopicPartition.Partition))
	return buf
}

// SerializeValue serializes the Checkpoint value: version (int16) + upstream (int64) + downstream (int64) + metadata (len-prefixed string).
func (c Checkpoint) SerializeValue() []byte {
	meta := []byte(c.Metadata)
	buf := make([]byte, 2+8+8+2+len(meta))
	off := 0
	binary.BigEndian.PutUint16(buf[off:off+2], 0) // version
	off += 2
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(c.UpstreamOffset))
	off += 8
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(c.DownstreamOffset))
	off += 8
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(meta)))
	off += 2
	copy(buf[off:], meta)
	return buf
}

// SourceAndTarget identifies a directed replication flow.
type SourceAndTarget struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}

func (st SourceAndTarget) String() string {
	return st.Source + "->" + st.Target
}
