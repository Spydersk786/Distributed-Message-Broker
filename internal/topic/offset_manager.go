package topic

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync"
)

const ConsumerOffsetTopic = "__consumer_offsets"
const NumOffsetPartitions = 50

type ConsumerKey struct{
	Group     string
	Topic     string
	Partition uint32 
}

type OffsetManager struct{
	mu				sync.RWMutex
	offsets 		map[ConsumerKey]uint64
	topicManager	*Manager
}

func NewOffsetManager(tm *Manager) (*OffsetManager, error){
	om := &OffsetManager{
		offsets: make(map[ConsumerKey]uint64),
		topicManager: tm,
	}

	if err := om.bootstrap();err != nil{
		return nil, err
	}

	return om, nil
}

// partitionForGroup determines which partition of __consumer_offsets owns this group
func partitionForGroup(group string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(group))
	return h.Sum32() % NumOffsetPartitions
}

func (om *OffsetManager) GetLeaderForGroup(group string) uint32{
	offsetPartitionID := partitionForGroup(group)
	return om.topicManager.GetLeaderID(ConsumerOffsetTopic, offsetPartitionID)
}

func (om *OffsetManager) GetLocalID() uint32{
	return om.topicManager.GetLocalID()
}

func (om *OffsetManager) CommitOffset(group string, topic string, partition uint32, offset uint64, rawPayload []byte) error {
	// 1. Find which partition of __consumer_offsets this group maps to
	offsetPartitionID := partitionForGroup(group)

	internalPartition, err := om.topicManager.GetOrCreatePartition(ConsumerOffsetTopic, offsetPartitionID)
	if err != nil {
		return err
	}
	
	// 2. Append to disk (If we aren't the leader for this offset partition, this will correctly fail!)
	if _, err := internalPartition.Append(rawPayload); err != nil {
		return fmt.Errorf("failed to append to consumer offsets: %v", err)
	}

	// 3. Update in-memory map
	key := ConsumerKey{Group:group, Topic:topic, Partition:partition}
	om.mu.Lock()
	om.offsets[key] = offset
	om.mu.Unlock()

	return nil
}

func (om *OffsetManager) FetchOffset(group string, topic string, partition uint32) uint64 {
	key := ConsumerKey{Group: group, Topic: topic, Partition: partition}
	om.mu.RLock()
	defer om.mu.RUnlock()

	if offset, exists := om.offsets[key]; exists {
		return offset
	}
	return 0
}

func (om *OffsetManager) bootstrap() error {
	// The TopicManager already recovered all partitions from disk in its own recoverState().
	// We just need to find the __consumer_offsets partitions and read them.
	
	om.topicManager.mu.RLock()
	offsetTopic, exists := om.topicManager.topics[ConsumerOffsetTopic]
	om.topicManager.mu.RUnlock()

	if !exists {
		return nil // First boot, no offsets exist yet
	}

	// Iterate through all offset partitions this broker holds locally
	offsetTopic.mu.RLock()
	for _, partition := range offsetTopic.Partitions {
		// Only read from disk to memory
		var currentOffset uint64 = 0
		for {
			payload, err := partition.Read(currentOffset)
			if err != nil {
				break // End of log for this partition
			}

			idx := 0
			groupLen := int(binary.BigEndian.Uint16(payload[idx : idx+2]))
			idx += 2
			groupName := string(payload[idx : idx+groupLen])
			idx += groupLen

			topicLen := int(binary.BigEndian.Uint16(payload[idx : idx+2]))
			idx += 2
			topicName := string(payload[idx : idx+topicLen])
			idx += topicLen

			// Read the topic partition ID (4 bytes)
			topicPartition := binary.BigEndian.Uint32(payload[idx : idx+4])
			idx += 4

			offset := binary.BigEndian.Uint64(payload[idx : idx+8])

			om.offsets[ConsumerKey{Group: groupName, Topic: topicName, Partition: topicPartition}] = offset
			currentOffset++
		}
	}
	offsetTopic.mu.RUnlock()

	return nil
}