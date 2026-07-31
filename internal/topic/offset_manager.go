package topic

import (
	"encoding/binary"
	"fmt"
	"sync"
)

const ConsumerOffsetTopic = "__consumer_offsets"

type ConsumerKey struct{
	Group string
	Topic string
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

	// ensure internal topic exists
	_, err := tm.GetOrCreate(ConsumerOffsetTopic)
	if err != nil{
		return nil, err
	}

	if err := om.bootstrap();err != nil{
		return nil, err
	}

	return om, nil
}

func (om *OffsetManager) CommitOffset(group string, topic string, offset uint64, rawPayload []byte) error{
	// Write to disk first then map to prevent loss of data
	// Also writting to append only logs is fast
	internalTopic, err := om.topicManager.GetOrCreate(ConsumerOffsetTopic)
	if err != nil{
		return err
	}
	// TODO: Implement Log Compaction
	if _, err := internalTopic.Append(rawPayload); err != nil{
		return fmt.Errorf("failed to append to consumer offsets: %v", err)
	}

	key := ConsumerKey{
		Group: group,
		Topic: topic,
	}

	om.mu.Lock()
	om.offsets[key] = offset
	om.mu.Unlock()

	return nil
}

func (om *OffsetManager) FetchOffset(group string, topic string) uint64{
	key := ConsumerKey{
		Group: group,
		Topic: topic,
	}

	om.mu.RLock()
	defer om.mu.RUnlock()

	if offset, exists := om.offsets[key]; exists{
		return offset
	}

	return 0
}

func (om *OffsetManager) bootstrap() error{
	t, err := om.topicManager.GetOrCreate(ConsumerOffsetTopic)
	if err != nil{
		return err
	}

	var currentOffset uint64 = 0
	for {
		payload, err := t.Read(currentOffset)
		if err != nil{
			break // either at the end of log or its empty
		}

		idx := 0
		groupLen := int(binary.BigEndian.Uint16(payload[idx:idx+2]))
		idx += 2
		groupName := string(payload[idx:idx+groupLen])
		idx += groupLen

		topicLen := int(binary.BigEndian.Uint16(payload[idx:idx+2]))
		idx += 2
		topicName := string(payload[idx:idx+topicLen])
		idx += topicLen

		offset := binary.BigEndian.Uint64(payload[idx:idx+8])

		om.offsets[ConsumerKey{Group : groupName,Topic : topicName}] = offset
		currentOffset++
	}

	return nil
}