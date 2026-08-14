package protocol

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/Spydersk786/broker/internal/topic"
	"github.com/Spydersk786/broker/internal/metrics"
)

func HandleProduce(topicManager *topic.Manager) Handler{
	return func(payload []byte) ([]byte, error){
		if len(payload) < 2{
			return nil, fmt.Errorf("payload too short for topic length")
		}

		topicNameLen := binary.BigEndian.Uint16(payload[:2])

		if len(payload) < int(2+topicNameLen) {
			return nil, fmt.Errorf("payload doesn't match the topic length")
		}

		topicName := string(payload[2:2+topicNameLen])

		message := payload[2+topicNameLen:]

		topic, err := topicManager.GetOrCreate(topicName); if err != nil{
			log.Printf("Failed to create/get the topic: %v", err)
			return nil, err
		}
		offset,err := topic.Append(message); if err != nil{
			log.Printf("Failed to append message to disk: %v", err)
			return nil, err
		}
		metrics.MessagesProduced.WithLabelValues(topicName).Inc()
		metrics.BytesWritten.WithLabelValues(topicName).Add(float64(len(message)))

		respBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(respBuf, uint64(offset))

		return respBuf, nil
	}
}

func HandleFetch(topicManager *topic.Manager) Handler{
	return func(payload []byte) ([]byte, error){
		if len(payload) < 2{
			return nil, fmt.Errorf("payload too short for topic length")
		}

		topicNameLen := binary.BigEndian.Uint16(payload[:2])

		if len(payload) < int(2+topicNameLen+8) {
			return nil, fmt.Errorf("payload too short for offset")
		}

		topicName := string(payload[2:2+topicNameLen])

		// Extract message offset to be extracted
		offsetIdx := 2 + topicNameLen
		offset := binary.BigEndian.Uint64(payload[offsetIdx:offsetIdx+8])

		topic, err := topicManager.GetOrCreate(topicName)
		if err != nil{
			return nil, err
		}

		message, err := topic.Read(offset)
		if err != nil{
			return nil, err
		}

		return message, nil
	}
}

// We explicity need a commit req instead of automatic commits
// In case message is delivered to client and it crashed before
// processing it and need the same message again. 
func HandleCommitOffset(om *topic.OffsetManager) Handler{
	return func(payload []byte) ([]byte, error){
		// [GroupLen(2)] [Group] [TopicLen(2)] [Topic] [offset(8)]
		idx := 0
		groupLen := binary.BigEndian.Uint16(payload[idx:idx+2])
		idx +=2
		group := string(payload[idx:idx+int(groupLen)])
		idx += int(groupLen)

		topicLen := binary.BigEndian.Uint16(payload[idx:idx+2])
		idx +=2
		topic := string(payload[idx:idx+int(topicLen)])
		idx += int(topicLen)

		offset := binary.BigEndian.Uint64(payload[idx:idx+8])

		err := om.CommitOffset(group, topic, offset, payload)
		if err != nil{
			return nil, err
		}

		return []byte{1}, nil // Success ACK
	}
}

func HandleFetchOffset(om *topic.OffsetManager) Handler{
	return func(payload []byte) ([]byte, error){
		idx := 0
		groupLen := binary.BigEndian.Uint16(payload[idx:idx+2])
		idx += 2
		group := string(payload[idx:idx+int(groupLen)])
		idx += int(groupLen)

		topic := string(payload[idx:]) // Rest of the payload is topic name

		offset := om.FetchOffset(group, topic)

		resp := make([]byte, 8)
		binary.BigEndian.PutUint64(resp, offset)
		return resp, nil
	}
}