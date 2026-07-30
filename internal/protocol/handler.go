package protocol

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/Spydersk786/broker/internal/topic"
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