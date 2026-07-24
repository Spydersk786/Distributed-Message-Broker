package protocol

import (
	"encoding/binary"
	"fmt"

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

		topic := topicManager.GetOrCreate(topicName)
		offset := topic.Append(message)

		respBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(respBuf, uint64(offset))

		return respBuf, nil
	}
}