package handler

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/Spydersk786/broker/internal/cluster"
	"github.com/Spydersk786/broker/internal/metrics"
	"github.com/Spydersk786/broker/internal/protocol"
	"github.com/Spydersk786/broker/internal/topic"
)

func proxyRequest(targetAddress string, cmdByte byte, payload []byte) ([]byte, error) {
	conn, err := net.Dial("tcp", targetAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader: %v", err)
	}
	defer conn.Close()

	// Rebuild the framing
	finalPayload := append([]byte{cmdByte}, payload...)
	req := make([]byte, 4+len(finalPayload))
	binary.BigEndian.PutUint32(req[0:4], uint32(len(finalPayload)))
	copy(req[4:], finalPayload)

	if _, err := conn.Write(req); err != nil {
		return nil, err
	}

	// Read the response from the leader
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(sizeBuf)
	if size == 0 {
		return []byte{}, nil
	}

	resp := make([]byte, size)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func HandleProduce(topicManager *topic.Manager, ClusterManager *cluster.Manager) protocol.Handler{
	return func(payload []byte) ([]byte, error){
		if len(payload) < 2{
			return nil, fmt.Errorf("payload too short for topic length")
		}

		topicNameLen := binary.BigEndian.Uint16(payload[:2])

		if len(payload) < int(2+topicNameLen) {
			return nil, fmt.Errorf("payload doesn't match the topic length")
		}

		topicName := string(payload[2:2+topicNameLen])
		idx := 2 + topicNameLen

		partitionID := binary.BigEndian.Uint32(payload[idx:idx+4])
		idx += 4

		message := payload[idx:]

		leaderID := topicManager.GetLeaderID(topicName, partitionID) 

		if topicManager.GetLocalID() != leaderID{
			peers := ClusterManager.GetAllPeers()
			var leaderAddr string
			for _, p := range peers {
				if p.ID == leaderID {
					leaderAddr = p.Address
					break
				}
			}
			
			if leaderAddr == "" {
				return nil, fmt.Errorf("leader %d is currently offline", leaderID)
			}

			// Proxy the exact same payload we received over to the actual Leader
			return proxyRequest(leaderAddr, byte(protocol.ProduceCmd), payload)
		}

		partition, err := topicManager.GetOrCreatePartition(topicName, partitionID)
		if err != nil{
			log.Printf("Failed to create/get the partition %s-%d: %v",topicName, partitionID, err)
			return nil, err
		}

		offset, err := partition.Append(message); if err != nil{
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

func HandleFetch(topicManager *topic.Manager, ClusterManager *cluster.Manager) protocol.Handler{
	return func(payload []byte) ([]byte, error){
		if len(payload) < 2{
			return nil, fmt.Errorf("payload too short for topic length")
		}

		topicNameLen := binary.BigEndian.Uint16(payload[:2])

		if len(payload) < int(2+topicNameLen+8) {
			return nil, fmt.Errorf("payload too short for offset")
		}

		topicName := string(payload[2:2+topicNameLen])
		idx := 2 + topicNameLen
		
		partitionID := binary.BigEndian.Uint32(payload[idx:idx+4])
		idx += 4

		leaderID := topicManager.GetLeaderID(topicName, partitionID) 

		if topicManager.GetLocalID() != leaderID{
			peers := ClusterManager.GetAllPeers()
			var leaderAddr string
			for _, p := range peers {
				if p.ID == leaderID {
					leaderAddr = p.Address
					break
				}
			}
			
			if leaderAddr == "" {
				return nil, fmt.Errorf("leader %d is currently offline", leaderID)
			}

			// Proxy the exact same payload we received over to the actual Leader
			return proxyRequest(leaderAddr, byte(protocol.FetchCmd), payload)
		}

		// Extract message offset to be extracted
		offsetIdx := idx
		offset := binary.BigEndian.Uint64(payload[offsetIdx:offsetIdx+8])

		partition, err := topicManager.GetOrCreatePartition(topicName, partitionID)
		if err != nil{
			return nil, err
		}

		message, err := partition.Read(offset)
		if err != nil{
			return []byte{}, nil // No more messages
		}

		return message, nil
	}
}

// We explicity need a commit req instead of automatic commits
// In case message is delivered to client and it crashed before
// processing it and need the same message again. 
func HandleCommitOffset(om *topic.OffsetManager, cm *cluster.Manager) protocol.Handler{
	return func(payload []byte) ([]byte, error){
		// [GroupLen(2)] [Group] [TopicLen(2)] [Topic] [offset(8)]
		idx := 0
		groupLen := binary.BigEndian.Uint16(payload[idx:idx+2])
		idx +=2
		group := string(payload[idx:idx+int(groupLen)])
		idx += int(groupLen)

		leaderID := om.GetLeaderForGroup(group) 

		if om.GetLocalID() != leaderID{
			peers := cm.GetAllPeers()
			var leaderAddr string
			for _, p := range peers {
				if p.ID == leaderID {
					leaderAddr = p.Address
					break
				}
			}
			
			if leaderAddr == "" {
				return nil, fmt.Errorf("leader %d is currently offline", leaderID)
			}

			// Proxy the exact same payload we received over to the actual Leader
			return proxyRequest(leaderAddr, byte(protocol.CommitOffset), payload)
		}

		topicLen := binary.BigEndian.Uint16(payload[idx:idx+2])
		idx +=2
		topic := string(payload[idx:idx+int(topicLen)])
		idx += int(topicLen)

		partitionID := binary.BigEndian.Uint32(payload[idx:idx+4])
		idx += 4

		offset := binary.BigEndian.Uint64(payload[idx:idx+8])

		err := om.CommitOffset(group, topic, partitionID, offset, payload)
		if err != nil{
			return nil, err
		}

		return []byte{}, nil // Success ACK
	}
}

func HandleFetchOffset(om *topic.OffsetManager, cm *cluster.Manager) protocol.Handler{
	return func(payload []byte) ([]byte, error){
		idx := 0
		groupLen := binary.BigEndian.Uint16(payload[idx:idx+2])
		idx += 2
		group := string(payload[idx:idx+int(groupLen)])
		idx += int(groupLen)

		leaderID := om.GetLeaderForGroup(group) 

		if om.GetLocalID() != leaderID{
			peers := cm.GetAllPeers()
			var leaderAddr string
			for _, p := range peers {
				if p.ID == leaderID {
					leaderAddr = p.Address
					break
				}
			}
			
			if leaderAddr == "" {
				return nil, fmt.Errorf("leader %d is currently offline", leaderID)
			}

			// Proxy the exact same payload we received over to the actual Leader
			return proxyRequest(leaderAddr, byte(protocol.FetchOffset), payload)
		}

		topicLen := binary.BigEndian.Uint16(payload[idx:idx+2])
		idx +=2
		topic := string(payload[idx:idx+int(topicLen)]) 
		idx += int(topicLen)

		partitionID := binary.BigEndian.Uint32(payload[idx:idx+4])
		idx += 4

		offset := om.FetchOffset(group, topic, partitionID)

		resp := make([]byte, 8)
		binary.BigEndian.PutUint64(resp, offset)
		return resp, nil
	}
}

func HandleMetaDataSync(tm *topic.Manager) protocol.Handler{
	return func(payload []byte) ([]byte, error){
		partitions := tm.GetAllPartitions()
		var resp []byte

		for _, p := range partitions{
			topicBytes := []byte(p.TopicName)
			// [TopicLen(2)] [Topic] [PartitionID(4)]
			entry := make([]byte, 2+len(topicBytes)+4)
			binary.BigEndian.PutUint16(entry[0:2], uint16(len(topicBytes)))
			copy(entry[2:2+len(topicBytes)], topicBytes)
			binary.BigEndian.PutUint32(entry[2+len(topicBytes):], p.ID)

			resp = append(resp, entry...)
		}

		return resp, nil
	}
}