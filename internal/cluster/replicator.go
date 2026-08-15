package cluster

import (
	"context"
	"encoding/binary"
	"io"
	"log"
	"net"
	"time"

	"github.com/Spydersk786/broker/internal/topic"
	"github.com/Spydersk786/broker/internal/protocol"
)

func StartReplicator(ctx context.Context, tm *topic.Manager, cm *Manager){
	ticker := time.NewTicker(2*time.Second)

	go func() {
		defer ticker.Stop()
		for{
			select{
			case <- ctx.Done():
				log.Println("[Replicator] Shutting down background sync")
				return
			case <- ticker.C:
				discoverPartitions(tm, cm)
				for _, p := range tm.GetAllPartitions(){
					if p.Role == topic.RoleFollower{
						syncPartition(p, cm)
					}
				}
			}

		}
	}()
}

func discoverPartitions(tm *topic.Manager, cm *Manager){
	peers := cm.GetAllPeers()
	for _, peer := range peers{
		if peer.ID == cm.LocalID{
			continue
		}
		
		conn, err := net.DialTimeout("tcp", peer.Address, 2*time.Second)
		if err != nil {
			log.Printf("[Replicator] Failed to connect to peer %d (%s): %v", peer.ID, peer.Address, err)
			continue
		}
		
		req := make([]byte, 5)
		binary.BigEndian.PutUint32(req[0:4], 1) 
		req[4] = byte(protocol.DiscoverPartitionsCmd)
		if _, err := conn.Write(req); err != nil {
			log.Printf("[Replicator] Failed to send request to peer %d (%s): %v", peer.ID, peer.Address, err)
			conn.Close()
			continue
		}

		sizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, sizeBuf); err != nil {
			log.Printf("[Replicator] Failed to read response size from peer %d (%s): %v", peer.ID, peer.Address, err)
			conn.Close()
			continue
		}

		size := binary.BigEndian.Uint32(sizeBuf)
		if size == 0 {
			conn.Close()
			continue
		}

		respBuf := make([]byte, size)
		if _, err := io.ReadFull(conn, respBuf); err != nil {
			log.Printf("[Replicator] Failed to read response payload from peer %d (%s): %v", peer.ID, peer.Address, err)
			conn.Close()
			continue
		}

		conn.Close()

		idx := 0
		for idx < int(size) {
			tLen := int(binary.BigEndian.Uint16(respBuf[idx:idx+2]))
			idx += 2
			topicName := string(respBuf[idx:idx+tLen])
			idx += tLen
			pID := binary.BigEndian.Uint32(respBuf[idx:idx+4])
			idx += 4
			
			if tm.GetLeaderID(topicName, pID) != tm.GetLocalID() {
				tm.GetOrCreatePartition(topicName, pID)
			}
		}
	}
}

func syncPartition(p *topic.Partition, cm *Manager){
	peers := cm.GetAllPeers()
	var leaderAddr string
	for _, peer := range peers{
		if peer.ID == p.LeaderID{
			leaderAddr = peer.Address
			break
		}
	}

	if leaderAddr == "" {return}

	offsetToFetch := p.NextOffset()

	topicBytes := []byte(p.TopicName)
	payload := make([]byte, 2+len(topicBytes)+4+8)

	binary.BigEndian.PutUint16(payload[0:2], uint16(len(topicBytes)))
	copy(payload[2:], topicBytes)

	idx := len(topicBytes)+2
	binary.BigEndian.PutUint32(payload[idx:idx+4], p.ID)
	binary.BigEndian.PutUint64(payload[idx+4:idx+12], offsetToFetch)

	finalPayload := append([]byte{2}, payload...)
	req := make([]byte, 4+len(finalPayload))
	binary.BigEndian.PutUint32(req[0:4], uint32(len(finalPayload)))
	copy(req[4:], finalPayload)

	conn, err := net.DialTimeout("tcp", leaderAddr, 2*time.Second)
	if err != nil {return}
	defer conn.Close()

	conn.Write(req)

	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {return}

	size := binary.BigEndian.Uint32(sizeBuf)

	if size > 0{
		msgBuf := make([]byte, size)
		io.ReadFull(conn, msgBuf)

		p.AppendFollower(msgBuf)
		log.Printf("[Replicator] Synced offset %d for %s-%d from Broker %d", offsetToFetch, p.TopicName, p.LeaderID, p.LeaderID)
	} 
}