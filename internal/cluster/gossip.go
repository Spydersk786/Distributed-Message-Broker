package cluster

import (
	"context"
	"encoding/binary"
	"io"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/Spydersk786/broker/internal/protocol"
)

func (m *Manager) StartGossip(ctx context.Context, seedNodes []string){
	ticker := time.NewTicker(3*time.Second)

	go func(){
		defer ticker.Stop()
		for {
			select{
			case <- ctx.Done():
				log.Println("[Cluster] Stopping background gossip worker")
				return
			case <- ticker.C:
				m.EvictDeadPeers(15*time.Second)
				m.gossipRound(seedNodes)
			}
		}
	}()
}

func (m *Manager) gossipRound(seedNodes []string){
	m.mu.RLock()
	peers := []Peer{{ID: m.LocalID, Address: m.LocalAddress}}

	var targetAddresses []string

	for _, p := range m.peers{
		peers = append(peers, *p)
		targetAddresses = append(targetAddresses, p.Address)
	}

	m.mu.RUnlock()

	// If we dont know anyone yet, Then try seed Nodes
	if(len(targetAddresses) == 0){
		targetAddresses = seedNodes
	}

	if(len(targetAddresses) == 0){
		return // Alone
	}

	target := targetAddresses[rand.Intn(len(targetAddresses))]
	m.sendGossip(target, peers)
}

func (m *Manager) sendGossip(target string, peers []Peer){
	conn, err := net.DialTimeout("tcp", target, 2*time.Second)
	if err != nil{
		return // target is offline
	}
	defer conn.Close()

	payloadSize := 2
	for _,p := range peers{
		payloadSize += 4+2+len(p.Address)
	}

	totalMsgSize := payloadSize + 1 // Command bit

	buf := make([]byte, 4+totalMsgSize)

	binary.BigEndian.PutUint32(buf[0:4], uint32(totalMsgSize))
	buf[4] = byte(protocol.GossipCmd)

	offset := 5
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(peers)))
	offset += 2

	for _, p := range peers{
		binary.BigEndian.PutUint32(buf[offset:offset+4], p.ID)
		offset += 4
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(p.Address)))
		offset += 2

		copy(buf[offset:], []byte(p.Address))
		offset += len(p.Address)
	}

	if _, err = conn.Write(buf); err != nil{
		log.Printf("[Cluster] Failed to send gossip to %s: %v", target, err)
		return
	}

	ackBuf := make([]byte, 4)
	io.ReadFull(conn, ackBuf)
}