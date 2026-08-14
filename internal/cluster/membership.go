package cluster

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Spydersk786/broker/internal/protocol"
)

type Peer struct {
	ID       	uint32
	Address  	string
	LastSeen 	time.Time
}

type Manager struct{
	LocalID			uint32
	LocalAddress	string
	peers			map[uint32]*Peer
	mu 				sync.RWMutex
}

func NewClusterManager(id uint32, address string) (*Manager, error){
	return &Manager{
		LocalID: id,
		LocalAddress: address,
		peers: make(map[uint32]*Peer),
	}, nil
}

func (m *Manager) MergePeers(incoming []Peer){
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range incoming{

		if p.ID == m.LocalID{
			continue
		}

		existing, exists := m.peers[p.ID]

		if !exists{
			log.Printf("[Cluster] Discovered new broker: ID=%d Address=%s", p.ID, p.Address)
			m.peers[p.ID] = &Peer{
				ID:p.ID,
				Address:p.Address,
				LastSeen:time.Now(),
			}
		} else{
			existing.LastSeen = time.Now()
		}
	}
}

func (m *Manager) GetAllPeers() []Peer{
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]Peer, 0, len(m.peers)+1)

	list = append(list, Peer{
		ID: m.LocalID,
		Address: m.LocalAddress,
	})

	for _, p := range m.peers{
		list = append(list, *p)
	}

	return list
}

func HandleGossip(clusterMgr *Manager) protocol.Handler{
	return func(payload []byte) ([]byte, error){
		if len(payload) < 2{
			return nil, fmt.Errorf("payload too short for topic length")
		}
		// [0 1](Count)[0 0 0 1](ID)[0 14](Addr len)[localhost:8099]
		idx := 0
		peerCount := binary.BigEndian.Uint16(payload[idx:idx+2])
		idx += 2

		var incomingPeers []Peer

		for i:= uint16(0); i< peerCount; i++{
			if idx+4 > len(payload){
				return nil, fmt.Errorf("buffer overflow reading peer ID")
			}
			peerID := binary.BigEndian.Uint32(payload[idx:idx+4])
			idx += 4

			if idx+2 > len(payload){
				return nil, fmt.Errorf("buffer overflow reading address length")
			}

			addrLen := binary.BigEndian.Uint16(payload[idx:idx+2])
			idx += 2

			if idx+int(addrLen) > len(payload){
				return nil, fmt.Errorf("buffer overflow reading address")
			}

			address := string(payload[idx:idx+int(addrLen)])
			idx += int(addrLen)

			incomingPeers = append(incomingPeers, Peer{
				ID: peerID,
				Address: address,
			})
		}

		clusterMgr.MergePeers(incomingPeers)

		return []byte{}, nil
	}
}

func (m *Manager) EvictDeadPeers(timeout time.Duration){
	m.mu.Lock()
	defer m.mu.Unlock()

	cuttoff := time.Now().Add(-timeout)

	for id, peer := range m.peers{
		if peer.LastSeen.Before(cuttoff){
			log.Printf("[Cluster] Broker %d (%s) is unreachable. Evicting from cluster.", id, peer.Address)
			delete(m.peers, id)
		}
	}
}