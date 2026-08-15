package cluster

import (
	"testing"
	"time"
)

func TestMergePeersRefreshesLastSeenAndAddress(t *testing.T) {
	m := &Manager{
		LocalID:      1,
		LocalAddress: "localhost:8091",
		peers: map[uint32]*Peer{
			2: {ID: 2, Address: "old:8092", LastSeen: time.Now().Add(-1 * time.Hour)},
		},
	}

	m.MergePeers([]Peer{{ID: 2, Address: "new:8092"}})

	p := m.peers[2]
	if p == nil {
		t.Fatal("peer missing after merge")
	}
	if p.Address != "new:8092" {
		t.Fatalf("expected refreshed address, got %q", p.Address)
	}
	if p.LastSeen.IsZero() {
		t.Fatal("last seen should be set")
	}
}

func TestEvictDeadPeersDoesNotRemoveFreshPeer(t *testing.T) {
	m := &Manager{
		LocalID:      1,
		LocalAddress: "localhost:8091",
		peers: map[uint32]*Peer{
			2: {ID: 2, Address: "alive:8092", LastSeen: time.Now()},
		},
	}

	m.EvictDeadPeers(15 * time.Second)

	if _, exists := m.peers[2]; !exists {
		t.Fatal("fresh peer was evicted")
	}
}
