package topic

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"sort"
	"errors"

	"github.com/Spydersk786/broker/internal/storage"
)

type ReplicaRole int

const (
	RoleLeader ReplicaRole = iota
	RoleFollower
)

type Partition struct{
	ID			uint32
	TopicName	string
	dir			string

	Role		ReplicaRole
	LeaderID	uint32

	segments	[]*storage.Segment
	active 		*storage.Segment
	mu			sync.RWMutex
}

// Temprorary Leader determination
func CalculateLeader(topicName string, partitionID uint32, totalBrokers uint32) uint32{
	h := fnv.New32a()
	h.Write([]byte(topicName))
	hashVal := h.Sum32()

	return ((hashVal + partitionID) % totalBrokers) + 1
}

func NewPartition(topicName string, id uint32, baseDir string, localBrokerID uint32, totalBrokers uint32) (*Partition, error){
	dirName := fmt.Sprintf("%s-%d",topicName,id)
	partitionDir := filepath.Join(baseDir, dirName)

	if err := os.MkdirAll(partitionDir, 0755); err != nil{
		return nil, err
	}

	firstSeg, err := storage.NewSegment(partitionDir, 0)
	if err != nil{
		return nil, err
	}

	leaderID := CalculateLeader(topicName, id, totalBrokers)
	role := RoleFollower
	if leaderID == localBrokerID{
		role = RoleLeader
	}

	return &Partition{
		ID: id,
		TopicName: topicName,
		dir: partitionDir,
		Role: role,
		LeaderID: leaderID,
		segments: []*storage.Segment{firstSeg},
		active: firstSeg,
	}, nil
}

// What to ask the leader
func (p *Partition) NextOffset() uint64{
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active.NextOffset()
}

func (p *Partition) AppendFollower(msg []byte) error{
	p.mu.Lock()
	defer p.mu.Unlock()

	offset, err := p.active.Append(msg)
	if err != nil{
		return err
	}

	if p.active.Size() > 250{   // Used for testing functionality
	// 1 GB == 1,073,741,824 bytes
	// if t.active.Size() > 1073741824{
		newSeg, err := storage.NewSegment(p.dir, offset+1)
		if err != nil{
			return err
		}
		p.segments = append(p.segments, newSeg)
		p.active = newSeg
	}
	return nil
}

func (p *Partition) Append(msg []byte) (uint64, error){
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.Role != RoleLeader{
		return 0, fmt.Errorf("NotLeaderForPartition: current leader is Broker %d", p.LeaderID)
	}

	offset, err := p.active.Append(msg)
	if err != nil{
		return 0, err
	}

	if p.active.Size() > 250{   // Used for testing functionality
	// 1 GB == 1,073,741,824 bytes
	// if t.active.Size() > 1073741824{
		newSeg, err := storage.NewSegment(p.dir, offset+1)
		if err != nil{
			return 0, err
		}
		p.segments = append(p.segments, newSeg)
		p.active = newSeg
	}

	return offset, nil
}

func (p *Partition) Read(offset uint64) ([]byte, error){
	seg := p.findSegment(offset)
	if seg == nil{
		return nil, fmt.Errorf("Offset %d not found in partition %s-%d", offset, p.TopicName, p.ID)
	}

	return seg.Read(offset)
}

func (p *Partition) findSegment(targetOffset uint64) *storage.Segment{
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.segments) == 0{
		return nil
	}

	i := sort.Search(len(p.segments), func (i int) bool{
		return p.segments[i].BaseOffset() > targetOffset
	})

	if i == 0{
		return p.segments[0]
	}

	return p.segments[i-1]
}

func (p *Partition) Close() error{
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error

	for _, seg := range p.segments{
		if err := seg.Close(); err != nil{
			errs = append(errs, err)
		} 
	}
	return errors.Join(errs...)
}