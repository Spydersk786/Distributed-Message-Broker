package topic

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/Spydersk786/broker/internal/storage"
)

type Topic struct{
	name	 	string
	Partitions	map[uint32]*Partition
	mu		 	sync.RWMutex
}

func NewTopic(name string) (*Topic){
	return &Topic{
		name: name,
		Partitions: make(map[uint32]*Partition),
	}
}

func (t *Topic) GetPartition(id uint32) (*Partition, bool){
	t.mu.RLock()
	defer t.mu.RUnlock()

	p, exists := t.Partitions[id]
	return p, exists
}

func (t *Topic) Close() error{
	t.mu.Lock()
	defer t.mu.Unlock()

	var errs []error

	for _, p := range t.Partitions{
		if err := p.Close(); err != nil{
			errs = append(errs, err)
		} 
	}
	return errors.Join(errs...)
}

type Manager struct{
	topics			map[string]*Topic
	mu				sync.RWMutex
	dataDir 		string
	localID			uint32
	totalBrokers 	uint32
}

func NewManager(dataDir string, localID uint32, totalBrokers uint32) (*Manager, error){
	m := &Manager{
		topics: make(map[string]*Topic),
		dataDir: dataDir,
		localID: localID,
		totalBrokers: totalBrokers,
	}

	err := m.recoverState()
	return m, err
}

func (m *Manager) GetLeaderID(topicName string, partitionID uint32) uint32{
	return CalculateLeader(topicName, partitionID, m.totalBrokers)
}

func (m *Manager) GetLocalID() uint32{
	return m.localID
}

func (m *Manager) GetAllPartitions() []*Partition{
	m.mu.RLock()
	defer m.mu.RUnlock()

	var parts []*Partition
	for _,t := range m.topics{
		for _, p := range t.Partitions{
			parts = append(parts, p)
		}
	}

	return parts
}

func (m *Manager) GetOrCreatePartition(name string, partitionID uint32) (*Partition, error){
	m.mu.Lock()
	defer m.mu.Unlock()

	t, exists := m.topics[name]
	if !exists {
		t = NewTopic(name)
		m.topics[name] = t
	}

	p, exists := t.Partitions[partitionID]
	if exists{
		return p, nil
	}

	newPartition, err := NewPartition(name, partitionID, m.dataDir, m.localID, m.totalBrokers)
	if err != nil{
		return nil, err
	}

	t.Partitions[partitionID] = newPartition
	return newPartition, nil
}

func (m *Manager) recoverState() error{
	entries, err := os.ReadDir(m.dataDir)
	if err != nil{
		if os.IsNotExist(err){
			return nil
		}
		return err
	}

	for _, entry := range entries{
		if entry.IsDir(){
			// folder name are like "topic.name-0", "topic.name-1" 
			lastDash := strings.LastIndex(entry.Name(), "-")
			if lastDash == -1{
				continue 
			}

			topicName := entry.Name()[:lastDash]
			partitionIDStr := entry.Name()[lastDash+1:] 

			partitionID, err := strconv.ParseUint(partitionIDStr, 10, 32)
			if err != nil{
				return err
			}

			partition, err := loadPartition(topicName, uint32(partitionID), m.dataDir, m.localID, m.totalBrokers)
			if err != nil{
				return err
			}

			m.mu.Lock()
			t, exists := m.topics[topicName]
			if !exists{
				t = NewTopic(topicName)
				m.topics[topicName] = t
			}
			t.Partitions[uint32(partitionID)] = partition
			m.mu.Unlock()
		}
	}
	return nil
}

func (m *Manager) Close() error{
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error

	for _, t := range m.topics{
		if err := t.Close(); err != nil{
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func loadPartition(topicName string, partitionID uint32, baseDir string, localBrokerID uint32, totalBrokers uint32) (*Partition, error){
	dirName := fmt.Sprintf("%s-%d", topicName, partitionID)
	partitionDir := filepath.Join(baseDir, dirName)
	entries, err := os.ReadDir(partitionDir)
	if err != nil{
		return nil, err
	}

	var segments []*storage.Segment

	for _, entry := range entries{
		if strings.HasSuffix(entry.Name(), ".log"){
			baseName := strings.TrimSuffix(entry.Name(), ".log")
			baseOffset, err := strconv.ParseUint(baseName, 10, 64)
			if err != nil{
				return nil, err
			}

			seg, err := storage.NewSegment(partitionDir, baseOffset)
			if err != nil{
				return nil, err
			}

			segments = append(segments, seg)
		}
	}

	if len(segments) == 0{
		return NewPartition(topicName, partitionID, baseDir, localBrokerID, totalBrokers)
	}

	leaderID := CalculateLeader(topicName, partitionID, totalBrokers)
	role := RoleFollower
	if localBrokerID == leaderID{
		role = RoleLeader
	}

	return &Partition{
		ID: partitionID,
		TopicName: topicName,
		dir: partitionDir,
		Role: role,
		LeaderID: leaderID,
		// Already sorted so no need to sort
		segments: segments,
		active: segments[len(segments)-1],
	}, nil
}