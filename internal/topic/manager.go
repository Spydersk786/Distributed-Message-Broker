package topic

import (
	"sort"
	"sync"

	"github.com/Spydersk786/broker/internal/storage"
)

type Topic struct{
	name	 string
	segments []*storage.Segment
	active	 *storage.Segment	
	mu		 sync.RWMutex
}

func NewTopic(name string, dir string) (*Topic,error){
	firstSeg,err := storage.NewSegment(dir, 0)

	if err != nil{
		return nil, err
	}

	return &Topic{
		name: name,
		segments: []*storage.Segment{firstSeg},
		active: firstSeg,
	}, nil
}

func (t *Topic) Append(msg []byte) (uint64, error){
	t.mu.Lock()
	defer t.mu.Unlock()

	offset, err := t.active.Append(msg)
	if err != nil{
		return 0, err
	}

	// 1 GB == 1,073,741,824 bytes
	if t.active.Size() > 1073741824{
		newSeg,err := storage.NewSegment("data/dir", offset+1)
		if err != nil{
			return 0, err
		}
		t.segments = append(t.segments, newSeg)
		t.active = newSeg
	}

	return offset, nil
}

type Manager struct{
	topics	map[string]*Topic
	mu		sync.RWMutex
	dataDir string
}

func NewManager(dataDir string) *Manager{
	return &Manager{
		topics: make(map[string]*Topic),
		dataDir: dataDir,
	}
}

func (t *Topic) findSegment(targetOffset uint64) *storage.Segment{
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.segments) == 0{
		return nil
	}

	i := sort.Search(len(t.segments), func (i int) bool{
		return t.segments[i].BaseOffset() > targetOffset
	})

	if i == 0{
		return t.segments[0]
	}

	return t.segments[i-1]
}

func (m *Manager) GetOrCreate(name string) (*Topic, error){
	m.mu.RLock()

	t, exists := m.topics[name]

	m.mu.RUnlock()

	if exists{
		return t, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double check as a goroutine might have created it 
	// while we were upgrading from RLock to Lock
	if t, exists := m.topics[name]; exists{
		return t, nil
	}

	newTopic, err := NewTopic(name, m.dataDir); if err != nil{
		return nil, err
	}
	m.topics[name] = newTopic
	return newTopic, nil
}