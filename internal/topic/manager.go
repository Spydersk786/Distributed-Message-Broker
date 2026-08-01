package topic

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Spydersk786/broker/internal/storage"
)

type Topic struct{
	name	 string
	dir 	 string
	segments []*storage.Segment
	active	 *storage.Segment	
	mu		 sync.RWMutex
}

func NewTopic(name string, baseDir string) (*Topic, error){
	topicDir := filepath.Join(baseDir, name)
	if err:= os.MkdirAll(topicDir, 0755); err != nil{
		return nil, err
	}

	firstSeg,err := storage.NewSegment(topicDir, 0)

	if err != nil{
		return nil, err
	}

	return &Topic{
		name: name,
		dir: topicDir,
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

	if t.active.Size() > 250{   // Used for testing functionality
	// 1 GB == 1,073,741,824 bytes
	// if t.active.Size() > 1073741824{
		newSeg, err := storage.NewSegment(t.dir, offset+1)
		if err != nil{
			return 0, err
		}
		t.segments = append(t.segments, newSeg)
		t.active = newSeg
	}

	return offset, nil
}

func (t *Topic) Read(offset uint64) ([]byte, error){
	seg := t.findSegment(offset)
	if seg == nil{
		return nil, fmt.Errorf("offset %d not found in topic %s", offset, t.name)
	}

	return seg.Read(offset)
}

func (t *Topic) Close() error{
	t.mu.Lock()
	defer t.mu.Unlock()

	var errs []error

	for _, seg := range t.segments{
		if err := seg.Close(); err != nil{
			errs = append(errs, err)
		} 
	}
	return errors.Join(errs...)
}

type Manager struct{
	topics	map[string]*Topic
	mu		sync.RWMutex
	dataDir string
}

func NewManager(dataDir string) (*Manager, error){
	m := &Manager{
		topics: make(map[string]*Topic),
		dataDir: dataDir,
	}

	err := m.recoverState()
	return m, err
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
			topicName := entry.Name()
			topic, err := loadTopic(topicName, m.dataDir) 
			if err != nil{
				return err
			}
			m.topics[topicName] = topic
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

func loadTopic(name string, baseDir string) (*Topic, error){
	topicDir := filepath.Join(baseDir, name)
	entries, err := os.ReadDir(topicDir)
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

			seg, err := storage.NewSegment(topicDir, baseOffset)
			if err != nil{
				return nil, err
			}

			segments = append(segments, seg)
		}
	}

	if len(segments) == 0{
		return NewTopic(name, baseDir)
	}

	return &Topic{
		name: name,
		dir: topicDir,
		// Already sorted so no need to sort
		segments: segments,
		active: segments[len(segments)-1],
	}, nil
}