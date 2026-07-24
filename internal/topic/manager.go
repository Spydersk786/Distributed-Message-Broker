package topic

import (
	"sync"
)

type Topic struct{
	messages [][]byte
	mu		 sync.Mutex
}

func NewTopic() *Topic{
	return &Topic{
		messages: make([][]byte, 0),
	}
}

func (t *Topic) Append(msg []byte) int{
	t.mu.Lock()
	defer t.mu.Unlock()

	offset := len(t.messages)
	t.messages = append(t.messages, msg)
	return offset
}

type Manager struct{
	topics	map[string]*Topic
	mu		sync.RWMutex
}

func NewManager() *Manager{
	return &Manager{
		topics: make(map[string]*Topic),
	}
}

func (m *Manager) GetOrCreate(name string) *Topic{
	m.mu.RLock()

	t, exists := m.topics[name]

	m.mu.RUnlock()

	if exists{
		return t
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double check as a goroutine might have created it 
	// while we were upgrading from RLock to Lock
	if t, exists := m.topics[name]; exists{
		return t
	}

	newTopic := NewTopic()
	m.topics[name] = newTopic
	return newTopic
}