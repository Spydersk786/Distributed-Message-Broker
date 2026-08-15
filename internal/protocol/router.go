package protocol

import (
	"fmt"
)

type CommandCode byte

const (
	ProduceCmd CommandCode = 1
	FetchCmd   CommandCode = 2
	CommitOffset   CommandCode = 3
	FetchOffset   CommandCode = 4
	GossipCmd	CommandCode = 5
	DiscoverPartitionsCmd CommandCode = 6
)

type Handler func(payload []byte) ([]byte, error)

type Router struct{
	handlers map[CommandCode]Handler
}

func NewRouter() (*Router){
	return &Router{
		handlers: make(map[CommandCode]Handler),
	}
}

func (r *Router) Register(code CommandCode, h Handler){
	r.handlers[code] = h
}

func (r *Router) Route(code CommandCode, payload []byte) ([]byte, error) {
	handler, exists := r.handlers[code]
	if !exists {
		return nil, fmt.Errorf("Unsupported command code %d", code)
	}
	return handler(payload)
}