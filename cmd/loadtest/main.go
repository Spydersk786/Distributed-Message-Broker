package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const (
	Workers       = 50
	MessagesPer   = 2000
	BrokerAddress = "localhost:8090"
	Topic         = "load-test-topic"
)

func main() {
	fmt.Printf("Starting Load Test: %d workers sending %d messages each (Total: %d)...\n", 
		Workers, MessagesPer, Workers*MessagesPer)

	start := time.Now()
	var wg sync.WaitGroup

	// Spin up concurrent producers
	for i:=0;i<Workers;i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runProducer(workerID)
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	totalMessages := float64(Workers * MessagesPer)
	messagesPerSec := totalMessages / duration.Seconds()

	fmt.Println("=====================================")
	fmt.Printf("Load Test Complete in %v\n", duration)
	fmt.Printf("Throughput: %.2f messages/second\n", messagesPerSec)
	fmt.Println("=====================================")
}

func runProducer(workerID int) {
	conn, err := net.Dial("tcp", BrokerAddress)
	if err != nil {
		log.Fatalf("Worker %d failed to connect: %v", workerID, err)
	}
	defer conn.Close()

	msgPayload := []byte(`{"event":"user_signup", "id": 123456789}`)

	for i := 0; i < MessagesPer; i++ {
		payload := make([]byte, 2+len(Topic)+len(msgPayload))
		binary.BigEndian.PutUint16(payload[:2], uint16(len(Topic)))
		copy(payload[2:], Topic)
		copy(payload[2+len(Topic):], msgPayload)

		req := buildRequest(1, payload)

		if _, err := conn.Write(req); err != nil {
			log.Fatalf("Worker %d failed to write: %v", workerID, err)
		}

		readResponse(conn)
	}
}

func buildRequest(cmdByte byte, payload []byte) []byte {
	finalPayload := append([]byte{cmdByte}, payload...)
	size := uint32(len(finalPayload))
	req := make([]byte, 4+size)
	binary.BigEndian.PutUint32(req[0:4], size)
	copy(req[4:], finalPayload)
	return req
}

func readResponse(conn net.Conn) {
	sizeBuf := make([]byte, 4)
	conn.Read(sizeBuf)
	size := binary.BigEndian.Uint32(sizeBuf)
	respBuf := make([]byte, size)
	conn.Read(respBuf)
}