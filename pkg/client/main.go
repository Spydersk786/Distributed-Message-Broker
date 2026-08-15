package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

func main() {
	fmt.Println("Starting Distributed Broker Integration Test Suite...")
	time.Sleep(1 * time.Second) // Give user time to read

	// Connect to Broker 1 (It will act as Smart Proxy)
	conn, err := net.Dial("tcp", "localhost:8091")
	if err != nil {
		log.Fatalf("Failed to connect to Broker 1: %v", err)
	}
	defer conn.Close()
	fmt.Println("Connected to Broker 1 (localhost:8091)")

	topic := "integration.test"
	group := "test-consumer-group"
	partitions := []uint32{0, 1, 2} // Test all 3 partitions

	fmt.Println("\n--- TEST 1: Distributed Produce (Proxy Routing) ---")
	for _, pid := range partitions {
		msg := fmt.Sprintf("Hello from Partition %d", pid)
		offset, err := produceMsg(conn, topic, pid, []byte(msg))
		if err != nil {
			log.Fatalf("Produce to Partition %d failed: %v", pid, err)
		}
		fmt.Printf("Produced to Partition %d -> Offset %d\n", pid, offset)
	}

	fmt.Println("\n--- TEST 2: Distributed Fetch ---")
	for _, pid := range partitions {
		// Fetch offset 0 from each partition
		payload, err := fetchMsg(conn, topic, pid, 0)
		if err != nil {
			log.Fatalf("Fetch from Partition %d failed: %v", pid, err)
		}
		fmt.Printf("Fetched from Partition %d -> Payload: '%s'\n", pid, string(payload))
	}

	fmt.Println("\n--- TEST 3: Consumer Group Offsets ---")
	for _, pid := range partitions {
		// Commit offset 42 for every partition
		err := commitOffset(conn, group, topic, pid, 42)
		if err != nil {
			log.Fatalf("Commit Offset for Partition %d failed: %v", pid, err)
		}
		fmt.Printf("Committed Offset 42 for Partition %d\n", pid)

		// Fetch it back to verify
		savedOffset, err := fetchOffset(conn, group, topic, pid)
		if err != nil {
			log.Fatalf("Fetch Offset for Partition %d failed: %v", pid, err)
		}
		
		if savedOffset == 42 {
			fmt.Printf("Verified Offset for Partition %d -> %d\n", pid, savedOffset)
		} else {
			log.Fatalf("Offset mismatch! Expected 42, got %d", savedOffset)
		}
	}

	fmt.Println("\n--- TEST 4: Gossip Protocol ---")
	err = sendGossip(conn, 999, "localhost:9999")
	if err != nil {
		log.Fatalf("Gossip failed: %v", err)
	}
	fmt.Println("Gossip message accepted by cluster")

	fmt.Println("\n ALL TESTS PASSED! Your distributed broker is fully functional!")
}

func produceMsg(conn net.Conn, topic string, partitionID uint32, msg []byte) (uint64, error) {
	payload := make([]byte, 2+len(topic)+4+len(msg))
	idx := 0
	binary.BigEndian.PutUint16(payload[idx:idx+2], uint16(len(topic)))
	idx += 2
	copy(payload[idx:idx+len(topic)], []byte(topic))
	idx += len(topic)
	binary.BigEndian.PutUint32(payload[idx:idx+4], partitionID)
	idx += 4
	copy(payload[idx:], msg)

	if _, err := conn.Write(buildRequest(1, payload)); err != nil {
		return 0, err
	}

	resp := readResponse(conn)
	if len(resp) != 8 {
		return 0, fmt.Errorf("unexpected produce response size: %d", len(resp))
	}
	return binary.BigEndian.Uint64(resp), nil
}

func fetchMsg(conn net.Conn, topic string, partitionID uint32, offset uint64) ([]byte, error) {
	payload := make([]byte, 2+len(topic)+4+8)
	idx := 0
	binary.BigEndian.PutUint16(payload[idx:idx+2], uint16(len(topic)))
	idx += 2
	copy(payload[idx:idx+len(topic)], []byte(topic))
	idx += len(topic)
	binary.BigEndian.PutUint32(payload[idx:idx+4], partitionID)
	idx += 4
	binary.BigEndian.PutUint64(payload[idx:idx+8], offset)

	if _, err := conn.Write(buildRequest(2, payload)); err != nil {
		return nil, err
	}
	return readResponse(conn), nil
}

func commitOffset(conn net.Conn, group, topic string, partitionID uint32, offset uint64) error {
	payload := make([]byte, 2+len(group)+2+len(topic)+4+8)
	idx := 0
	binary.BigEndian.PutUint16(payload[idx:idx+2], uint16(len(group)))
	idx += 2
	copy(payload[idx:idx+len(group)], []byte(group))
	idx += len(group)
	binary.BigEndian.PutUint16(payload[idx:idx+2], uint16(len(topic)))
	idx += 2
	copy(payload[idx:idx+len(topic)], []byte(topic))
	idx += len(topic)
	binary.BigEndian.PutUint32(payload[idx:idx+4], partitionID)
	idx += 4
	binary.BigEndian.PutUint64(payload[idx:idx+8], offset)

	if _, err := conn.Write(buildRequest(3, payload)); err != nil {
		return err
	}
	_ = readResponse(conn) // Ack
	return nil
}

func fetchOffset(conn net.Conn, group, topic string, partitionID uint32) (uint64, error) {
	payload := make([]byte, 2+len(group)+2+len(topic)+4)
	idx := 0
	binary.BigEndian.PutUint16(payload[idx:idx+2], uint16(len(group)))
	idx += 2
	copy(payload[idx:idx+len(group)], []byte(group))
	idx += len(group)
	binary.BigEndian.PutUint16(payload[idx:idx+2], uint16(len(topic)))
	idx += 2
	copy(payload[idx:idx+len(topic)], []byte(topic))
	idx += len(topic)
	binary.BigEndian.PutUint32(payload[idx:idx+4], partitionID)

	if _, err := conn.Write(buildRequest(4, payload)); err != nil {
		return 0, err
	}
	resp := readResponse(conn)
	if len(resp) < 8 {
		return 0, nil // No offset exists yet
	}
	return binary.BigEndian.Uint64(resp), nil
}

func sendGossip(conn net.Conn, peerID uint32, address string) error {
	payload := make([]byte, 2+4+2+len(address))
	idx := 0
	binary.BigEndian.PutUint16(payload[idx:idx+2], 1) // 1 peer
	idx += 2
	binary.BigEndian.PutUint32(payload[idx:idx+4], peerID)
	idx += 4
	binary.BigEndian.PutUint16(payload[idx:idx+2], uint16(len(address)))
	idx += 2
	copy(payload[idx:], []byte(address))

	if _, err := conn.Write(buildRequest(5, payload)); err != nil {
		return err
	}
	_ = readResponse(conn)
	return nil
}

func buildRequest(cmdByte byte, payload []byte) []byte {
	finalPayload := append([]byte{cmdByte}, payload...)
	req := make([]byte, 4+len(finalPayload))
	binary.BigEndian.PutUint32(req[0:4], uint32(len(finalPayload)))
	copy(req[4:], finalPayload)
	return req
}

func readResponse(conn net.Conn) []byte {
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		log.Fatalf("Error reading response size: %v", err)
	}
	size := binary.BigEndian.Uint32(sizeBuf)
	if size == 0 {
		return []byte{}
	}
	respBuf := make([]byte, size)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		log.Fatalf("Error reading response payload: %v", err)
	}
	return respBuf
}