package main

import (
	"encoding/binary"
	"io"
	"log"
	"net"
)

func main() {
    // Connect to the server
    conn, err := net.Dial("tcp", "localhost:8090")
    if err != nil {
       log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()

    cmdByte := byte(1)
    message := []byte("Hello Broker")

    payload := append([]byte{cmdByte}, message...)
    payloadSize := uint32(len(payload))

    req := make([]byte, 4+payloadSize)
    
    binary.BigEndian.PutUint32(req[0:4], payloadSize)
    copy(req[4:], payload)

    if _, err := conn.Write(req); err !=nil{
        log.Fatalf("Failed to write: %v", err)
    }

    sizeBuf := make([]byte, 4)

    if _, err := io.ReadFull(conn, sizeBuf); err != nil{
        log.Fatalf("Error reading response: %v", err)
    }

    size := binary.BigEndian.Uint32(sizeBuf)

    responseBuf := make([]byte, size)
    
    if _, err := io.ReadFull(conn, responseBuf); err != nil{
        log.Fatalf("Error reading response: %v", err)
    }

    log.Printf("Response from server: %s", string(responseBuf))
}


