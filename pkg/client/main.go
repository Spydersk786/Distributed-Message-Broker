package main

import (
	"encoding/binary"
	"fmt"
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
    topic := "orders.created"
    message := []byte("Order ID: 12345")

    // len("string") returns the number of bytes it would take instead of its actual length
    payload := make([]byte, 2+len(topic)+len(message))
    binary.BigEndian.PutUint16(payload[0:2], uint16(len(topic)))
    copy(payload[2:],[]byte(topic))
    copy(payload[2+len(topic):],message)

    finalPayload := append([]byte{cmdByte},payload...)
    payloadSize := uint32(len(finalPayload))

    req := make([]byte, 4+payloadSize)
    
    binary.BigEndian.PutUint32(req[0:4], payloadSize)
    copy(req[4:], finalPayload)

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

    if len(responseBuf) == 8 {
        offset := binary.BigEndian.Uint64(responseBuf)
        fmt.Printf("Produce Succeeded! offset:%d\n", offset)
    }else{
        fmt.Printf("Unexpected response size: %d bytes\n", size)
    }
}


