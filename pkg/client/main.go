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

    noOfProduce := 40
    noOfConsume := 10
    topic := "orders.deleted"
    ReadOffset := 14
    
    for i:=0; i<noOfProduce ;i++{
        msgStr := fmt.Sprintf("Order ID: %d", i)
        message := []byte(msgStr)
    
        // PRODUCE THE MESSAGE
        produceCmd := byte(1)
    
        prodPayload := make([]byte, 2+len(topic)+len(message))
        binary.BigEndian.PutUint16(prodPayload[:2], uint16(len(topic)))
        copy(prodPayload[2:], []byte(topic))
        copy(prodPayload[2+len(topic):], message)
    
        prodReq := buildRequest(produceCmd, prodPayload)

        if _, err := conn.Write(prodReq); err != nil{
            log.Fatalf("Failed to write produce req %v", err)
        }

        prodResp := readResponse(conn)
        if len(prodResp) != 8{
            fmt.Printf("Unexpected response size: %d bytes\n", len(prodResp))
        }
        offset := binary.BigEndian.Uint64(prodResp)
        fmt.Printf("Produce Succeeded! offset:%d\n", offset)
    }

    for i:=0; i<noOfConsume ;i++{
        fetchCmd := byte(2)

        fetchPayload := make([]byte, 2+len(topic)+8)
        binary.BigEndian.PutUint16(fetchPayload[:2], uint16(len(topic)))
        copy(fetchPayload[2:], []byte(topic))

        offsetIdx := 2 + len(topic)
        binary.BigEndian.PutUint64(fetchPayload[offsetIdx:offsetIdx+8], uint64(ReadOffset))

        fetchReq := buildRequest(fetchCmd, fetchPayload)
        if _, err := conn.Write(fetchReq); err != nil{
            log.Fatalf("Failed to fetch req %v", err)
        }

        fetchResp := readResponse(conn)
        fmt.Printf("Fetched message payload: '%s'\n", string(fetchResp))
        
        ReadOffset = ReadOffset + 1
    }
}

func buildRequest(cmdByte byte, payload []byte) [] byte{
    finalPayload := append([]byte{cmdByte},payload...)
    payloadSize := uint32(len(finalPayload))

    req := make([]byte, 4+payloadSize)
    
    binary.BigEndian.PutUint32(req[0:4], payloadSize)
    copy(req[4:], finalPayload)
    return req
}

func readResponse(conn net.Conn) []byte{
    sizeBuf := make([]byte, 4)

    if _, err := io.ReadFull(conn, sizeBuf); err != nil{
        log.Fatalf("Error reading response: %v", err)
    }

    size := binary.BigEndian.Uint32(sizeBuf)

    responseBuf := make([]byte, size)
    
    if _, err := io.ReadFull(conn, responseBuf); err != nil{
        log.Fatalf("Error reading response: %v", err)
    }

    return responseBuf
}

