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

    noOfProduce := 3
    noOfConsume := 15
    topic := "orders.created"
    
    for i:=0; i<noOfProduce ;i++{
        msgStr := fmt.Sprintf("Order ID: %d", (40+i))
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

    group := "payment-service"

    for i:=0; i<noOfConsume ;i++{
        currentOffset, err := fetchOffset(conn, group, topic)
        if err != nil{
            log.Fatalf("Failed to fetch offest %v", err)
        }
        fetchCmd := byte(2)

        fetchPayload := make([]byte, 2+len(topic)+8)
        binary.BigEndian.PutUint16(fetchPayload[:2], uint16(len(topic)))
        copy(fetchPayload[2:], []byte(topic))

        offsetIdx := 2 + len(topic)
        binary.BigEndian.PutUint64(fetchPayload[offsetIdx:offsetIdx+8], uint64(currentOffset))

        fetchReq := buildRequest(fetchCmd, fetchPayload)
        if _, err := conn.Write(fetchReq); err != nil{
            log.Fatalf("Failed to fetch req %v", err)
        }

        fetchResp := readResponse(conn)
        fmt.Printf("Fetched message payload: '%s'\n", string(fetchResp))
        
        currentOffset, err = commitOffset(conn, currentOffset, group, topic)
        if err != nil{
            log.Fatalf("Failed to commit the offset %v", err)
        }
    }

    address := "localhost:9000"
    payload := make([]byte , 2+4+2+len(address))
    idx := 0
    binary.BigEndian.PutUint16(payload[idx:idx+2],uint16(1))
    idx += 2
    binary.BigEndian.PutUint32(payload[idx:idx+4],uint32(2))
    idx += 4
    binary.BigEndian.PutUint16(payload[idx:idx+2],uint16(len(address)))
    idx += 2
    copy(payload[idx:], []byte(address))
    if _, err := conn.Write(buildRequest(5, payload)); err != nil{
        log.Fatalf("Failed to gossip: %v", err)
    }

    gossipResponse := readResponse(conn)
    if(len(gossipResponse) != 0){
        log.Println("Unexpected Response")
    }else {
        log.Println("Gossip Succeeded")
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

func fetchOffset(conn net.Conn, group string, topic string) (int64, error){
    fetchOffCmd := byte(4)
    fetchOffPayload := make([]byte, 2+len(group)+len(topic))
    binary.BigEndian.PutUint16(fetchOffPayload[:2], uint16(len(group)))
    copy(fetchOffPayload[2:2+len(group)], group)
    copy(fetchOffPayload[2+len(group):], topic)

    if _, err := conn.Write(buildRequest(fetchOffCmd, fetchOffPayload)); err != nil{
        log.Fatalf("Failed to Write fetch offset req: %v", err)
    }

    fetchOffResp := readResponse(conn)
    currentOffset := binary.BigEndian.Uint64(fetchOffResp)
    fmt.Printf("Group '%s' is at offset '%d' \n", group, currentOffset)
    return int64(currentOffset), nil
}

func commitOffset(conn net.Conn, currentOffset int64, group string, topic string) (int64, error){
    nextOffset := currentOffset + 1
    commitCmd := byte(3)


    commitPayload := make([]byte, 2+len(group)+2+len(topic)+8)
    idx := 0
    binary.BigEndian.PutUint16(commitPayload[idx:idx+2], uint16(len(group)))
    idx += 2
    copy(commitPayload[idx:idx+len(group)], []byte(group))
    idx += len(group)

    binary.BigEndian.PutUint16(commitPayload[idx:idx+2], uint16(len(topic)))
    idx += 2
    copy(commitPayload[idx:idx+len(topic)], []byte(topic))
    idx += len(topic)

    binary.BigEndian.PutUint64(commitPayload[idx:], uint64(nextOffset))

    if _, err := conn.Write(buildRequest(commitCmd, commitPayload)); err != nil{
        return currentOffset, err
    }

    commitResp := readResponse(conn)
    if commitResp[0] == 1{
        fmt.Printf("Successfully committed next offset: %d \n", nextOffset)
    }

    return nextOffset, nil
}
