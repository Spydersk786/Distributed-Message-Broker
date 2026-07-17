package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

func main() {
    // Connect to the server
    conn, err := net.Dial("tcp", "localhost:8090")
    if err != nil {
        fmt.Println(err)
        return
    }

    payload := []byte("Hello World!")
    payloadLength := uint32(12)
    fmt.Printf("payload length sent:%d", payloadLength)
    buf := new(bytes.Buffer)

    err = binary.Write(buf, binary.BigEndian, payloadLength)
	buf.WriteTo(conn)
    conn.Write(payload)

    // Close the connection
    conn.Close()
}


