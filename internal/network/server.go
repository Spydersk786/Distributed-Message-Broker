package network

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"fmt"
)

type Server struct{
	listenAddr string
	ln 		   net.Listener
}

func NewServer(listenAddr string) (*Server){
	return &Server{
		listenAddr : listenAddr,
	}
} 

func (s *Server) Start() error{
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	s.ln = ln
	log.Printf("Broker listening on %s\n", s.listenAddr)

	return s.acceptLoop()
}

func (s *Server) acceptLoop() error{
    for {
        conn, err := s.ln.Accept()
        if err != nil {
            log.Printf("Error accepting conn: %v\n", err)
            continue
        }

        go s.handleConnection(conn)
    }
}

func (s *Server) handleConnection(conn net.Conn){
	defer conn.Close()

    payloadSizeBuf := make([]byte, 4)
    
	for {
		_, err := io.ReadFull(conn, payloadSizeBuf)
		if err != nil{
			if err == io.EOF{
				// As no bytes were read, It indicates the client disconnected gracefully
				return 
			}
			log.Printf("Error reading size header: %v\n", err)
			return
		}

		payloadSize := binary.BigEndian.Uint32(payloadSizeBuf)

		if payloadSize > 10*1024*1024{ // 10MB max limit of payload for now
			log.Printf("Payload size %d exceeds limit\n", payloadSize)
			return
		}

		payloadBuf := make([]byte, payloadSize)

		_, err = io.ReadFull(conn, payloadBuf)
		if err != nil{
			log.Printf("Error reading payload: %v\n", err)
			return
		}

		fmt.Printf("Recieved %d bytes: %s\n", payloadSize, string(payloadBuf))
	}
}

