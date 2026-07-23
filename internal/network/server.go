package network

import (
	"context"
	"encoding/binary"
	"io"
	"log"
	"net"
	"sync"

	"github.com/Spydersk786/broker/internal/protocol"
)

type Server struct{
	listenAddr string
	ln 		   net.Listener
	// struct{} used as this channel doesn't need to pass any data and is just used for signaling
	// struct{} is a 0 Byte signal reducing unnecessary memory allocation and garbage collection
	quit 	   chan struct{}
	wg 		   sync.WaitGroup
	route 	   *protocol.Router
}

func NewServer(listenAddr string, router *protocol.Router) *Server {
	return &Server{
		listenAddr : listenAddr,
		quit : make(chan struct{}),
		route: router,
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
			// See docs/references.md
			select{
			case <-s.quit:
				return nil
			default:
				log.Printf("Accept error: %v\n", err)
			}
        }

		log.Printf("New Connection from: %s\n", conn.RemoteAddr())
		// Track the connection before spinning up the goroutine
		s.wg.Add(1)
        go s.handleConnection(conn)
    }
}

func (s *Server) handleConnection(conn net.Conn){
	defer s.wg.Done()
	defer conn.Close()
	defer log.Printf("Connection closed: %s\n",conn.RemoteAddr())

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

		if len(payloadBuf) < 1{
			log.Println("Payload too short to contain command code")
			return
		}

		cmdCode := protocol.CommandCode(payloadBuf[0])
		actualPayload := payloadBuf[1:]

		respPayload, err := s.route.Route(cmdCode,actualPayload)
		if err != nil {
			log.Printf("Routing error: %v\n", err)
			continue
		}

		respSize := uint32(len(respPayload))
		
		// Buffur that can accomodate both the header that contains response length for client
		// and the actual response
		responseBuf := make([]byte, 4 + respSize)
		
		binary.BigEndian.PutUint32(responseBuf[0:4], respSize)
		
		copy(responseBuf[4:], respPayload)

		if _, err := conn.Write(responseBuf); err != nil{
			log.Printf("Error writting response: %v\n", err)
			return
		}
	}
}

func (s *Server) Shutdown(ctx context.Context) error{
	log.Println("Initiating gracefull Shutdown")

	// Signal the accept loop to stop treating Accept errors as anomalies
	close(s.quit)

	err := s.ln.Close()

	done := make(chan struct{})

	go func ()  {
		s.wg.Wait()
		close(done)
	}()

	select{
	case <-done:
		log.Println("All active connections finished gracefully.")
		return err
	case <-ctx.Done():
		log.Println("Shutdown timeout exceeded. Forcing exit.")
		return ctx.Err()
	}
}
