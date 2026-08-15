package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type Segment struct{
	mu				sync.RWMutex
	baseOffset		uint64  	// The ID of first message in segment
	nextOffset      uint64  	// The ID for the next message to be written
	logFile 		*os.File	// Log file that contains the actual messages
	indexFile 		*os.File 	// Index file for the corresponding log file for O(1) lookup of messages
	currentPos		uint32		// Current byte size of the file(where to append next)
}

func NewSegment(dir string, baseOffset uint64) (*Segment, error){
	logPath :=  fmt.Sprintf("%s/%08d.log", dir, baseOffset)
	indexPath := fmt.Sprintf("%s/%08d.index",dir, baseOffset)

	logFile, err := os.OpenFile(logPath, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil{
		return nil, err
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil{
		return nil, err
	}

	stat, _ := logFile.Stat()
	currentPos := uint32(stat.Size())

	idxStat,_ := indexFile.Stat()
	// idx file stores offsets of messages we can directly go to
	// 8*(num of message required) byte to get its offset in log file
	// Above is possible because of dense indexing that we are following.
	// While in case of sparse indexing which is more common and practical
	// A binary search followed by a linear search would be required 
	numEntries := (uint64(idxStat.Size())/8) 

	return &Segment{
		baseOffset: baseOffset,
		nextOffset: baseOffset+numEntries,
		logFile: logFile,
		indexFile: indexFile,
		currentPos: currentPos,
	}, nil
}

func (s *Segment) Append(msg []byte) (uint64, error){
	s.mu.Lock()
	defer s.mu.Unlock()

	// We will store the msg with its length then actual message
	size := uint32(len(msg))
	record := make([]byte, 4 + size) // Bug fix

	binary.BigEndian.PutUint32(record[0:4],size)
	copy(record[4:],msg)

	if _, err := s.logFile.Write(record); err != nil{
		return 0, err
	}

	idxEntry := make([]byte, 8)
	// Relative offset stored to save memory on cost of CPU cycles
	relativeOffset := uint32(s.nextOffset - s.baseOffset)
	binary.BigEndian.PutUint32(idxEntry[:4],relativeOffset)
	binary.BigEndian.PutUint32(idxEntry[4:],s.currentPos)

	if _, err := s.indexFile.Write(idxEntry); err != nil{
		return 0, err
	}

	offsetToReturn := s.nextOffset
	// Update in in-memory struct
	s.nextOffset++
	s.currentPos += uint32(len(record))
	
	return offsetToReturn, nil
}

func (s *Segment) Read(offset uint64) ([]byte, error){
	s.mu.RLock()
	defer s.mu.RUnlock()

	if offset < s.baseOffset || offset >= s.nextOffset{
		return nil, fmt.Errorf("offset %d out of bound of segment", offset)
	}

	relativeOffset := offset - s.baseOffset

	// The starting idx of position of message 
	indexPos := int64(relativeOffset*8 + 4)
	posBuf := make([]byte, 4)
	if _, err := s.indexFile.ReadAt(posBuf, indexPos); err != nil{
		return nil, err
	}

	logPosition := binary.BigEndian.Uint32(posBuf)
	sizeBuf := make([]byte, 4)
	if _, err := s.logFile.ReadAt(sizeBuf, int64(logPosition)); err != nil{
		return nil, err
	}

	msgSize := binary.BigEndian.Uint32(sizeBuf)
	msgBuf := make([]byte, msgSize)
	_, err := s.logFile.ReadAt(msgBuf, int64(logPosition)+4)
	if err != nil{
		return nil, fmt.Errorf("failed to read msg payload: %v", err)
	}

	return msgBuf, nil
}

func (s *Segment) Size() uint32{
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentPos
}

// It remains the same concurreny wouldn't effect it
func (s *Segment) BaseOffset() uint64{
	return s.baseOffset
}

func (s *Segment) NextOffset() uint64{
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

func (s *Segment) Close() error{
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.logFile.Close(); err != nil{
		return err
	}

	if err := s.indexFile.Close(); err != nil{
		return err
	}

	return nil
}