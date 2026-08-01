package topic

import (
	"bytes"
	"testing"
)

func TestTopicAppendAndRead(t *testing.T){
	testDir := t.TempDir()

	tm, err := NewManager(testDir)
	if err != nil{
		t.Fatalf("Failed to create manager: %v", err)
	}

	topicName := "test_topic"
	testTopic, err := tm.GetOrCreate(topicName)
	if err != nil{
		t.Fatalf("Failed to create topic: %v", err)
	}

	defer testTopic.Close()
	
	msg1 := []byte("hello kafka")
	msg2 := []byte("this is first message")
	
	offset1, err := testTopic.Append(msg1)
	if err != nil{
		t.Fatalf("Failed to append msg1: %v", err)
	}
	if offset1 != 0{
		t.Fatalf("Expected first offset to be 0, got %d", offset1)
	}

	offset2, err := testTopic.Append(msg2)
	if err != nil{
		t.Fatalf("Failed to append msg2: %v", err)
	}
	if offset2 != 1{
		t.Fatalf("Expected first offset to be 1, got %d", offset2)	
	}

	readMsg1, err := testTopic.Read(0)
	if err != nil{
		t.Fatalf("Failed to read msg1: %v", err)
	}

	if !bytes.Equal(readMsg1, msg1){
		t.Errorf("Expected %s, got %s", string(msg1), string(readMsg1))
	}

	readMsg2, err := testTopic.Read(1)
	if err != nil{
		t.Fatalf("Failed to read msg2: %v", err)
	}
	
	if !bytes.Equal(readMsg2, msg2){
		t.Errorf("Expected %s, got %s", string(msg1), string(readMsg1))
	}
}