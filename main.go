package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
	"time"
)

type broadcastBody struct {
	Type    string  `json:"type,omitempty"`
	Message float64 `json:"message"`
}

type broadcastBatchBody struct {
	Type     string    `json:"type,omitempty"`
	Messages []float64 `json:"messages"`
}

var member struct{}

type server struct {
	messages     map[float64]struct{}
	messagesLock sync.RWMutex

	messageBatch     map[string][]float64 // key: dest, value: message slice
	messageBatchLock sync.RWMutex

	topology map[string][]string

	n *maelstrom.Node
}

const MaxRetry = 100

func main() {
	s := server{
		messages:     make(map[float64]struct{}),
		messagesLock: sync.RWMutex{},

		messageBatch:     make(map[string][]float64),
		messageBatchLock: sync.RWMutex{},

		topology: make(map[string][]string),

		n: maelstrom.NewNode(),
	}

	go s.broadcastJob()

	s.n.Handle("read", s.readHandler)
	s.n.Handle("topology", s.topologyHandler)
	s.n.Handle("broadcast", s.broadcastHandler)
	s.n.Handle("broadcast_batch", s.broadcastBatchHandler)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) broadcastJob() {
	for {
		select {
		case <-time.After(600 * time.Millisecond):
			newMessagesBatch := s.getCopyOfMessageBatch()
			s.broadcastMessageBatch(newMessagesBatch)
		}
	}
}

func (s *server) getCopyOfMessageBatch() map[string][]float64 {
	s.messageBatchLock.RLock()
	defer s.messageBatchLock.RUnlock()

	copiedMessageBatch := make(map[string][]float64)
	for key, value := range s.messageBatch {
		newValue := make([]float64, len(value))
		copy(newValue, value)
		copiedMessageBatch[key] = newValue
	}
	return copiedMessageBatch
}

func (s *server) broadcastMessageBatch(newMessagesBatch map[string][]float64) {
	for dest, messages := range newMessagesBatch {
		go s.sendWithRetries(dest, broadcastBatchBody{
			Type:     "broadcast_batch",
			Messages: messages,
		})
	}
}

func (s *server) sendWithRetries(dest string, body broadcastBatchBody) {
	for i := 0; i < MaxRetry; i++ {
		if s.sendWithTimeout(dest, body) == nil {
			break
		} else {
			time.Sleep(time.Second) // avoid putting too much pressure on the failed node
		}
	}
}

func (s *server) sendWithTimeout(dest string, body broadcastBatchBody) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := s.n.SyncRPC(ctx, dest, body)
	return err
}

func (s *server) readHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	return s.n.Reply(msg, s.createReplyFromMessages())
}

func (s *server) createReplyFromMessages() map[string]any {
	s.messagesLock.RLock()
	defer s.messagesLock.RUnlock()

	messagesSlice := make([]float64, 0)
	for key := range s.messages {
		messagesSlice = append(messagesSlice, key)
	}
	return map[string]any{"type": "read_ok", "messages": messagesSlice}
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	// define the custom topology: node 0 connects to all other nodes
	// reason: it takes at most 2 hops for a message to be broadcast between any two nodes
	// and on average, each node will send 2 message per broadcast operation
	if len(s.n.NodeIDs()) != 0 {
		bridge := s.n.NodeIDs()[0]
		s.topology[bridge] = s.n.NodeIDs()[1:]
		for _, node := range s.n.NodeIDs()[1:] {
			s.topology[node] = []string{bridge}
		}
	}
	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body broadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	go s.storeAndBatchMessages([]float64{body.Message}, msg)
	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) broadcastBatchHandler(msg maelstrom.Message) error {
	var body broadcastBatchBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	go s.storeAndBatchMessages(body.Messages, msg)
	return s.n.Reply(msg, map[string]any{"type": "broadcast_batch_ok"})
}

func (s *server) storeAndBatchMessages(messages []float64, msg maelstrom.Message) {
	nonExistedMessages := s.getNonExistedMessages(messages)
	s.storeMessages(nonExistedMessages)
	s.batchMessages(msg, nonExistedMessages)
}

func (s *server) getNonExistedMessages(messages []float64) []float64 {
	nonExistedMessages := make([]float64, 0)
	for _, message := range messages {
		if !s.isMessageExisted(message) {
			nonExistedMessages = append(nonExistedMessages, message)
		}
	}
	return nonExistedMessages
}

func (s *server) storeMessages(messages []float64) {
	s.messagesLock.Lock()
	defer s.messagesLock.Unlock()

	for _, message := range messages {
		s.messages[message] = member
	}
}

func (s *server) batchMessages(msg maelstrom.Message, nonExistedMessages []float64) {
	s.messageBatchLock.Lock()
	defer s.messageBatchLock.Unlock()

	for _, dest := range s.topology[msg.Dest] {
		if dest != msg.Src {
			s.messageBatch[dest] = append(s.messageBatch[dest], nonExistedMessages...)
		}
	}
}

func (s *server) isMessageExisted(message float64) bool {
	s.messagesLock.RLock()
	defer s.messagesLock.RUnlock()

	_, ok := s.messages[message]
	return ok
}
