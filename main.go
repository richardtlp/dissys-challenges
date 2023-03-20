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

var member struct{}

type server struct {
	messages     map[float64]struct{}
	messagesLock sync.RWMutex

	topology map[string][]string

	n *maelstrom.Node
}

const MaxRetry = 100

func main() {
	s := server{
		messages:     make(map[float64]struct{}),
		messagesLock: sync.RWMutex{},

		topology: make(map[string][]string),

		n: maelstrom.NewNode(),
	}

	s.n.Handle("read", s.readHandler)
	s.n.Handle("topology", s.topologyHandler)
	s.n.Handle("broadcast", s.broadcastHandler)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
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
	go s.storeAndBroadcastMessage(body, msg)
	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) storeAndBroadcastMessage(body broadcastBody, msg maelstrom.Message) {
	if !s.isMessageExisted(body.Message) {
		s.storeMessage(body)
		for _, dest := range s.topology[msg.Dest] {
			if dest != msg.Src {
				go s.broadcastWhileTimeout(dest, body)
			}
		}
	}
}

func (s *server) broadcastWhileTimeout(dest string, body broadcastBody) {
	for i := 0; i < MaxRetry; i++ {
		if s.broadcast(dest, body) == nil {
			break
		} else {
			time.Sleep(time.Second) // avoid putting too much pressure on the failed node
		}
	}
}

func (s *server) broadcast(dest string, body broadcastBody) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := s.n.SyncRPC(ctx, dest, body)
	return err
}

func (s *server) isMessageExisted(message float64) bool {
	s.messagesLock.RLock()
	defer s.messagesLock.RUnlock()

	_, ok := s.messages[message]
	return ok
}

func (s *server) storeMessage(body broadcastBody) {
	s.messagesLock.Lock()
	defer s.messagesLock.Unlock()

	s.messages[body.Message] = member
}
