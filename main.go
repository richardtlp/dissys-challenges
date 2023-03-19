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

type topologyBody struct {
	Type     string              `json:"type,omitempty"`
	Topology map[string][]string `json:"topology"`
}

var member struct{}

type server struct {
	numbers    []float64
	setNumbers map[float64]struct{}
	topology   map[string][]string

	n          *maelstrom.Node
	numberLock sync.RWMutex
}

func main() {
	s := server{
		numbers:    make([]float64, 0),
		setNumbers: make(map[float64]struct{}),
		topology:   make(map[string][]string),
		n:          maelstrom.NewNode(),
		numberLock: sync.RWMutex{},
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
	s.numberLock.RLock()
	reply := map[string]any{"type": "read_ok", "messages": s.numbers}
	s.numberLock.RUnlock()
	return s.n.Reply(msg, reply)
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	var body topologyBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.topology = body.Topology
	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body broadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	go storeAndBroadcastMessage(s, body, msg)
	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func storeAndBroadcastMessage(s *server, body broadcastBody, msg maelstrom.Message) {
	if !isMessageExisted(s, body.Message) {
		storeMessage(s, body)
		for _, dest := range s.topology[msg.Dest] {
			if dest != msg.Src {
				go broadcastWhileTimeout(s, dest, body)
			}
		}
	}
}

func isMessageExisted(s *server, message float64) bool {
	s.numberLock.RLock()
	_, ok := s.setNumbers[message]
	s.numberLock.RUnlock()
	return ok
}

func storeMessage(s *server, body broadcastBody) {
	s.numberLock.Lock()
	s.numbers = append(s.numbers, body.Message)
	s.setNumbers[body.Message] = member
	s.numberLock.Unlock()
}

func broadcastWhileTimeout(s *server, dest string, body broadcastBody) {
	for {
		if broadcast(s, dest, body) == nil {
			break
		}
	}
}

func broadcast(s *server, dest string, body broadcastBody) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.n.SyncRPC(ctx, dest, body)
	return err
}
