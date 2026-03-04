package algorithms

import (
	"hms/models"
	"log"
	"net/rpc"
	"sync"
	"time"
)

type RicartAgrawala struct {
	ID          int
	Nodes       map[int]string
	Clock       int
	Requesting  bool
	Replies     int
	Deferred    []int
	InCS        bool
	ServiceName string // Added to allow different RPC service names (e.g., ClientRA or HMS)
	mu          sync.Mutex
	replyMu     sync.Mutex
}

func (ra *RicartAgrawala) RequestCS() {
	ra.mu.Lock()
	ra.Clock++
	ra.Requesting = true
	ra.Replies = 0
	timestamp := ra.Clock
	ra.mu.Unlock()

	log.Printf("[Client %d] Requesting CS (RA) with timestamp %d", ra.ID, timestamp)

	var wg sync.WaitGroup
	for id, addr := range ra.Nodes {
		if id == ra.ID {
			continue
		}
		wg.Add(1)
		go func(id int, addr string) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer client.Close()
			var reply models.RicartReply
			// Use the ServiceName for routing RPCs
			serviceCall := ra.ServiceName + ".RequestPermission"
			err = client.Call(serviceCall, models.RicartRequest{Timestamp: timestamp, NodeID: ra.ID}, &reply)
			if err == nil {
				ra.HandleReply()
			}
		}(id, addr)
	}

	for {
		ra.replyMu.Lock()
		if ra.Replies >= len(ra.Nodes)-1 {
			ra.replyMu.Unlock()
			break
		}
		ra.replyMu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	ra.mu.Lock()
	ra.InCS = true
	ra.mu.Unlock()
	log.Printf("[Client %d] Entering CS", ra.ID)
}

func (ra *RicartAgrawala) ReleaseCS() {
	ra.mu.Lock()
	ra.InCS = false
	ra.Requesting = false
	deferred := ra.Deferred
	ra.Deferred = []int{}
	ra.mu.Unlock()

	log.Printf("[Client %d] Leaving CS, replying to %v", ra.ID, deferred)

	for _, id := range deferred {
		addr := ra.Nodes[id]
		go func(id int, addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer client.Close()
			var reply models.RicartReply
			serviceCall := ra.ServiceName + ".ReceiveReply"
			client.Call(serviceCall, models.RicartReply{NodeID: ra.ID}, &reply)
		}(id, addr)
	}
}

func (ra *RicartAgrawala) HandleRequest(req models.RicartRequest, reply *models.RicartReply) bool {
	ra.mu.Lock()
	defer ra.mu.Unlock()

	if ra.Clock < req.Timestamp {
		ra.Clock = req.Timestamp
	}
	ra.Clock++

	if ra.InCS || (ra.Requesting && (req.Timestamp > ra.Clock || (req.Timestamp == ra.Clock && req.NodeID > ra.ID))) {
		ra.Deferred = append(ra.Deferred, req.NodeID)
		return true // Defer
	}
	reply.NodeID = ra.ID
	return false // Reply now
}

func (ra *RicartAgrawala) HandleReply() {
	ra.replyMu.Lock()
	ra.Replies++
	ra.replyMu.Unlock()
	log.Printf("[Client %d] Received reply", ra.ID)
}
