package algorithms

import (
	"hms/models"
	"log"
	"net/rpc"
	"time"
)

type Bully struct {
	ID       int
	Nodes    map[int]string
	LeaderID int
	State    models.NodeState
	Callback func(int) // Notify leader change
}

func (b *Bully) StartElection() {
	log.Printf("[Server %d] Starting Election (Bully)", b.ID)
	b.State = models.CANDIDATE

	answered := false
	for id, addr := range b.Nodes {
		if id > b.ID {
			client, err := rpc.Dial("tcp", addr)
			if err == nil {
				var reply models.ElectionReply
				err = client.Call("HMS.Election", models.ElectionArgs{FromID: b.ID}, &reply)
				if err == nil && reply.OK {
					answered = true
				}
				client.Close()
			}
		}
	}

	if !answered {
		b.BecomeLeader()
	} else {
		time.Sleep(1 * time.Second)
		if b.State == models.CANDIDATE {
			b.BecomeLeader()
		}
	}
}

func (b *Bully) BecomeLeader() {
	log.Printf("[Server %d] Becoming Leader", b.ID)
	b.State = models.LEADER
	b.LeaderID = b.ID
	if b.Callback != nil {
		b.Callback(b.ID)
	}

	for id, addr := range b.Nodes {
		if id == b.ID {
			continue
		}
		go func(addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer client.Close()
			var reply models.GenericReply
			client.Call("HMS.Coordinator", models.CoordinatorArgs{LeaderID: b.ID}, &reply)
		}(addr)
	}
}

func (b *Bully) HandleElection(args models.ElectionArgs) {
	log.Printf("[Server %d] Received Election from %d", b.ID, args.FromID)
	go b.StartElection()
}

func (b *Bully) HandleCoordinator(args models.CoordinatorArgs) {
	log.Printf("[Server %d] New Coordinator: %d", b.ID, args.LeaderID)
	b.LeaderID = args.LeaderID
	b.State = models.FOLLOWER
	if b.Callback != nil {
		b.Callback(args.LeaderID)
	}
}
