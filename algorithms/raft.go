package algorithms

import (
	"hms/models"
	"net/rpc"
	"sync"
	"time"
)

type Raft struct {
	ID        int
	Nodes     map[int]string
	Term      int
	Log       []models.LogEntry
	CommitIdx int
	State     models.NodeState
	mu        sync.Mutex
	Apply     func(models.LogEntry)
}

func (r *Raft) Replicate(entry models.LogEntry) bool {
	r.mu.Lock()
	r.Log = append(r.Log, entry)
	r.mu.Unlock()

	successCount := 1
	var wg sync.WaitGroup
	for id, addr := range r.Nodes {
		if id == r.ID {
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
			var reply models.AppendEntriesReply
			err = client.Call("HMS.AppendEntries", models.AppendEntriesArgs{
				Term:         r.Term,
				LeaderID:     r.ID,
				Entries:      []models.LogEntry{entry},
				LeaderCommit: r.CommitIdx,
			}, &reply)
			if err == nil && reply.Success {
				r.mu.Lock()
				successCount++
				r.mu.Unlock()
			}
		}(id, addr)
	}

	time.Sleep(200 * time.Millisecond)
	if successCount > len(r.Nodes)/2 {
		r.CommitIdx++
		if r.Apply != nil {
			r.Apply(entry)
		}
		return true
	}
	return false
}

func (r *Raft) HandleAppendEntries(args models.AppendEntriesArgs) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if args.Term >= r.Term {
		r.Term = args.Term
		r.State = models.FOLLOWER
		if len(args.Entries) > 0 {
			r.Log = append(r.Log, args.Entries...)
			for _, entry := range args.Entries {
				if r.Apply != nil {
					r.Apply(entry)
				}
			}
		}
		return true
	}
	return false
}
