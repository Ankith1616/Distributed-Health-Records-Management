package main

import (
	"fmt"
	"hms/algorithms"
	"hms/models"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Server handles the core HMS logic and coordinates distributed algorithms
type Server struct {
	ID       int
	Nodes    map[int]string
	RA       *algorithms.RicartAgrawala
	Bully    *algorithms.Bully
	Raft     *algorithms.Raft
	Database map[int]models.Patient
	mu       sync.Mutex
}

func (s *Server) LogInfo(format string, v ...interface{}) {
	log.Printf("[Server %d] "+format, append([]interface{}{s.ID}, v...)...)
}

// HMS is the RPC object registered for client and inter-node communication
type HMS struct {
	server *Server
}

// --- Ricart-Agrawala RPCs ---
func (h *HMS) RequestPermission(req models.RicartRequest, reply *models.RicartReply) error {
	h.server.RA.HandleRequest(req, reply)
	return nil
}

func (h *HMS) ReceiveReply(req models.RicartReply, reply *models.RicartReply) error {
	h.server.RA.HandleReply()
	return nil
}

// --- Bully RPCs ---
func (h *HMS) Election(args models.ElectionArgs, reply *models.ElectionReply) error {
	reply.OK = true
	h.server.Bully.HandleElection(args)
	return nil
}

func (h *HMS) Coordinator(args models.CoordinatorArgs, reply *models.GenericReply) error {
	h.server.Bully.HandleCoordinator(args)
	reply.Success = true
	return nil
}

// --- Raft RPCs ---
func (h *HMS) AppendEntries(args models.AppendEntriesArgs, reply *models.AppendEntriesReply) error {
	reply.Success = h.server.Raft.HandleAppendEntries(args)
	return nil
}

// --- HMS Service RPCs ---

func (h *HMS) RegisterPatient(args models.RegisterArgs, reply *models.GenericReply) error {
	h.server.RA.RequestCS()
	defer h.server.RA.ReleaseCS()

	if h.server.Bully.State != models.LEADER {
		reply.Success = false
		reply.Message = fmt.Sprintf("Not the leader. Contact leader %d", h.server.Bully.LeaderID)
		return nil
	}

	p := models.Patient{ID: args.ID, Name: args.Name}
	entry := models.LogEntry{Term: h.server.Raft.Term, Command: "REGISTER", Patient: p}

	if h.server.Raft.Replicate(entry) {
		reply.Success = true
		reply.Message = "Patient registered and replicated."
	} else {
		reply.Success = false
		reply.Message = "Replication failed."
	}
	return nil
}

func (h *HMS) UpdateDiagnosis(args models.DiagnosisArgs, reply *models.GenericReply) error {
	h.server.RA.RequestCS()
	defer h.server.RA.ReleaseCS()

	if h.server.Bully.State != models.LEADER {
		reply.Success = false
		reply.Message = "Not the leader."
		return nil
	}

	models.DBMutex.Lock()
	p, ok := h.server.Database[args.ID]
	models.DBMutex.Unlock()

	if !ok {
		reply.Success = false
		reply.Message = "Patient not found."
		return nil
	}

	p.Diagnosis = args.Diagnosis
	entry := models.LogEntry{Term: h.server.Raft.Term, Command: "DIAGNOSIS", Patient: p}

	if h.server.Raft.Replicate(entry) {
		reply.Success = true
		reply.Message = "Diagnosis updated."
	} else {
		reply.Success = false
		reply.Message = "Replication failed."
	}
	return nil
}

func (h *HMS) GenerateBill(args models.BillArgs, reply *models.GenericReply) error {
	h.server.RA.RequestCS()
	defer h.server.RA.ReleaseCS()

	if h.server.Bully.State != models.LEADER {
		reply.Success = false
		reply.Message = "Not the leader."
		return nil
	}

	models.DBMutex.Lock()
	p, ok := h.server.Database[args.ID]
	models.DBMutex.Unlock()

	if !ok {
		reply.Success = false
		reply.Message = "Patient not found."
		return nil
	}

	p.Bill = args.Amount
	entry := models.LogEntry{Term: h.server.Raft.Term, Command: "BILL", Patient: p}

	if h.server.Raft.Replicate(entry) {
		reply.Success = true
		reply.Message = "Bill generated."
	} else {
		reply.Success = false
		reply.Message = "Replication failed."
	}
	return nil
}

func (h *HMS) GetPatientRecord(args models.GetPatientArgs, reply *models.GetPatientReply) error {
	models.DBMutex.Lock()
	defer models.DBMutex.Unlock()
	p, ok := h.server.Database[args.ID]
	if ok {
		reply.Patient = p
		reply.Success = true
	} else {
		reply.Success = false
	}
	return nil
}

func main() {
	var id int
	var port string
	fmt.Print("Enter Node ID (0, 1, 2): ")
	fmt.Scan(&id)
	fmt.Print("Enter local Port to listen on: ")
	fmt.Scan(&port)

	// LAN CONFIGURATION: Replace these with actual IP addresses of computers in your network.
	// Example: Node 0 on 192.168.1.10, Node 1 on 192.168.1.11, etc.
	nodes := map[int]string{
		0: "192.168.1.10:8080",
		1: "192.168.1.11:8081",
		2: "192.168.1.12:8082",
	}

	// For local testing on one machine, you can keep them as localhost:
	// nodes := map[int]string{
	// 	0: "localhost:8080",
	// 	1: "localhost:8081",
	// 	2: "localhost:8082",
	// }

	s := &Server{
		ID:       id,
		Nodes:    nodes,
		Database: make(map[int]models.Patient),
	}

	s.RA = &algorithms.RicartAgrawala{ID: id, Nodes: nodes}
	s.Bully = &algorithms.Bully{ID: id, Nodes: nodes, LeaderID: -1, State: models.FOLLOWER}
	s.Raft = &algorithms.Raft{
		ID:    id,
		Nodes: nodes,
		Apply: func(e models.LogEntry) {
			models.DBMutex.Lock()
			s.Database[e.Patient.ID] = e.Patient
			models.DBMutex.Unlock()
			s.LogInfo("Commit Applied: %s for Patient %d", e.Command, e.Patient.ID)
		},
	}

	hms := &HMS{server: s}
	rpc.Register(hms)

	// Listen on all interfaces (0.0.0.0)
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	s.LogInfo("HMS Server running on all interfaces at port %s", port)

	// Background threads for leader election / health checks
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if s.Bully.State != models.LEADER {
				if s.Bully.LeaderID != -1 {
					s.mu.Lock()
					leaderAddr := nodes[s.Bully.LeaderID]
					s.mu.Unlock()
					client, err := rpc.Dial("tcp", leaderAddr)
					if err != nil {
						s.LogInfo("Leader %d (%s) timed out, starting election", s.Bully.LeaderID, leaderAddr)
						s.Bully.StartElection()
					} else {
						client.Close()
					}
				} else {
					s.Bully.StartElection()
				}
			}
		}
	}()

	for {
		conn, err := listener.Accept()
		if err == nil {
			go rpc.ServeConn(conn)
		}
	}
}
