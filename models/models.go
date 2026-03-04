package models

import "sync"

// Patient structure
type Patient struct {
	ID        int
	Name      string
	Diagnosis string
	Bill      float64
}

// Message types
const (
	MSG_REQUEST        = "REQUEST"
	MSG_REPLY          = "REPLY"
	MSG_ELECTION       = "ELECTION"
	MSG_OK             = "OK"
	MSG_COORDINATOR    = "COORDINATOR"
	MSG_APPEND_ENTRIES = "APPEND_ENTRIES"
)

// RPC Arguments and Replies

type RegisterArgs struct {
	ID   int
	Name string
}

type DiagnosisArgs struct {
	ID        int
	Diagnosis string
}

type BillArgs struct {
	ID     int
	Amount float64
}

type GenericReply struct {
	Success bool
	Message string
}

type GetPatientArgs struct {
	ID int
}

type GetPatientReply struct {
	Patient Patient
	Success bool
}

// Ricart-Agrawala Structs
type RicartRequest struct {
	Timestamp int
	NodeID    int
}

type RicartReply struct {
	NodeID int
}

// Bully Algorithm Structs
type ElectionArgs struct {
	FromID int
}

type ElectionReply struct {
	OK bool
}

type CoordinatorArgs struct {
	LeaderID int
}

// Replication Structs
type LogEntry struct {
	Term    int
	Command string
	Patient Patient
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Server State
type NodeState int

const (
	FOLLOWER NodeState = iota
	CANDIDATE
	LEADER
)

// Server configuration
type ServerInfo struct {
	ID      int
	Address string
	Nodes   map[int]string
}

// Shared Mutex for DB access
var DBMutex sync.Mutex
