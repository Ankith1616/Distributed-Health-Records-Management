// Package models defines all shared data structures used across the distributed system.
// These structs are serialized as JSON for HTTP communication and WebSocket broadcasts.
package models

import "time"

// ─────────────────────────────────────────────
// Patient Data
// ─────────────────────────────────────────────

// PatientRecord represents a single patient's medical record.
// Each record is identified by PatientID and stored on RegionServers.
type PatientRecord struct {
	PatientID    string    `json:"patient_id"`
	Name         string    `json:"name"`
	Age          int       `json:"age"`
	Diagnosis    string    `json:"diagnosis"`
	Prescription string    `json:"prescription"`
	BloodType    string    `json:"blood_type"`
	Ward         string    `json:"ward"`
	LastUpdated  time.Time `json:"last_updated"`
	StoredOn     []string  `json:"stored_on"` // which RegionServer IDs hold this record
}

// ─────────────────────────────────────────────
// Node / Cluster
// ─────────────────────────────────────────────

// NodeRole classifies a node as Master or RegionServer.
type NodeRole string

const (
	RoleMaster       NodeRole = "MASTER"
	RoleRegionServer NodeRole = "REGION_SERVER"
)

// NodeStatus tracks the health of a node.
type NodeStatus string

const (
	StatusOnline  NodeStatus = "ONLINE"
	StatusOffline NodeStatus = "OFFLINE"
	StatusFailed  NodeStatus = "FAILED"
)

// NodeInfo describes a node registered in ZooKeeper.
type NodeInfo struct {
	ID            string     `json:"id"`
	Address       string     `json:"address"`
	Role          NodeRole   `json:"role"`
	Status        NodeStatus `json:"status"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	RecordCount   int        `json:"record_count"`
}

// ─────────────────────────────────────────────
// Locking (Mutual Exclusion)
// ─────────────────────────────────────────────

// LockEntry represents a row-level lock held on a patient record.
// Implements distributed mutual exclusion: only one writer per record at a time.
type LockEntry struct {
	PatientID  string    `json:"patient_id"`
	HeldBy     string    `json:"held_by"`   // clientID holding the lock
	AcquiredAt time.Time `json:"acquired_at"`
	WaitQueue  []string  `json:"wait_queue"` // clients waiting for this lock
}

// ─────────────────────────────────────────────
// Consensus
// ─────────────────────────────────────────────

// VoteRequest is sent to RegionServers asking them to vote on a proposal.
type VoteRequest struct {
	OperationID string         `json:"operation_id"`
	Operation   string         `json:"operation"`  // "CREATE" | "UPDATE"
	Record      *PatientRecord `json:"record"`
}

// VoteResponse is returned by a RegionServer after evaluating a proposal.
type VoteResponse struct {
	ServerID    string `json:"server_id"`
	OperationID string `json:"operation_id"`
	Vote        string `json:"vote"` // "YES" | "NO"
	Reason      string `json:"reason,omitempty"`
}

// ConsensusRound captures the full lifecycle of one consensus round.
type ConsensusRound struct {
	OperationID string          `json:"operation_id"`
	Operation   string          `json:"operation"`
	PatientID   string          `json:"patient_id"`
	Votes       []*VoteResponse `json:"votes"`
	Result      string          `json:"result"` // "COMMITTED" | "ABORTED"
	Timestamp   time.Time       `json:"timestamp"`
}

// ─────────────────────────────────────────────
// Logging
// ─────────────────────────────────────────────

// LogLevel represents the severity of a log event.
type LogLevel string

const (
	LogInfo    LogLevel = "INFO"
	LogWarn    LogLevel = "WARN"
	LogError   LogLevel = "ERROR"
	LogSuccess LogLevel = "SUCCESS"
	LogEvent   LogLevel = "EVENT"
)

// LogEntry is a single timestamped log message emitted by any component.
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     LogLevel  `json:"level"`
	Source    string    `json:"source"`
	Message   string    `json:"message"`
}

// ─────────────────────────────────────────────
// Request History
// ─────────────────────────────────────────────

// RequestEvent records a client operation for the dashboard history panel.
type RequestEvent struct {
	Timestamp time.Time `json:"timestamp"`
	ClientID  string    `json:"client_id"`
	Operation string    `json:"operation"` // "READ" | "CREATE" | "UPDATE"
	PatientID string    `json:"patient_id"`
	Status    string    `json:"status"` // "SUCCESS" | "FAILED" | "LOCKED" | "ABORTED"
	Latency   string    `json:"latency"`
}

// ─────────────────────────────────────────────
// Client State
// ─────────────────────────────────────────────

// ClientState represents the current activity of a simulated client.
type ClientState struct {
	ClientID      string    `json:"client_id"`
	LastOperation string    `json:"last_operation"`
	LastPatientID string    `json:"last_patient_id"`
	Status        string    `json:"status"` // "IDLE" | "REQUESTING" | "WAITING"
	OpCount       int       `json:"op_count"`
	LastSeen      time.Time `json:"last_seen"`
}

// ─────────────────────────────────────────────
// System State (Dashboard Broadcast)
// ─────────────────────────────────────────────

// SystemState is the full snapshot of distributed system state
// broadcast over WebSocket to the dashboard every 2 seconds.
type SystemState struct {
	Timestamp      time.Time         `json:"timestamp"`
	Leader         *NodeInfo         `json:"leader"`
	Nodes          []*NodeInfo       `json:"nodes"`
	Clients        []*ClientState    `json:"clients"`
	LockTable      []*LockEntry      `json:"lock_table"`
	LastConsensus  *ConsensusRound   `json:"last_consensus"`
	AllConsensus   []*ConsensusRound `json:"all_consensus"`
	PatientRecords []*PatientRecord  `json:"patient_records"`
	Logs           []*LogEntry       `json:"logs"`
	RequestHistory []*RequestEvent   `json:"request_history"`
	FailureEvents  []string          `json:"failure_events"`
}

// ─────────────────────────────────────────────
// HTTP Request/Response Bodies
// ─────────────────────────────────────────────

// CreatePatientRequest is the body for POST /patient/create
type CreatePatientRequest struct {
	ClientID     string `json:"client_id"`
	Name         string `json:"name"`
	Age          int    `json:"age"`
	Diagnosis    string `json:"diagnosis"`
	Prescription string `json:"prescription"`
	BloodType    string `json:"blood_type"`
	Ward         string `json:"ward"`
}

// UpdatePatientRequest is the body for PUT /patient/update/{id}
type UpdatePatientRequest struct {
	ClientID     string `json:"client_id"`
	Diagnosis    string `json:"diagnosis,omitempty"`
	Prescription string `json:"prescription,omitempty"`
	Ward         string `json:"ward,omitempty"`
}

// APIResponse is a standard JSON response envelope.
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}
