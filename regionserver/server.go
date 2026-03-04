// Package regionserver implements the RegionServer nodes of the distributed system.
//
// RegionServers are the data nodes of the cluster, analogous to HBase RegionServers.
// Each RegionServer:
//   - Stores a subset of patient records in its local DataStore
//   - Participates in consensus voting
//   - Accepts replicated writes from the Master
//   - Can be killed/recovered to simulate node failures
package regionserver

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"distributed-health-system/models"
	"distributed-health-system/storage"
	"distributed-health-system/utils"
	"distributed-health-system/zookeeper"
)

// RegionServer represents a single data node in the cluster.
type RegionServer struct {
	ID      string
	Address string
	Status  models.NodeStatus

	store  *storage.DataStore
	zk     *zookeeper.ZooKeeper
	mu     sync.RWMutex
	server *http.Server
}

// New creates and returns a new RegionServer instance.
func New(id, address string, zk *zookeeper.ZooKeeper) *RegionServer {
	return &RegionServer{
		ID:      id,
		Address: address,
		Status:  models.StatusOnline,
		store:   storage.NewDataStore(id),
		zk:      zk,
	}
}

// Start registers this RegionServer with ZooKeeper and launches its HTTP server.
// It also begins the heartbeat goroutine to signal liveness to ZooKeeper.
func (rs *RegionServer) Start() {
	// Register with ZooKeeper as a REGION_SERVER node.
	rs.zk.RegisterNode(rs.ID, rs.Address, models.RoleRegionServer)

	mux := http.NewServeMux()

	// ── Data endpoints (called by Master) ───────────────────────────
	mux.HandleFunc("/data/write", rs.handleWrite)
	mux.HandleFunc("/data/read/", rs.handleRead)
	mux.HandleFunc("/data/all", rs.handleGetAll)

	// ── Consensus endpoint ───────────────────────────────────────────
	// POST /consensus/vote — receive a proposal and respond with YES/NO
	mux.HandleFunc("/consensus/vote", rs.handleConsensusVote)

	// ── Replication endpoint ─────────────────────────────────────────
	// POST /replicate — accept a replicated record from another server
	mux.HandleFunc("/replicate", rs.handleReplicate)

	// ── Health & Admin endpoints ─────────────────────────────────────
	mux.HandleFunc("/health", rs.handleHealth)
	mux.HandleFunc("/admin/kill", rs.handleKill)
	mux.HandleFunc("/admin/recover", rs.handleRecover)

	rs.server = &http.Server{
		Addr:    rs.Address,
		Handler: mux,
	}

	utils.GlobalLogger.Info(rs.ID,
		fmt.Sprintf("RegionServer starting on http://%s", rs.Address))

	// Start heartbeat goroutine.
	go rs.sendHeartbeats()

	// Start HTTP server in goroutine (non-blocking).
	go func() {
		if err := rs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			utils.GlobalLogger.Error(rs.ID,
				fmt.Sprintf("HTTP server error: %v", err))
		}
	}()
}

// GetStatus returns the current online/failed status of this RegionServer.
func (rs *RegionServer) GetStatus() models.NodeStatus {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.Status
}

// GetRecordCount returns how many patient records are stored locally.
func (rs *RegionServer) GetRecordCount() int {
	return rs.store.Count()
}

// ─────────────────────────────────────────────
// Heartbeat
// ─────────────────────────────────────────────

// sendHeartbeats periodically notifies ZooKeeper that this node is alive.
// If the node is marked FAILED (via /admin/kill), heartbeats stop.
func (rs *RegionServer) sendHeartbeats() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		rs.mu.RLock()
		status := rs.Status
		rs.mu.RUnlock()

		if status == models.StatusFailed {
			// Dead nodes don't send heartbeats.
			continue
		}
		rs.zk.SendHeartbeat(rs.ID)
		rs.zk.UpdateRecordCount(rs.ID, rs.store.Count())
	}
}

// ─────────────────────────────────────────────
// HTTP Handlers
// ─────────────────────────────────────────────

// handleWrite handles POST /data/write
// Master calls this to persist a patient record after consensus.
func (rs *RegionServer) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !rs.isOnline() {
		writeError(w, "RegionServer is offline", http.StatusServiceUnavailable)
		return
	}

	var record models.PatientRecord
	if err := json.NewDecoder(r.Body).Decode(&record); err != nil {
		writeError(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err := rs.store.Put(&record); err != nil {
		writeError(w, "Write failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	utils.GlobalLogger.Success(rs.ID,
		fmt.Sprintf("💾 Written patient %s", record.PatientID))
	writeJSON(w, models.APIResponse{Success: true, Message: "Record written"})
}

// handleRead handles GET /data/read/{patientID}
// Returns the patient record if found.
func (rs *RegionServer) handleRead(w http.ResponseWriter, r *http.Request) {
	if !rs.isOnline() {
		writeError(w, "RegionServer is offline", http.StatusServiceUnavailable)
		return
	}

	// Extract patientID from URL path: /data/read/{id}
	patientID := strings.TrimPrefix(r.URL.Path, "/data/read/")
	if patientID == "" {
		writeError(w, "Missing patient ID", http.StatusBadRequest)
		return
	}

	record, found := rs.store.Get(patientID)
	if !found {
		writeError(w, "Patient not found", http.StatusNotFound)
		return
	}

	writeJSON(w, models.APIResponse{Success: true, Data: record})
}

// handleGetAll handles GET /data/all
// Returns all patient records stored on this RegionServer.
func (rs *RegionServer) handleGetAll(w http.ResponseWriter, r *http.Request) {
	if !rs.isOnline() {
		writeError(w, "RegionServer is offline", http.StatusServiceUnavailable)
		return
	}

	records := rs.store.GetAll()
	writeJSON(w, models.APIResponse{Success: true, Data: records})
}

// handleConsensusVote handles POST /consensus/vote
//
// Consensus Algorithm - Participant side:
// When the Master proposes an operation, each RegionServer evaluates it.
// A healthy server votes YES. A server simulates NO with 15% probability
// to demonstrate fault scenarios. A FAILED server never responds (timeout).
func (rs *RegionServer) handleConsensusVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !rs.isOnline() {
		// Dead server doesn't respond — this triggers a timeout on the Master side,
		// which counts as a NO vote, potentially blocking consensus.
		time.Sleep(5 * time.Second) // simulate no response
		writeError(w, "offline", http.StatusServiceUnavailable)
		return
	}

	var req models.VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Simulate occasional NO votes (15% chance) to demonstrate fault tolerance.
	vote := "YES"
	reason := ""
	if rand.Float32() < 0.15 {
		vote = "NO"
		reason = "disk write simulation failed"
	}

	resp := models.VoteResponse{
		ServerID:    rs.ID,
		OperationID: req.OperationID,
		Vote:        vote,
		Reason:      reason,
	}

	writeJSON(w, resp)
}

// handleReplicate handles POST /replicate
// Accepts a replicated patient record from another RegionServer or the Master.
// This implements the HDFS replication factor=2 requirement.
func (rs *RegionServer) handleReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !rs.isOnline() {
		writeError(w, "RegionServer is offline", http.StatusServiceUnavailable)
		return
	}

	var record models.PatientRecord
	if err := json.NewDecoder(r.Body).Decode(&record); err != nil {
		writeError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := rs.store.Put(&record); err != nil {
		writeError(w, "Replication write failed", http.StatusInternalServerError)
		return
	}

	utils.GlobalLogger.Info(rs.ID,
		fmt.Sprintf("📋 Replicated patient %s from primary", record.PatientID))
	writeJSON(w, models.APIResponse{Success: true, Message: "Replicated"})
}

// handleHealth handles GET /health
// Used by monitoring and integration tests.
func (rs *RegionServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	rs.mu.RLock()
	status := rs.Status
	rs.mu.RUnlock()

	if status != models.StatusOnline {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]string{"status": string(status)})
		return
	}
	writeJSON(w, map[string]interface{}{
		"status":       "online",
		"id":           rs.ID,
		"record_count": rs.store.Count(),
	})
}

// handleKill handles POST /admin/kill
// Simulates a RegionServer failure (e.g., hardware crash, network partition).
// After this call, the server stops responding to data/vote requests.
func (rs *RegionServer) handleKill(w http.ResponseWriter, r *http.Request) {
	rs.mu.Lock()
	rs.Status = models.StatusFailed
	rs.mu.Unlock()

	rs.zk.MarkFailed(rs.ID)
	utils.GlobalLogger.Error(rs.ID, "💀 RegionServer KILLED (failure simulation)")
	writeJSON(w, models.APIResponse{Success: true, Message: rs.ID + " killed"})
}

// handleRecover handles POST /admin/recover
// Brings a failed RegionServer back online.
func (rs *RegionServer) handleRecover(w http.ResponseWriter, r *http.Request) {
	rs.mu.Lock()
	rs.Status = models.StatusOnline
	rs.mu.Unlock()

	rs.zk.RecoverNode(rs.ID)
	utils.GlobalLogger.Success(rs.ID, "✅ RegionServer RECOVERED")
	writeJSON(w, models.APIResponse{Success: true, Message: rs.ID + " recovered"})
}

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

func (rs *RegionServer) isOnline() bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.Status == models.StatusOnline
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(models.APIResponse{Success: false, Message: msg})
}
