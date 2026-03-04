// Package master implements the Master Node of the distributed health system.
//
// The Master is analogous to the HBase Master — it:
//
//   - Acts as the central coordinator for all client requests
//   - Maintains the lock table for mutual exclusion (one writer per patient record)
//   - Orchestrates consensus voting across RegionServers
//   - Routes writes to the primary RegionServer and triggers replication
//   - Registers with ZooKeeper, participates in leader election
//   - Exposes REST APIs for clients and admin operations
package master

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"distributed-health-system/consensus"
	"distributed-health-system/models"
	"distributed-health-system/utils"
	"distributed-health-system/zookeeper"
)

// ─────────────────────────────────────────────
// Lock Table (Mutual Exclusion)
// ─────────────────────────────────────────────

// lockEntry is an internal lock record with a wait-queue channel.
// When a lock is held, other writers block on the `waitCh` channel.
type lockEntry struct {
	clientID   string
	acquiredAt time.Time
	waitCh     chan struct{} // signaled when lock is released
	waiters    []string      // client IDs queued behind this lock
	mu         sync.Mutex
}

// LockTable manages per-patient-record write locks.
//
// Implements distributed mutual exclusion:
//   - Only one client may write a given patient record at a time
//   - Other clients queue up and are unblocked in order
//   - Reads are never blocked (reads bypass the lock table)
type LockTable struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

func newLockTable() *LockTable {
	return &LockTable{locks: make(map[string]*lockEntry)}
}

// Acquire obtains the write lock for `patientID` on behalf of `clientID`.
// If the lock is free → returns immediately.
// If the lock is held → blocks until it is released (simulating wait queue).
func (lt *LockTable) Acquire(patientID, clientID string) {
	for {
		lt.mu.Lock()
		entry, held := lt.locks[patientID]
		if !held {
			// Lock is free — take it.
			lt.locks[patientID] = &lockEntry{
				clientID:   clientID,
				acquiredAt: time.Now(),
				waitCh:     make(chan struct{}, 1),
			}
			lt.mu.Unlock()
			utils.GlobalLogger.Info("LOCK",
				fmt.Sprintf("🔒 Lock ACQUIRED — patient %s by %s", patientID, clientID))
			return
		}

		// Lock is held. Add this client to the wait queue.
		entry.mu.Lock()
		entry.waiters = append(entry.waiters, clientID)
		ch := entry.waitCh
		entry.mu.Unlock()
		lt.mu.Unlock()

		utils.GlobalLogger.Warn("LOCK",
			fmt.Sprintf("⏳ %s WAITING for lock on patient %s (held by %s)",
				clientID, patientID, entry.clientID))

		// Block until the current holder releases the lock.
		<-ch
		// Loop again to re-check (another waiter may have grabbed it).
	}
}

// Release removes the write lock for `patientID` and signals all waiters.
func (lt *LockTable) Release(patientID string) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	entry, ok := lt.locks[patientID]
	if !ok {
		return
	}

	utils.GlobalLogger.Info("LOCK",
		fmt.Sprintf("🔓 Lock RELEASED — patient %s by %s", patientID, entry.clientID))

	delete(lt.locks, patientID)

	// Wake all waiters — they will re-compete to acquire the lock.
	close(entry.waitCh)
}

// Snapshot returns the current state of the lock table for dashboard display.
func (lt *LockTable) Snapshot() []*models.LockEntry {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	result := make([]*models.LockEntry, 0, len(lt.locks))
	for pid, e := range lt.locks {
		e.mu.Lock()
		waiters := make([]string, len(e.waiters))
		copy(waiters, e.waiters)
		e.mu.Unlock()

		result = append(result, &models.LockEntry{
			PatientID:  pid,
			HeldBy:     e.clientID,
			AcquiredAt: e.acquiredAt,
			WaitQueue:  waiters,
		})
	}
	return result
}

// ─────────────────────────────────────────────
// Master Node
// ─────────────────────────────────────────────

// regionServerRef holds connection info for a known RegionServer.
type regionServerRef struct {
	id      string
	address string
}

// MasterNode is the central coordinator of the distributed cluster.
type MasterNode struct {
	ID      string
	Address string
	Status  models.NodeStatus

	zk          *zookeeper.ZooKeeper
	lockTable   *LockTable
	consensus   *consensus.Manager
	servers     []*regionServerRef
	requestHist []*models.RequestEvent
	histMu      sync.Mutex

	isLeader atomic.Bool
	mu       sync.RWMutex
	server   *http.Server

	// opCounter generates unique operation IDs.
	opCounter atomic.Uint64
}

// New creates a new MasterNode.
func New(id, address string, zk *zookeeper.ZooKeeper) *MasterNode {
	m := &MasterNode{
		ID:        id,
		Address:   address,
		Status:    models.StatusOnline,
		zk:        zk,
		lockTable: newLockTable(),
		consensus: consensus.New(),
		servers:   make([]*regionServerRef, 0),
	}

	// Listen for leader changes from ZooKeeper.
	zk.OnLeaderChange(func(leader *models.NodeInfo) {
		if leader != nil && leader.ID == id {
			m.isLeader.Store(true)
			utils.GlobalLogger.Event("MASTER",
				fmt.Sprintf("👑 This Master (%s) is now the LEADER", id))
		} else {
			m.isLeader.Store(false)
		}
	})

	return m
}

// AddRegionServer registers a known RegionServer with this Master.
func (m *MasterNode) AddRegionServer(id, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.servers = append(m.servers, &regionServerRef{id: id, address: address})
}

// Start registers with ZooKeeper and launches the HTTP server.
func (m *MasterNode) Start() {
	// Register as a MASTER node in ZooKeeper.
	m.zk.RegisterNode(m.ID, m.Address, models.RoleMaster)

	mux := http.NewServeMux()

	// ── Client-facing REST API ───────────────────────────────────────
	mux.HandleFunc("/patient/create", m.handleCreate)
	mux.HandleFunc("/patient/update/", m.handleUpdate)
	mux.HandleFunc("/patient/", m.handleRead)

	// ── System status APIs ────────────────────────────────────────────
	mux.HandleFunc("/system/status", m.handleSystemStatus)
	mux.HandleFunc("/locks", m.handleLocks)
	mux.HandleFunc("/leader", m.handleLeader)
	mux.HandleFunc("/servers", m.handleServers)

	// ── Admin / failure injection APIs ───────────────────────────────
	mux.HandleFunc("/admin/kill-leader", m.handleKillLeader)
	mux.HandleFunc("/admin/kill-server/", m.handleKillServer)
	mux.HandleFunc("/admin/recover-server/", m.handleRecoverServer)
	mux.HandleFunc("/admin/recover-all", m.handleRecoverAll)

	m.server = &http.Server{Addr: m.Address, Handler: mux}

	utils.GlobalLogger.Info("MASTER",
		fmt.Sprintf("Master node %s starting on http://%s", m.ID, m.Address))

	// Send heartbeats to ZooKeeper.
	go m.sendHeartbeats()

	go func() {
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			utils.GlobalLogger.Error("MASTER",
				fmt.Sprintf("HTTP server error: %v", err))
		}
	}()
}

// sendHeartbeats periodically signals ZooKeeper that this Master is alive.
func (m *MasterNode) sendHeartbeats() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.RLock()
		status := m.Status
		m.mu.RUnlock()
		if status == models.StatusOnline {
			m.zk.SendHeartbeat(m.ID)
		}
	}
}

// ─────────────────────────────────────────────
// Write Pipeline (Lock → Consensus → Write → Replicate)
// ─────────────────────────────────────────────

// writePatient is the core write pipeline for CREATE and UPDATE.
//
// Steps:
//  1. Acquire row-level lock (mutual exclusion)
//  2. Run consensus across RegionServers (majority vote)
//  3. If committed → write to primary RegionServer
//  4. Replicate to secondary RegionServer (HDFS replication factor=2)
//  5. Release lock
func (m *MasterNode) writePatient(clientID string, record *models.PatientRecord, operation string) (bool, string) {
	start := time.Now()

	// Step 1: Acquire row-level lock.
	m.lockTable.Acquire(record.PatientID, clientID)
	defer m.lockTable.Release(record.PatientID)

	// Step 2: Build consensus server refs.
	m.mu.RLock()
	srvs := make([]*regionServerRef, len(m.servers))
	copy(srvs, m.servers)
	m.mu.RUnlock()

	csRefs := make([]*consensus.RegionServerRef, len(srvs))
	for i, s := range srvs {
		status := m.getServerStatus(s.address)
		csRefs[i] = &consensus.RegionServerRef{
			ID:      s.id,
			Address: s.address,
			Status:  status,
		}
	}

	opID := fmt.Sprintf("OP-%d", m.opCounter.Add(1))
	round := m.consensus.Propose(opID, operation, record, csRefs)

	if round.Result != "COMMITTED" {
		latency := time.Since(start).String()
		m.recordRequest(clientID, operation, record.PatientID, "ABORTED", latency)
		return false, "Consensus ABORTED — majority did not agree"
	}

	// Step 3: Write to primary RegionServer (round-robin selection).
	primaryIdx := 0
	if len(srvs) > 0 {
		primaryIdx = rand.Intn(len(srvs))
	}
	var primary, secondary *regionServerRef
	if len(srvs) > 0 {
		primary = srvs[primaryIdx]
	}
	if len(srvs) > 1 {
		secondary = srvs[(primaryIdx+1)%len(srvs)]
	}

	if primary == nil {
		m.recordRequest(clientID, operation, record.PatientID, "FAILED", time.Since(start).String())
		return false, "No RegionServers available"
	}

	// Tag which server stores this record.
	record.StoredOn = []string{primary.id}
	if secondary != nil {
		record.StoredOn = append(record.StoredOn, secondary.id)
	}

	if err := m.writeToServer(record, primary.address); err != nil {
		utils.GlobalLogger.Error("MASTER",
			fmt.Sprintf("Primary write to %s failed: %v", primary.id, err))
		m.recordRequest(clientID, operation, record.PatientID, "FAILED", time.Since(start).String())
		return false, "Primary write failed"
	}
	utils.GlobalLogger.Success("MASTER",
		fmt.Sprintf("✅ Written patient %s to primary %s", record.PatientID, primary.id))

	// Step 4: Replicate to secondary (fire-and-forget).
	if secondary != nil {
		go m.replicateToServer(record, secondary.address)
	}

	latency := time.Since(start).String()
	m.recordRequest(clientID, operation, record.PatientID, "SUCCESS", latency)
	return true, "Operation successful"
}

// writeToServer sends a patient record to a RegionServer's /data/write endpoint.
func (m *MasterNode) writeToServer(record *models.PatientRecord, address string) error {
	body, _ := json.Marshal(record)
	url := fmt.Sprintf("http://%s/data/write", address)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %d", resp.StatusCode)
	}
	return nil
}

// replicateToServer sends a patient record to a RegionServer's /replicate endpoint.
func (m *MasterNode) replicateToServer(record *models.PatientRecord, address string) {
	body, _ := json.Marshal(record)
	url := fmt.Sprintf("http://%s/replicate", address)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		utils.GlobalLogger.Warn("MASTER",
			fmt.Sprintf("Replication to %s failed: %v", address, err))
		return
	}
	defer resp.Body.Close()
	utils.GlobalLogger.Success("MASTER",
		fmt.Sprintf("📋 Replication to %s successful", address))
}

// getServerStatus probes a RegionServer's /health endpoint.
func (m *MasterNode) getServerStatus(address string) models.NodeStatus {
	client := &http.Client{Timeout: 1 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s/health", address))
	if err != nil || resp.StatusCode != http.StatusOK {
		return models.StatusFailed
	}
	resp.Body.Close()
	return models.StatusOnline
}

// recordRequest logs a completed client operation to the request history.
func (m *MasterNode) recordRequest(clientID, op, patientID, status, latency string) {
	m.histMu.Lock()
	defer m.histMu.Unlock()
	m.requestHist = append(m.requestHist, &models.RequestEvent{
		Timestamp: time.Now(),
		ClientID:  clientID,
		Operation: op,
		PatientID: patientID,
		Status:    status,
		Latency:   latency,
	})
	if len(m.requestHist) > 100 {
		m.requestHist = m.requestHist[len(m.requestHist)-100:]
	}
}

// GetRequestHistory returns recent request events.
func (m *MasterNode) GetRequestHistory() []*models.RequestEvent {
	m.histMu.Lock()
	defer m.histMu.Unlock()
	result := make([]*models.RequestEvent, len(m.requestHist))
	copy(result, m.requestHist)
	return result
}

// GetConsensusHistory returns recent consensus rounds.
func (m *MasterNode) GetConsensusHistory() []*models.ConsensusRound {
	return m.consensus.GetHistory()
}

// GetLastConsensus returns the most recent consensus round.
func (m *MasterNode) GetLastConsensus() *models.ConsensusRound {
	return m.consensus.GetLast()
}

// GetLockSnapshot returns the current lock table state.
func (m *MasterNode) GetLockSnapshot() []*models.LockEntry {
	return m.lockTable.Snapshot()
}

// ─────────────────────────────────────────────
// HTTP Handlers — Client API
// ─────────────────────────────────────────────

// handleCreate handles POST /patient/create
func (m *MasterNode) handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	enableCORS(w)

	// Leader check: only the active master processes writes.
	if !m.isLeader.Load() {
		utils.GlobalLogger.Warn("MASTER", fmt.Sprintf("Rejecting CREATE: This node (%s) is not the leader", m.ID))
		writeJSON(w, models.APIResponse{Success: false, Message: "Not the current leader. Please retry against the active master."})
		return
	}

	var req models.CreatePatientRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, models.APIResponse{Success: false, Message: "Invalid request: " + err.Error()})
		return
	}

	// Generate a patient ID if not provided.
	patientID := fmt.Sprintf("P%04d", rand.Intn(9000)+1000)

	record := &models.PatientRecord{
		PatientID:    patientID,
		Name:         req.Name,
		Age:          req.Age,
		Diagnosis:    req.Diagnosis,
		Prescription: req.Prescription,
		BloodType:    req.BloodType,
		Ward:         req.Ward,
		LastUpdated:  time.Now(),
	}

	utils.GlobalLogger.Info("MASTER",
		fmt.Sprintf("📝 CREATE request from %s for patient %s", req.ClientID, patientID))

	ok, msg := m.writePatient(req.ClientID, record, "CREATE")
	if !ok {
		writeJSON(w, models.APIResponse{Success: false, Message: msg})
		return
	}
	writeJSON(w, models.APIResponse{Success: true, Message: "Patient created", Data: record})
}

// handleUpdate handles PUT /patient/update/{id}
func (m *MasterNode) handleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	enableCORS(w)

	// Leader check: only the active master processes writes.
	if !m.isLeader.Load() {
		utils.GlobalLogger.Warn("MASTER", fmt.Sprintf("Rejecting UPDATE: This node (%s) is not the leader", m.ID))
		writeJSON(w, models.APIResponse{Success: false, Message: "Not the current leader."})
		return
	}

	patientID := strings.TrimPrefix(r.URL.Path, "/patient/update/")
	if patientID == "" {
		writeJSON(w, models.APIResponse{Success: false, Message: "Missing patient ID"})
		return
	}

	var req models.UpdatePatientRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, models.APIResponse{Success: false, Message: "Invalid request"})
		return
	}

	utils.GlobalLogger.Info("MASTER",
		fmt.Sprintf("✏️  UPDATE request from %s for patient %s", req.ClientID, patientID))

	// Fetch existing record from any available RegionServer.
	existing := m.fetchRecord(patientID)
	var record *models.PatientRecord
	if existing != nil {
		record = existing
	} else {
		// Record doesn't exist yet — treat as create.
		record = &models.PatientRecord{PatientID: patientID}
	}

	// Apply updates.
	if req.Diagnosis != "" {
		record.Diagnosis = req.Diagnosis
	}
	if req.Prescription != "" {
		record.Prescription = req.Prescription
	}
	if req.Ward != "" {
		record.Ward = req.Ward
	}

	ok, msg := m.writePatient(req.ClientID, record, "UPDATE")
	if !ok {
		writeJSON(w, models.APIResponse{Success: false, Message: msg})
		return
	}
	writeJSON(w, models.APIResponse{Success: true, Message: "Patient updated", Data: record})
}

// handleRead handles GET /patient/{id}
// Reads are NOT locked — multiple concurrent reads are allowed.
func (m *MasterNode) handleRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	enableCORS(w)

	// Leader check: technically reads can be served by any master in some systems,
	// but for this consistent-view demo, we'll route everything through the leader.
	if !m.isLeader.Load() {
		writeJSON(w, models.APIResponse{Success: false, Message: "Not the current leader."})
		return
	}

	patientID := strings.TrimPrefix(r.URL.Path, "/patient/")
	if patientID == "" {
		writeJSON(w, models.APIResponse{Success: false, Message: "Missing patient ID"})
		return
	}

	record := m.fetchRecord(patientID)
	if record == nil {
		writeJSON(w, models.APIResponse{Success: false, Message: "Patient not found"})
		return
	}

	writeJSON(w, models.APIResponse{Success: true, Data: record})
}

// fetchRecord queries all RegionServers to find a record.
func (m *MasterNode) fetchRecord(patientID string) *models.PatientRecord {
	m.mu.RLock()
	srvs := make([]*regionServerRef, len(m.servers))
	copy(srvs, m.servers)
	m.mu.RUnlock()

	for _, s := range srvs {
		url := fmt.Sprintf("http://%s/data/read/%s", s.address, patientID)
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(url)
		if err != nil || resp.StatusCode != http.StatusOK {
			continue
		}
		var apiResp models.APIResponse
		if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()
		if apiResp.Success && apiResp.Data != nil {
			// Re-marshal to PatientRecord.
			b, _ := json.Marshal(apiResp.Data)
			var rec models.PatientRecord
			if err := json.Unmarshal(b, &rec); err == nil {
				return &rec
			}
		}
	}
	return nil
}

// GetAllRecords collects patient records from all RegionServers.
func (m *MasterNode) GetAllRecords() []*models.PatientRecord {
	m.mu.RLock()
	srvs := make([]*regionServerRef, len(m.servers))
	copy(srvs, m.servers)
	m.mu.RUnlock()

	seen := make(map[string]bool)
	result := make([]*models.PatientRecord, 0)

	for _, s := range srvs {
		url := fmt.Sprintf("http://%s/data/all", s.address)
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(url)
		if err != nil {
			continue
		}

		var apiResp models.APIResponse
		if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		if apiResp.Success && apiResp.Data != nil {
			b, _ := json.Marshal(apiResp.Data)
			var records []*models.PatientRecord
			if err := json.Unmarshal(b, &records); err == nil {
				for _, r := range records {
					if !seen[r.PatientID] {
						seen[r.PatientID] = true
						result = append(result, r)
					}
				}
			}
		}
	}
	return result
}

// ─────────────────────────────────────────────
// HTTP Handlers — System / Admin
// ─────────────────────────────────────────────

func (m *MasterNode) handleSystemStatus(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	writeJSON(w, map[string]interface{}{
		"master":    m.ID,
		"isLeader":  m.isLeader.Load(),
		"status":    m.Status,
		"servers":   m.servers,
		"lockTable": m.lockTable.Snapshot(),
	})
}

func (m *MasterNode) handleLocks(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	writeJSON(w, m.lockTable.Snapshot())
}

func (m *MasterNode) handleLeader(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	writeJSON(w, m.zk.GetLeader())
}

func (m *MasterNode) handleServers(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	writeJSON(w, m.zk.GetNodes())
}

// handleKillLeader simulates Master failure — stops heartbeats, marks failed.
func (m *MasterNode) handleKillLeader(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	m.mu.Lock()
	m.Status = models.StatusFailed
	m.mu.Unlock()

	m.zk.MarkFailed(m.ID)
	utils.GlobalLogger.Error("MASTER",
		fmt.Sprintf("💀 Master %s KILLED — triggering leader election", m.ID))
	writeJSON(w, models.APIResponse{Success: true, Message: "Master killed"})
}

// handleKillServer handles POST /admin/kill-server/{id}
func (m *MasterNode) handleKillServer(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	serverID := strings.TrimPrefix(r.URL.Path, "/admin/kill-server/")

	m.mu.RLock()
	var target *regionServerRef
	for _, s := range m.servers {
		if s.id == serverID {
			target = s
			break
		}
	}
	m.mu.RUnlock()

	if target == nil {
		writeJSON(w, models.APIResponse{Success: false, Message: "Server not found"})
		return
	}

	// Tell the RegionServer to kill itself.
	client := &http.Client{Timeout: 2 * time.Second}
	client.Post(fmt.Sprintf("http://%s/admin/kill", target.address), "application/json", nil) //nolint
	writeJSON(w, models.APIResponse{Success: true, Message: serverID + " killed"})
}

// handleRecoverServer handles POST /admin/recover-server/{id}
func (m *MasterNode) handleRecoverServer(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	serverID := strings.TrimPrefix(r.URL.Path, "/admin/recover-server/")

	m.mu.RLock()
	var target *regionServerRef
	for _, s := range m.servers {
		if s.id == serverID {
			target = s
			break
		}
	}
	m.mu.RUnlock()

	if target == nil {
		writeJSON(w, models.APIResponse{Success: false, Message: "Server not found"})
		return
	}

	client := &http.Client{Timeout: 2 * time.Second}
	client.Post(fmt.Sprintf("http://%s/admin/recover", target.address), "application/json", nil) //nolint
	writeJSON(w, models.APIResponse{Success: true, Message: serverID + " recovered"})
}

// handleRecoverAll recovers the master + all servers.
func (m *MasterNode) handleRecoverAll(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	m.mu.Lock()
	m.Status = models.StatusOnline
	m.mu.Unlock()
	m.zk.RecoverNode(m.ID)

	m.mu.RLock()
	srvs := make([]*regionServerRef, len(m.servers))
	copy(srvs, m.servers)
	m.mu.RUnlock()

	client := &http.Client{Timeout: 2 * time.Second}
	for _, s := range srvs {
		client.Post(fmt.Sprintf("http://%s/admin/recover", s.address), "application/json", nil) //nolint
	}

	utils.GlobalLogger.Success("MASTER", "✅ All nodes RECOVERED")
	writeJSON(w, models.APIResponse{Success: true, Message: "All nodes recovered"})
}

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
