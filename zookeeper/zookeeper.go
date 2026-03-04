// Package zookeeper simulates the Apache ZooKeeper coordination service.
//
// In real HBase, ZooKeeper is responsible for:
//   - Maintaining the list of live RegionServers
//   - Storing the location of the HBase Master
//   - Leader election when the Master fails
//
// This simulation implements those responsibilities using:
//   - An in-memory node registry (map[string]*NodeInfo)
//   - Heartbeat monitoring via goroutines
//   - Lowest-ID leader election algorithm
package zookeeper

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"distributed-health-system/models"
	"distributed-health-system/utils"
)

const (
	// heartbeatTimeout is how long before a node is considered dead.
	heartbeatTimeout = 5 * time.Second
	// checkInterval is how often ZooKeeper polls node health.
	checkInterval = 1 * time.Second
)

// LeaderChangeCallback is called when the leader changes (used by master and dashboard).
type LeaderChangeCallback func(newLeader *models.NodeInfo)

// ZooKeeper is the central coordination service for the distributed system.
// It maintains node registrations, monitors heartbeats, and manages leader election.
type ZooKeeper struct {
	mu       sync.RWMutex
	nodes    map[string]*models.NodeInfo // all registered nodes
	leader   *models.NodeInfo            // current elected leader
	onChange []LeaderChangeCallback      // callbacks triggered on leader change

	// failureEvents stores a log of notable cluster events.
	failureEvents []string
	feMu          sync.Mutex
}

// New creates and returns a new ZooKeeper instance.
func New() *ZooKeeper {
	return &ZooKeeper{
		nodes:         make(map[string]*models.NodeInfo),
		failureEvents: make([]string, 0),
	}
}

// Start begins the heartbeat monitoring goroutine.
// This runs continuously, checking node liveness every `checkInterval`.
func (zk *ZooKeeper) Start() {
	utils.GlobalLogger.Info("ZOOKEEPER", "ZooKeeper simulation started. Monitoring heartbeats.")
	go zk.monitorHeartbeats()
}

// RegisterNode adds a node to the ZooKeeper registry.
// This is called by each node at startup (analogous to creating an ephemeral znode in real ZK).
func (zk *ZooKeeper) RegisterNode(id, address string, role models.NodeRole) {
	zk.mu.Lock()
	defer zk.mu.Unlock()

	node := &models.NodeInfo{
		ID:            id,
		Address:       address,
		Role:          role,
		Status:        models.StatusOnline,
		LastHeartbeat: time.Now(),
	}
	zk.nodes[id] = node
	utils.GlobalLogger.Info("ZOOKEEPER",
		fmt.Sprintf("Node registered: %s (%s) at %s", id, role, address))

	// Trigger election after registration (leader may change).
	zk.electLeaderLocked()
}

// SendHeartbeat updates a node's LastHeartbeat timestamp.
// Nodes call this periodically (every ~1s) to signal they are alive.
func (zk *ZooKeeper) SendHeartbeat(nodeID string) {
	zk.mu.Lock()
	defer zk.mu.Unlock()

	node, ok := zk.nodes[nodeID]
	if !ok {
		return
	}
	if node.Status == models.StatusFailed {
		// Node was marked failed — don't silently recover from heartbeat alone.
		return
	}
	node.Status = models.StatusOnline
	node.LastHeartbeat = time.Now()
}

// UpdateRecordCount refreshes the record count displayed on a RegionServer node card.
func (zk *ZooKeeper) UpdateRecordCount(nodeID string, count int) {
	zk.mu.Lock()
	defer zk.mu.Unlock()
	if node, ok := zk.nodes[nodeID]; ok {
		node.RecordCount = count
	}
}

// MarkFailed manually marks a node as failed (used by failure injection APIs).
// After marking, it triggers a new leader election.
func (zk *ZooKeeper) MarkFailed(nodeID string) {
	zk.mu.Lock()

	node, ok := zk.nodes[nodeID]
	if !ok {
		zk.mu.Unlock()
		return
	}
	node.Status = models.StatusFailed
	msg := fmt.Sprintf("[%s] Node %s marked as FAILED",
		time.Now().Format("15:04:05"), nodeID)
	utils.GlobalLogger.Error("ZOOKEEPER",
		fmt.Sprintf("💀 Node FAILED: %s (%s)", nodeID, node.Role))

	zk.mu.Unlock()

	zk.feMu.Lock()
	zk.failureEvents = append(zk.failureEvents, msg)
	zk.feMu.Unlock()

	// Re-run leader election since a node (possibly the leader) just failed.
	zk.electLeader()
}

// RecoverNode brings a node back online after a simulated failure.
func (zk *ZooKeeper) RecoverNode(nodeID string) {
	zk.mu.Lock()
	node, ok := zk.nodes[nodeID]
	if ok {
		node.Status = models.StatusOnline
		node.LastHeartbeat = time.Now()
		utils.GlobalLogger.Success("ZOOKEEPER",
			fmt.Sprintf("✅ Node RECOVERED: %s", nodeID))
	}
	zk.mu.Unlock()

	zk.electLeader()
}

// OnLeaderChange registers a callback invoked whenever the leader changes.
func (zk *ZooKeeper) OnLeaderChange(fn LeaderChangeCallback) {
	zk.mu.Lock()
	defer zk.mu.Unlock()
	zk.onChange = append(zk.onChange, fn)
}

// GetLeader returns the currently elected leader node.
func (zk *ZooKeeper) GetLeader() *models.NodeInfo {
	zk.mu.RLock()
	defer zk.mu.RUnlock()
	return zk.leader
}

// GetNodes returns all registered nodes.
func (zk *ZooKeeper) GetNodes() []*models.NodeInfo {
	zk.mu.RLock()
	defer zk.mu.RUnlock()
	result := make([]*models.NodeInfo, 0, len(zk.nodes))
	for _, n := range zk.nodes {
		copy := *n
		result = append(result, &copy)
	}
	return result
}

// GetFailureEvents returns the list of recorded failure events.
func (zk *ZooKeeper) GetFailureEvents() []string {
	zk.feMu.Lock()
	defer zk.feMu.Unlock()
	result := make([]string, len(zk.failureEvents))
	copy(result, zk.failureEvents)
	return result
}

// ─────────────────────────────────────────────
// Leader Election
// ─────────────────────────────────────────────

// electLeader performs leader election with the mutex NOT held.
// It locks internally via electLeaderLocked.
func (zk *ZooKeeper) electLeader() {
	zk.mu.Lock()
	defer zk.mu.Unlock()
	zk.electLeaderLocked()
}

// electLeaderLocked performs the actual leader election.
// Must be called with zk.mu WRITE LOCK held.
//
// Algorithm: Lowest-ID Election (inspired by ZooKeeper's ephemeral sequential znodes)
//   - Collect all ONLINE nodes that have the MASTER role
//   - Sort by ID (lexicographic)
//   - The first (lowest) ID wins
//   - If the current leader is still alive and still the lowest — no change
func (zk *ZooKeeper) electLeaderLocked() {
	// Gather all alive master-role nodes.
	candidates := make([]*models.NodeInfo, 0)
	for _, node := range zk.nodes {
		if node.Role == models.RoleMaster && node.Status == models.StatusOnline {
			candidates = append(candidates, node)
		}
	}

	if len(candidates) == 0 {
		prevLeader := zk.leader
		zk.leader = nil
		if prevLeader != nil {
			utils.GlobalLogger.Error("ZOOKEEPER", "❌ No master candidates alive — cluster has no leader!")
		}
		return
	}

	// Sort by node ID so lowest ID wins deterministically.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ID < candidates[j].ID
	})
	newLeader := candidates[0]

	// Check if leader actually changed.
	if zk.leader != nil && zk.leader.ID == newLeader.ID {
		return // No change.
	}

	oldLeaderID := "none"
	if zk.leader != nil {
		oldLeaderID = zk.leader.ID
	}
	zk.leader = newLeader

	utils.GlobalLogger.Event("ZOOKEEPER",
		fmt.Sprintf("👑 Leader elected: %s (was: %s)", newLeader.ID, oldLeaderID))

	// Copy callbacks to call outside the lock.
	callbacks := make([]LeaderChangeCallback, len(zk.onChange))
	copy(callbacks, zk.onChange)

	// Release lock before calling callbacks (they may re-acquire the lock).
	go func() {
		for _, cb := range callbacks {
			cb(newLeader)
		}
	}()
}

// ─────────────────────────────────────────────
// Heartbeat Monitor
// ─────────────────────────────────────────────

// monitorHeartbeats runs in a goroutine, checking all nodes periodically.
// If a node has not sent a heartbeat within heartbeatTimeout, it is marked FAILED
// and leader election is triggered.
func (zk *ZooKeeper) monitorHeartbeats() {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		var failedNodes []string

		zk.mu.Lock()
		for id, node := range zk.nodes {
			if node.Status != models.StatusOnline {
				continue
			}
			if now.Sub(node.LastHeartbeat) > heartbeatTimeout {
				node.Status = models.StatusFailed
				failedNodes = append(failedNodes, id)
			}
		}
		needsElection := len(failedNodes) > 0
		zk.mu.Unlock()

		for _, id := range failedNodes {
			utils.GlobalLogger.Error("ZOOKEEPER",
				fmt.Sprintf("💀 Heartbeat timeout: Node %s declared FAILED", id))
			zk.feMu.Lock()
			zk.failureEvents = append(zk.failureEvents,
				fmt.Sprintf("[%s] Heartbeat timeout: %s declared FAILED",
					now.Format("15:04:05"), id))
			zk.feMu.Unlock()
		}

		if needsElection {
			zk.electLeader()
		}
	}
}

// ─────────────────────────────────────────────
// HTTP Status Endpoint
// ─────────────────────────────────────────────

// StatusHandler handles GET /zk/status — returns cluster state for debugging.
func (zk *ZooKeeper) StatusHandler(w http.ResponseWriter, r *http.Request) {
	zk.mu.RLock()
	nodes := make([]*models.NodeInfo, 0, len(zk.nodes))
	for _, n := range zk.nodes {
		copy := *n
		nodes = append(nodes, &copy)
	}
	leader := zk.leader
	zk.mu.RUnlock()

	resp := map[string]interface{}{
		"leader": leader,
		"nodes":  nodes,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
