// Package consensus implements a majority-vote consensus algorithm for the distributed system.
//
// Inspired by Two-Phase Commit (2PC) and Paxos:
//   - Phase 1 (Propose): Master sends a proposal to all RegionServers
//   - Phase 2 (Decide): If majority vote YES → Commit; otherwise → Abort
//
// Each RegionServer simulates realistic behavior:
//   - Votes YES if it's healthy
//   - May vote NO with a small probability (simulating network issues or disk errors)
//   - Failed servers do not respond (timeout)
package consensus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"distributed-health-system/models"
	"distributed-health-system/utils"
)

const (
	// voteTimeout is how long the master waits for each RegionServer vote.
	voteTimeout = 3 * time.Second
)

// RegionServerRef is a lightweight reference to a RegionServer used by consensus.
type RegionServerRef struct {
	ID      string
	Address string
	Status  models.NodeStatus
}

// Manager orchestrates consensus rounds across RegionServers.
type Manager struct {
	mu      sync.Mutex
	history []*models.ConsensusRound
}

// New creates a new consensus Manager.
func New() *Manager {
	return &Manager{
		history: make([]*models.ConsensusRound, 0),
	}
}

// Propose initiates a consensus round for an operation.
//
// Process:
//  1. Build a VoteRequest with the operation details
//  2. Send it concurrently to all online RegionServers via HTTP POST /consensus/vote
//  3. Collect YES/NO responses with timeout
//  4. Count votes: majority (> n/2) YES → COMMITTED; otherwise → ABORTED
//
// Returns a ConsensusRound summarising the decision.
func (cm *Manager) Propose(
	operationID, operation string,
	record *models.PatientRecord,
	servers []*RegionServerRef,
) *models.ConsensusRound {

	round := &models.ConsensusRound{
		OperationID: operationID,
		Operation:   operation,
		PatientID:   record.PatientID,
		Votes:       make([]*models.VoteResponse, 0),
		Timestamp:   time.Now(),
	}

	utils.GlobalLogger.Info("CONSENSUS",
		fmt.Sprintf("📢 Proposing %s for patient %s (op: %s)",
			operation, record.PatientID, operationID))

	// Filter to only online servers (failed servers don't participate).
	online := make([]*RegionServerRef, 0)
	for _, s := range servers {
		if s.Status == models.StatusOnline {
			online = append(online, s)
		}
	}

	if len(online) == 0 {
		round.Result = "ABORTED"
		utils.GlobalLogger.Error("CONSENSUS",
			fmt.Sprintf("❌ No online RegionServers — aborting %s", operationID))
		cm.record(round)
		return round
	}

	// Send vote requests concurrently using goroutines + channels.
	type voteResult struct {
		resp *models.VoteResponse
		err  error
	}
	votesCh := make(chan voteResult, len(online))

	req := &models.VoteRequest{
		OperationID: operationID,
		Operation:   operation,
		Record:      record,
	}
	body, _ := json.Marshal(req)

	for _, srv := range online {
		go func(s *RegionServerRef) {
			resp, err := sendVoteRequest(s, body)
			votesCh <- voteResult{resp: resp, err: err}
		}(srv)
	}

	// Collect votes with timeout.
	yesCount := 0
	noCount := 0
	timeout := time.After(voteTimeout)

	for i := 0; i < len(online); i++ {
		select {
		case result := <-votesCh:
			if result.err != nil || result.resp == nil {
				noCount++
				round.Votes = append(round.Votes, &models.VoteResponse{
					Vote:   "NO",
					Reason: "timeout or error",
				})
				utils.GlobalLogger.Warn("CONSENSUS",
					fmt.Sprintf("⚠️  Vote failed from a server: %v", result.err))
			} else {
				round.Votes = append(round.Votes, result.resp)
				if result.resp.Vote == "YES" {
					yesCount++
					utils.GlobalLogger.Info("CONSENSUS",
						fmt.Sprintf("  ✅ %s votes YES", result.resp.ServerID))
				} else {
					noCount++
					utils.GlobalLogger.Warn("CONSENSUS",
						fmt.Sprintf("  ❌ %s votes NO (%s)", result.resp.ServerID, result.resp.Reason))
				}
			}
		case <-timeout:
			// Any remaining votes are counted as NO (timeout = failure).
			remaining := len(online) - i
			for j := 0; j < remaining; j++ {
				noCount++
				round.Votes = append(round.Votes, &models.VoteResponse{
					Vote:   "NO",
					Reason: "timeout",
				})
			}
			utils.GlobalLogger.Warn("CONSENSUS", "⏱️  Vote timeout — remaining servers counted as NO")
			i = len(online) // break the loop
		}
	}

	// Majority decision: strictly more than half must vote YES.
	majority := len(online)/2 + 1
	if yesCount >= majority {
		round.Result = "COMMITTED"
		utils.GlobalLogger.Success("CONSENSUS",
			fmt.Sprintf("✅ Consensus COMMITTED — YES:%d NO:%d (need %d)", yesCount, noCount, majority))
	} else {
		round.Result = "ABORTED"
		utils.GlobalLogger.Error("CONSENSUS",
			fmt.Sprintf("❌ Consensus ABORTED — YES:%d NO:%d (need %d)", yesCount, noCount, majority))
	}

	cm.record(round)
	return round
}

// GetHistory returns all past consensus rounds (most recent last).
func (cm *Manager) GetHistory() []*models.ConsensusRound {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	result := make([]*models.ConsensusRound, len(cm.history))
	copy(result, cm.history)
	return result
}

// GetLast returns the most recent consensus round, or nil.
func (cm *Manager) GetLast() *models.ConsensusRound {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if len(cm.history) == 0 {
		return nil
	}
	return cm.history[len(cm.history)-1]
}

// record saves a completed round to history (keeps last 50).
func (cm *Manager) record(round *models.ConsensusRound) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.history = append(cm.history, round)
	if len(cm.history) > 50 {
		cm.history = cm.history[len(cm.history)-50:]
	}
}

// sendVoteRequest sends an HTTP POST to a RegionServer's consensus endpoint
// and returns its vote response.
func sendVoteRequest(server *RegionServerRef, body []byte) (*models.VoteResponse, error) {
	url := fmt.Sprintf("http://%s/consensus/vote", server.Address)
	client := &http.Client{Timeout: voteTimeout}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("HTTP request to %s failed: %w", server.Address, err)
	}
	defer resp.Body.Close()

	var voteResp models.VoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return nil, fmt.Errorf("failed to decode vote from %s: %w", server.Address, err)
	}
	return &voteResp, nil
}
