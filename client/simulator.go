// Package client simulates multiple hospital staff clients accessing patient records.
//
// Each client goroutine randomly performs:
//   - Create: generates a new patient record
//   - Read:   fetches an existing patient record
//   - Update: modifies an existing patient record
//
// This demonstrates:
//   - Concurrent access patterns
//   - Mutual exclusion (when two clients update the same patient)
//   - Parallel reads (multiple clients reading simultaneously)
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"distributed-health-system/models"
	"distributed-health-system/utils"
)

// patientNames is a pool of realistic patient names.
var patientNames = []string{
	"Alice Johnson", "Bob Martinez", "Carol White", "David Lee",
	"Eve Thompson", "Frank Garcia", "Grace Kim", "Henry Wilson",
	"Iris Brown", "Jack Davis", "Karen Miller", "Liam Anderson",
	"Mia Taylor", "Noah Jackson", "Olivia Harris", "Peter Clark",
}

// diagnoses and prescriptions are used to generate realistic test data.
var diagnoses = []string{
	"Type 2 Diabetes", "Hypertension", "Asthma", "Pneumonia",
	"Appendicitis", "Migraine", "Arthritis", "Cardiac Arrhythmia",
	"Influenza", "Fracture - Left Femur", "Chronic Kidney Disease", "Anemia",
}

var prescriptions = []string{
	"Metformin 500mg", "Lisinopril 10mg", "Albuterol Inhaler",
	"Amoxicillin 500mg", "Ibuprofen 400mg", "Sumatriptan 50mg",
	"Aspirin 81mg", "Atorvastatin 20mg", "Warfarin 5mg", "Insulin Glargine",
}

var wards = []string{
	"ICU", "Emergency", "Cardiology", "Orthopedics", "General",
	"Pediatrics", "Neurology", "Oncology",
}

var bloodTypes = []string{"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"}

// knownPatientIDs tracks patient IDs created during the simulation.
// Shared across all clients (with mutex) so update/read operations
// target real existing patients.
var (
	knownIDs   []string
	knownIDsMu sync.Mutex
)

// addKnownID registers a newly created patient ID.
func addKnownID(id string) {
	knownIDsMu.Lock()
	defer knownIDsMu.Unlock()
	knownIDs = append(knownIDs, id)
}

// randomPatientID picks a random known patient ID, or "P0001" as fallback.
func randomPatientID() string {
	knownIDsMu.Lock()
	defer knownIDsMu.Unlock()
	if len(knownIDs) == 0 {
		return "P0001"
	}
	return knownIDs[rand.Intn(len(knownIDs))]
}

// ClientState tracks a simulator client's activity for the dashboard.
type ClientState struct {
	*models.ClientState
	mu sync.Mutex
}

// Simulator manages multiple concurrent client goroutines.
type Simulator struct {
	masterAddrs   []string
	currentLeader string
	clients       []*ClientState
	httpClient    *http.Client
	mu            sync.RWMutex
}

// New creates a Simulator that targets the given Master node addresses.
func New(masterAddrs ...string) *Simulator {
	s := &Simulator{
		masterAddrs: masterAddrs,
		httpClient:  &http.Client{Timeout: 5 * time.Second},
	}
	if len(masterAddrs) > 0 {
		s.currentLeader = masterAddrs[0]
	}
	return s
}

// Start spawns n concurrent client goroutines.
// Each goroutine continuously performs random operations (create/read/update).
func (s *Simulator) Start(n int) {
	utils.GlobalLogger.Info("CLIENT", fmt.Sprintf("Starting %d simulated clients...", n))

	for i := 1; i <= n; i++ {
		clientID := fmt.Sprintf("Client-%d", i)
		state := &ClientState{
			ClientState: &models.ClientState{
				ClientID: clientID,
				Status:   "IDLE",
			},
		}
		s.clients = append(s.clients, state)
		go s.runClient(clientID, state)
	}
}

// GetClientStates returns current status of all simulated clients.
func (s *Simulator) GetClientStates() []*models.ClientState {
	result := make([]*models.ClientState, len(s.clients))
	for i, c := range s.clients {
		c.mu.Lock()
		copy := *c.ClientState
		c.mu.Unlock()
		result[i] = &copy
	}
	return result
}

// runClient is the main loop for a single simulated client.
// It continuously performs random operations with random delays.
func (s *Simulator) runClient(clientID string, state *ClientState) {
	// Stagger startup so clients don't all hit the server simultaneously.
	time.Sleep(time.Duration(rand.Intn(3000)) * time.Millisecond)

	opTypes := []string{"CREATE", "READ", "UPDATE", "UPDATE"} // weighted: more updates

	for {
		// Pick a random operation.
		op := opTypes[rand.Intn(len(opTypes))]

		state.mu.Lock()
		state.Status = "REQUESTING"
		state.LastOperation = op
		state.mu.Unlock()

		var patientID string
		var err error

		switch op {
		case "CREATE":
			patientID, err = s.doCreate(clientID)
			if err == nil {
				addKnownID(patientID)
			}
		case "READ":
			patientID = randomPatientID()
			err = s.doRead(clientID, patientID)
		case "UPDATE":
			patientID = randomPatientID()
			err = s.doUpdate(clientID, patientID)
		}

		state.mu.Lock()
		state.LastPatientID = patientID
		state.OpCount++
		state.LastSeen = time.Now()
		if err != nil {
			state.Status = "ERROR"
		} else {
			state.Status = "IDLE"
		}
		state.mu.Unlock()

		// Random sleep between operations: 0.5s – 3s.
		sleepMs := 500 + rand.Intn(2500)
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
}

// ─── Leader Discovery ─────────────────────────────────────────────

func (s *Simulator) getLeader() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentLeader
}

func (s *Simulator) discoverLeader() {
	utils.GlobalLogger.Info("CLIENT", "🔍 Discovering active leader...")

	for _, addr := range s.masterAddrs {
		url := fmt.Sprintf("http://%s/leader", addr)
		resp, err := s.httpClient.Get(url)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		var leader models.NodeInfo
		if err := json.NewDecoder(resp.Body).Decode(&leader); err == nil && leader.Address != "" {
			s.mu.Lock()
			s.currentLeader = leader.Address
			s.mu.Unlock()
			utils.GlobalLogger.Success("CLIENT", fmt.Sprintf("🎯 New leader discovered: %s", leader.Address))
			return
		}
	}
	utils.GlobalLogger.Warn("CLIENT", "❌ Could not reach any master to discover leader")
}

func (s *Simulator) performRequest(method, path string, body []byte) (*models.APIResponse, error) {
	for retry := 0; retry < 3; retry++ {
		leader := s.getLeader()
		url := fmt.Sprintf("http://%s/%s", leader, strings.TrimPrefix(path, "/"))

		var resp *http.Response
		var err error

		if method == http.MethodPost {
			resp, err = s.httpClient.Post(url, "application/json", bytes.NewBuffer(body))
		} else if method == http.MethodPut {
			req, _ := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err = s.httpClient.Do(req)
		} else {
			resp, err = s.httpClient.Get(url)
		}

		if err != nil {
			utils.GlobalLogger.Warn("CLIENT", fmt.Sprintf("Network error to %s: %v. Retrying...", leader, err))
			s.discoverLeader()
			time.Sleep(1 * time.Second)
			continue
		}
		defer resp.Body.Close()

		var apiResp models.APIResponse
		if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
			return nil, err
		}

		if !apiResp.Success && strings.Contains(apiResp.Message, "leader") {
			utils.GlobalLogger.Warn("CLIENT", "Hit standby master. Re-discovering leader...")
			s.discoverLeader()
			time.Sleep(500 * time.Millisecond)
			continue
		}

		return &apiResp, nil
	}
	return nil, fmt.Errorf("failed after retries")
}

// ─────────────────────────────────────────────
// Operations
// ─────────────────────────────────────────────

// doCreate sends POST /patient/create to the Master via performRequest.
func (s *Simulator) doCreate(clientID string) (string, error) {
	req := models.CreatePatientRequest{
		ClientID:     clientID,
		Name:         patientNames[rand.Intn(len(patientNames))],
		Age:          20 + rand.Intn(60),
		Diagnosis:    diagnoses[rand.Intn(len(diagnoses))],
		Prescription: prescriptions[rand.Intn(len(prescriptions))],
		BloodType:    bloodTypes[rand.Intn(len(bloodTypes))],
		Ward:         wards[rand.Intn(len(wards))],
	}

	body, _ := json.Marshal(req)
	utils.GlobalLogger.Info(clientID, "➕ CREATE new patient")

	apiResp, err := s.performRequest(http.MethodPost, "/patient/create", body)
	if err != nil {
		return "", err
	}

	if apiResp.Success && apiResp.Data != nil {
		// Extract patient ID from response.
		b, _ := json.Marshal(apiResp.Data)
		var rec models.PatientRecord
		if json.Unmarshal(b, &rec) == nil && rec.PatientID != "" {
			utils.GlobalLogger.Success(clientID,
				fmt.Sprintf("✅ Created patient %s", rec.PatientID))
			return rec.PatientID, nil
		}
	} else if apiResp != nil && !apiResp.Success {
		utils.GlobalLogger.Warn(clientID, fmt.Sprintf("CREATE rejected: %s", apiResp.Message))
	}
	return "", nil
}

// doRead sends GET /patient/{id} to the Master via performRequest.
func (s *Simulator) doRead(clientID, patientID string) error {
	utils.GlobalLogger.Info(clientID, fmt.Sprintf("📖 READ patient %s", patientID))

	apiResp, err := s.performRequest(http.MethodGet, "/patient/"+patientID, nil)
	if err != nil {
		return err
	}

	if apiResp != nil && apiResp.Success {
		utils.GlobalLogger.Info(clientID, fmt.Sprintf("📖 Read patient %s OK", patientID))
	}
	return nil
}

// doUpdate sends PUT /patient/update/{id} to the Master via performRequest.
func (s *Simulator) doUpdate(clientID, patientID string) error {
	req := models.UpdatePatientRequest{
		ClientID:     clientID,
		Diagnosis:    diagnoses[rand.Intn(len(diagnoses))],
		Prescription: prescriptions[rand.Intn(len(prescriptions))],
		Ward:         wards[rand.Intn(len(wards))],
	}

	body, _ := json.Marshal(req)
	utils.GlobalLogger.Info(clientID, fmt.Sprintf("✏️  UPDATE patient %s", patientID))

	apiResp, err := s.performRequest(http.MethodPut, "/patient/update/"+patientID, body)
	if err != nil {
		return err
	}

	if apiResp != nil && apiResp.Success {
		utils.GlobalLogger.Success(clientID, fmt.Sprintf("✅ Updated patient %s", patientID))
	} else if apiResp != nil {
		utils.GlobalLogger.Warn(clientID, fmt.Sprintf("⚠️  Update for patient %s: %s", patientID, apiResp.Message))
	}
	return nil
}
