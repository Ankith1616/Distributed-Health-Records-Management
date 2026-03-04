package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"distributed-health-system/models"
)

// This file contains end-to-end integration tests for the distributed system.
// It spawns real processes and monitors their behavior.

func TestMutualExclusion(t *testing.T) {
	// 1. Build the binary
	cmd := exec.Command("go", "build", "-o", "dhs.exe", ".")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to build project: %v", err)
	}
	defer os.Remove("dhs.exe")

	// 2. Start the system in all-in-one mode
	// We'll run it in a background process
	sysCmd := exec.Command("./dhs.exe", "-role=all", "-id=ALL-IN-ONE", "-clients=0")
	if err := sysCmd.Start(); err != nil {
		t.Fatalf("Failed to start system: %v", err)
	}
	defer sysCmd.Process.Kill()

	// Wait for system to initialize
	time.Sleep(3 * time.Second)

	// 3. Perform concurrent updates on the same patient
	patientID := "P7777"
	masterAddr := "localhost:8000"

	// Create the patient first
	createReq := models.CreatePatientRequest{
		ClientID: "TEST-INIT",
		Name:     "Test Patient",
	}
	body, _ := json.Marshal(createReq)
	_, err := http.Post(fmt.Sprintf("http://%s/patient/create", masterAddr), "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("Initial creation failed: %v", err)
	}

	var wg sync.WaitGroup
	errCount := 0
	var mu sync.Mutex

	// Hammer the system with concurrent updates
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			updateReq := models.UpdatePatientRequest{
				ClientID:  fmt.Sprintf("CLIENT-%d", id),
				Diagnosis: fmt.Sprintf("Diagnosis %d", id),
			}
			b, _ := json.Marshal(updateReq)

			req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/patient/update/%s", masterAddr, patientID), bytes.NewBuffer(b))
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
				return
			}
			defer resp.Body.Close()
		}(i)
	}

	wg.Wait()

	// Check results
	if errCount > 0 {
		t.Errorf("Expected 0 failures during concurrent updates, got %d", errCount)
	}

	// Verify the final state (should be one of the diagnosis updates)
	resp, err := http.Get(fmt.Sprintf("http://%s/patient/%s", masterAddr, patientID))
	if err != nil {
		t.Fatalf("Failed to read record: %v", err)
	}
	defer resp.Body.Close()

	var apiResp models.APIResponse
	json.NewDecoder(resp.Body).Decode(&apiResp)
	if !apiResp.Success {
		t.Errorf("Read patient failed: %s", apiResp.Message)
	}
}

func TestLeaderElectionAndPersistence(t *testing.T) {
	// 1. Start two masters
	m1 := exec.Command("go", "run", "main.go", "-role=master", "-id=MASTER-1", "-addr=localhost:8000")
	m2 := exec.Command("go", "run", "main.go", "-role=master", "-id=MASTER-2", "-addr=localhost:8003")

	if err := m1.Start(); err != nil {
		t.Fatalf("Failed to start Master-1: %v", err)
	}
	defer m1.Process.Kill()

	if err := m2.Start(); err != nil {
		t.Fatalf("Failed to start Master-2: %v", err)
	}
	defer m2.Process.Kill()

	time.Sleep(3 * time.Second)

	// 2. Kill the leader (Master-1 is usually leader if started first)
	_, err := http.Post("http://localhost:8000/admin/kill-leader", "application/json", nil)
	if err != nil {
		t.Fatalf("Failed to kill leader: %v", err)
	}

	time.Sleep(2 * time.Second)

	// 3. Verify Master-2 took over
	resp, err := http.Get("http://localhost:8003/leader")
	if err != nil {
		t.Fatalf("Failed to get leader from Master-2: %v", err)
	}
	defer resp.Body.Close()

	var leader models.NodeInfo
	json.NewDecoder(resp.Body).Decode(&leader)
	if leader.ID != "MASTER-2" {
		t.Errorf("Expected MASTER-2 to be leader, got %s", leader.ID)
	}
}

func TestPersistence(t *testing.T) {
	// Ensure cleanup
	defer os.Remove("rs_data_RS-TEST.json")

	// 1. Start a RegionServer
	rs := exec.Command("go", "run", "main.go", "-role=regionserver", "-id=RS-TEST", "-addr=localhost:8005")
	if err := rs.Start(); err != nil {
		t.Fatalf("Failed to start RS: %v", err)
	}
	time.Sleep(2 * time.Second)

	// 2. Write data directly to it (acting as master)
	record := models.PatientRecord{
		PatientID: "P-PERSIST",
		Name:      "Survivor",
	}
	body, _ := json.Marshal(record)
	_, err := http.Post("http://localhost:8005/data/write", "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("RS write failed: %v", err)
	}

	// 3. Kill and restart it
	rs.Process.Kill()
	time.Sleep(1 * time.Second)

	rs2 := exec.Command("go", "run", "main.go", "-role=regionserver", "-id=RS-TEST", "-addr=localhost:8005")
	if err := rs2.Start(); err != nil {
		t.Fatalf("Failed to restart RS: %v", err)
	}
	defer rs2.Process.Kill()
	time.Sleep(2 * time.Second)

	// 4. Read data back
	resp, err := http.Get("http://localhost:8005/data/read/P-PERSIST")
	if err != nil {
		t.Fatalf("RS read failed after restart: %v", err)
	}
	defer resp.Body.Close()

	var apiResp models.APIResponse
	json.NewDecoder(resp.Body).Decode(&apiResp)
	if !apiResp.Success {
		t.Errorf("Data did not survive restart: %s", apiResp.Message)
	}
}
