// Package storage provides a thread-safe in-memory data store for patient records.
// It supports replication to remote RegionServers over HTTP (replication factor = 2).
package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"distributed-health-system/models"
	"distributed-health-system/utils"
	"io/ioutil"
	"os"
)

// DataStore is a thread-safe in-memory store for PatientRecords.
// Each RegionServer owns one DataStore instance.
type DataStore struct {
	mu      sync.RWMutex
	records map[string]*models.PatientRecord
	ownerID string // which RegionServer this store belongs to
}

// NewDataStore creates a DataStore for the given RegionServer ID.
// It attempts to load existing data from disk if available.
func NewDataStore(ownerID string) *DataStore {
	ds := &DataStore{
		records: make(map[string]*models.PatientRecord),
		ownerID: ownerID,
	}
	ds.loadFromDisk()
	return ds
}

// Get retrieves a patient record by ID.
// Returns (record, true) if found, (nil, false) otherwise.
// Uses RLock so multiple concurrent reads are allowed — simulating parallel read access.
func (ds *DataStore) Get(patientID string) (*models.PatientRecord, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	rec, ok := ds.records[patientID]
	if !ok {
		return nil, false
	}
	// Return a copy to prevent race conditions on the caller side.
	copy := *rec
	return &copy, true
}

// Put writes or overwrites a patient record.
// Uses a full write lock — no reads allowed during write.
func (ds *DataStore) Put(record *models.PatientRecord) error {
	if record == nil {
		return fmt.Errorf("cannot store nil record")
	}
	if record.PatientID == "" {
		return fmt.Errorf("record must have a PatientID")
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	record.LastUpdated = time.Now()
	// Tag the record with this store's owner.
	found := false
	for _, s := range record.StoredOn {
		if s == ds.ownerID {
			found = true
			break
		}
	}
	if !found {
		record.StoredOn = append(record.StoredOn, ds.ownerID)
	}

	ds.records[record.PatientID] = record

	// Persistence: flush to disk (optional: do it asynchronously)
	go ds.flushToDisk()

	return nil
}

// Delete removes a patient record.
func (ds *DataStore) Delete(patientID string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	delete(ds.records, patientID)
	go ds.flushToDisk()
}

// GetAll returns all patient records as a slice (copy).
func (ds *DataStore) GetAll() []*models.PatientRecord {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	result := make([]*models.PatientRecord, 0, len(ds.records))
	for _, rec := range ds.records {
		copy := *rec
		result = append(result, &copy)
	}
	return result
}

// Count returns the number of stored records.
func (ds *DataStore) Count() int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return len(ds.records)
}

// Replicate sends a patient record to a remote RegionServer via HTTP POST.
// This implements the HDFS-inspired replication: each write is propagated
// to a secondary RegionServer so data survives single-node failure.
func (ds *DataStore) Replicate(record *models.PatientRecord, targetAddr string) error {
	body, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	url := fmt.Sprintf("http://%s/replicate", targetAddr)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		utils.GlobalLogger.Warn("STORAGE",
			fmt.Sprintf("Replication to %s failed: %v", targetAddr, err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("replication target returned status %d", resp.StatusCode)
	}

	utils.GlobalLogger.Success("STORAGE",
		fmt.Sprintf("Replication of patient %s to %s successful", record.PatientID, targetAddr))
	return nil
}

// ─── Persistence ──────────────────────────────────────────────────

func (ds *DataStore) filename() string {
	return fmt.Sprintf("rs_data_%s.json", ds.ownerID)
}

func (ds *DataStore) flushToDisk() {
	ds.mu.RLock()
	data, err := json.MarshalIndent(ds.records, "", "  ")
	ds.mu.RUnlock()

	if err != nil {
		utils.GlobalLogger.Error("STORAGE", fmt.Sprintf("Failed to marshal for flush: %v", err))
		return
	}

	if err := ioutil.WriteFile(ds.filename(), data, 0644); err != nil {
		utils.GlobalLogger.Error("STORAGE", fmt.Sprintf("Failed to write to disk: %v", err))
	} else {
		// Log sparingly to avoid spam
	}
}

func (ds *DataStore) loadFromDisk() {
	if _, err := os.Stat(ds.filename()); os.IsNotExist(err) {
		return
	}

	data, err := ioutil.ReadFile(ds.filename())
	if err != nil {
		utils.GlobalLogger.Error("STORAGE", fmt.Sprintf("Failed to read from disk: %v", err))
		return
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	if err := json.Unmarshal(data, &ds.records); err != nil {
		utils.GlobalLogger.Error("STORAGE", fmt.Sprintf("Failed to unmarshal from disk: %v", err))
	} else {
		utils.GlobalLogger.Success("STORAGE",
			fmt.Sprintf("Restored %d records from disk for %s", len(ds.records), ds.ownerID))
	}
}
