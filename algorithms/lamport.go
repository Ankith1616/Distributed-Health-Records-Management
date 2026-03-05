package algorithms

import (
	"sync"
)

type LamportClock struct {
	Time int
	mu   sync.Mutex
}

// Tick increments the local logical clock before internal events.
func (lc *LamportClock) Tick() {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.Time++
}

// SendEvent increments the clock when sending a message.
func (lc *LamportClock) SendEvent() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.Time++
	return lc.Time
}

// ReceiveEvent updates the clock based on the received timestamp.
func (lc *LamportClock) ReceiveEvent(receivedTimestamp int) int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if receivedTimestamp > lc.Time {
		lc.Time = receivedTimestamp
	}
	lc.Time++
	return lc.Time
}
