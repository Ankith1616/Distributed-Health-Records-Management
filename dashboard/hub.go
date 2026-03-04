// Package dashboard provides the WebSocket hub and HTTP handlers for the
// real-time monitoring dashboard.
//
// This implementation uses only the Go standard library (net/http, crypto/sha1,
// encoding/base64) to implement a minimal WebSocket server per RFC 6455.
// No third-party dependencies are needed.
package dashboard

import (
	"crypto/sha1" //nolint:gosec — SHA1 is required by the WebSocket RFC 6455 handshake spec
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"distributed-health-system/models"
)

// ─────────────────────────────────────────────
// WebSocket Constants (RFC 6455)
// ─────────────────────────────────────────────

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// WebSocket frame opcodes.
const (
	opText  = 0x1
	opClose = 0x8
	opPong  = 0xA
	opPing  = 0x9
)

// ─────────────────────────────────────────────
// wsConn — raw WebSocket connection
// ─────────────────────────────────────────────

// wsConn wraps a raw net.Conn upgraded from HTTP to WebSocket.
type wsConn struct {
	conn net.Conn
	mu   sync.Mutex
}

// writeText sends a UTF-8 text frame to the browser client.
// WebSocket framing (RFC 6455):
//
//	Byte 0: FIN(1) + RSV(000) + opcode(0001 = text)
//	Byte 1: MASK(0) + payload length (7-bit or extended)
//	Payload: raw bytes
func (c *wsConn) writeText(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	length := len(data)
	var header []byte

	// First byte: FIN bit set + text opcode.
	header = append(header, 0x81)

	// Payload length encoding.
	if length <= 125 {
		header = append(header, byte(length))
	} else if length <= 65535 {
		header = append(header, 126,
			byte(length>>8), byte(length))
	} else {
		header = append(header, 127,
			0, 0, 0, 0,
			byte(length>>24), byte(length>>16),
			byte(length>>8), byte(length))
	}

	c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(data)
	return err
}

// readFrame reads and decodes a single WebSocket frame.
// Browser clients always send masked frames (RFC 6455 §5.3).
// Returns opcode and unmasked payload.
func (c *wsConn) readFrame() (opcode byte, payload []byte, err error) {
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	header := make([]byte, 2)
	if _, err = io.ReadFull(c.conn, header); err != nil {
		return
	}

	// fin := (header[0] & 0x80) != 0  // not used for control
	opcode = header[0] & 0x0F
	masked := (header[1] & 0x80) != 0
	payLen := int(header[1] & 0x7F)

	// Extended payload length.
	switch payLen {
	case 126:
		ext := make([]byte, 2)
		if _, err = io.ReadFull(c.conn, ext); err != nil {
			return
		}
		payLen = int(ext[0])<<8 | int(ext[1])
	case 127:
		ext := make([]byte, 8)
		if _, err = io.ReadFull(c.conn, ext); err != nil {
			return
		}
		payLen = int(ext[4])<<24 | int(ext[5])<<16 | int(ext[6])<<8 | int(ext[7])
	}

	// Masking key (4 bytes), only from clients.
	var maskKey [4]byte
	if masked {
		if _, err = io.ReadFull(c.conn, maskKey[:]); err != nil {
			return
		}
	}

	// Payload.
	payload = make([]byte, payLen)
	if _, err = io.ReadFull(c.conn, payload); err != nil {
		return
	}

	// Unmask.
	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return
}

func (c *wsConn) close() {
	c.conn.Close()
}

// ─────────────────────────────────────────────
// Hub — manages all connected browser clients
// ─────────────────────────────────────────────

// wsClient represents one connected browser tab.
type wsClient struct {
	conn *wsConn
	send chan []byte
}

// Hub manages all WebSocket clients and broadcasts SystemState JSON.
type Hub struct {
	mu      sync.RWMutex
	clients map[*wsClient]bool
}

// NewHub creates an empty Hub.
func NewHub() *Hub {
	return &Hub{clients: make(map[*wsClient]bool)}
}

// BroadcastState serializes the SystemState and sends it to all connected browsers.
func (h *Hub) BroadcastState(state *models.SystemState) {
	data, err := json.Marshal(state)
	if err != nil {
		return
	}

	h.mu.RLock()
	clients := make([]*wsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, c := range clients {
		select {
		case c.send <- data:
		default:
			h.remove(c)
		}
	}
}

func (h *Hub) register(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = true
	h.mu.Unlock()
}

func (h *Hub) remove(c *wsClient) {
	h.mu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
		c.conn.close()
	}
	h.mu.Unlock()
}

// writePump sends queued messages to a browser client.
func (h *Hub) writePump(c *wsClient) {
	for msg := range c.send {
		if err := c.conn.writeText(msg); err != nil {
			h.remove(c)
			return
		}
	}
}

// readPump reads frames from the browser (needed for ping/pong/close).
func (h *Hub) readPump(c *wsClient) {
	defer h.remove(c)
	for {
		op, _, err := c.conn.readFrame()
		if err != nil {
			return
		}
		switch op {
		case opClose:
			return
		case opPing:
			// Send pong.
			c.conn.mu.Lock()
			c.conn.conn.Write([]byte{0x8A, 0x00}) // FIN + pong, 0 length
			c.conn.mu.Unlock()
		}
	}
}

// ─────────────────────────────────────────────
// HTTP Upgrade Handler
// ─────────────────────────────────────────────

// WSHandler upgrades an HTTP GET to a WebSocket connection per RFC 6455.
//
// Handshake:
//  1. Validate Upgrade: websocket + Connection: Upgrade headers
//  2. Compute Sec-WebSocket-Accept = base64(SHA1(key + GUID))
//  3. Respond with 101 Switching Protocols
//  4. Hijack the underlying TCP connection for frame I/O
func (h *Hub) WSHandler(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Upgrade") != "websocket" {
		http.Error(w, "Not a WebSocket request", http.StatusBadRequest)
		return
	}

	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "Missing Sec-WebSocket-Key", http.StatusBadRequest)
		return
	}

	// Compute accept key: SHA1(key + magic GUID), base64 encoded.
	h1 := sha1.New() //nolint:gosec
	fmt.Fprintf(h1, "%s%s", key, wsGUID)
	accept := base64.StdEncoding.EncodeToString(h1.Sum(nil))

	// Hijack underlying TCP connection before writing response.
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "WebSocket not supported", http.StatusInternalServerError)
		return
	}

	conn, rw, err := hj.Hijack()
	if err != nil {
		log.Printf("Hijack error: %v", err)
		return
	}

	// Write 101 Switching Protocols response directly on the raw connection.
	resp := fmt.Sprintf(
		"HTTP/1.1 101 Switching Protocols\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Accept: %s\r\n\r\n",
		accept,
	)
	if _, err := rw.WriteString(resp); err != nil {
		conn.Close()
		return
	}
	if err := rw.Flush(); err != nil {
		conn.Close()
		return
	}

	ws := &wsConn{conn: conn}
	client := &wsClient{
		conn: ws,
		send: make(chan []byte, 64),
	}

	h.register(client)
	go h.writePump(client)
	go h.readPump(client)
}
