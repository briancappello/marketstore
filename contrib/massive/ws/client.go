// Package ws provides a WebSocket client for the Massive real-time data API.
//
// This is a purpose-built client for MarketStore's Massive plugin. It supports
// only the Stocks market in raw-data mode and does not perform autonomous
// reconnection — the caller is responsible for retry logic.
//
// Key design decisions (and how they differ from the upstream client-go library):
//
//   - Synchronous Connect: Connect() blocks until the server confirms auth and
//     subscriptions. The caller knows the connection is fully established before
//     proceeding. This eliminates phantom "bare" connections that the upstream
//     library creates when auth/subscription messages are sent asynchronously.
//
//   - No autonomous reconnection: when the connection drops, the read loop
//     pushes an error and exits. The caller decides when and how to reconnect
//     (e.g., wall-clock scheduling for market hours).
//
//   - max_connections is a first-class fatal error: the upstream library logs it
//     as an "unknown status message" and continues. We return ErrConnectionLimit
//     immediately.
//
//   - Buffered error channel: the upstream library uses an unbuffered channel,
//     which causes goroutine deadlocks when the consumer has already exited.
//     We use a buffered channel (size 1) so the sender never blocks.
package ws

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Timing constants matching the upstream Massive WebSocket protocol.
const (
	writeWait      = 5 * time.Second
	pongWait       = 30 * time.Second
	pingPeriod     = pongWait - 5*time.Second // 25s
	maxMessageSize = 1_000_000                // 1 MB

	// handshakeTimeout is the maximum time Connect() will wait for the server
	// to confirm auth and subscriptions before giving up.
	handshakeTimeout = 10 * time.Second
)

// Sentinel errors returned by Connect() and the message loop.
var (
	ErrAuthFailed      = errors.New("authentication failed")
	ErrConnectionLimit = errors.New("connection limit exceeded")
)

// Feed is the WebSocket server URL to connect to.
type Feed string

const (
	RealTime Feed = "wss://socket.massive.com"
	Delayed  Feed = "wss://delayed.massive.com"
)

// Topic identifies a data stream to subscribe to.
type Topic uint8

const (
	StocksSecAggs Topic = iota
	StocksMinAggs
	StocksTrades
	StocksQuotes
)

// prefix returns the subscription prefix for a topic (e.g., "AM" for minute aggs).
func (t Topic) prefix() string {
	switch t {
	case StocksSecAggs:
		return "A"
	case StocksMinAggs:
		return "AM"
	case StocksTrades:
		return "T"
	case StocksQuotes:
		return "Q"
	default:
		return ""
	}
}

// String returns a human-readable name for the topic.
func (t Topic) String() string {
	switch t {
	case StocksSecAggs:
		return "StocksSecAggs"
	case StocksMinAggs:
		return "StocksMinAggs"
	case StocksTrades:
		return "StocksTrades"
	case StocksQuotes:
		return "StocksQuotes"
	default:
		return fmt.Sprintf("Topic(%d)", t)
	}
}

// --- Wire protocol types ---

type action string

const (
	actionAuth      action = "auth"
	actionSubscribe action = "subscribe"
)

// controlMessage is the JSON structure for auth, subscribe, and status messages.
type controlMessage struct {
	Action    action `json:"action,omitempty"`
	Params    string `json:"params,omitempty"`
	EventType string `json:"ev,omitempty"`
	Status    string `json:"status,omitempty"`
	Message   string `json:"message,omitempty"`
}

// --- Client ---

// Client is a WebSocket client for the Massive real-time data API.
// It is not safe for concurrent use — callers should use one Client per
// goroutine and create a new Client for each connection attempt.
type Client struct {
	apiKey string
	url    string

	conn *websocket.Conn

	subs []subscription // registered subscriptions

	outCh chan json.RawMessage // data messages (buffered)
	errCh chan error           // fatal errors (buffered, size 1)
	done  chan struct{}        // closed when read loop exits

	closeOnce sync.Once
}

type subscription struct {
	topic   Topic
	tickers []string
}

// New creates a new Client. Call Subscribe() to register topics, then Connect()
// to establish the connection.
func New(apiKey string, feed Feed) *Client {
	url := string(feed) + "/stocks"
	return &Client{
		apiKey: apiKey,
		url:    url,
		outCh:  make(chan json.RawMessage, 100000),
		errCh:  make(chan error, 1),
		done:   make(chan struct{}),
	}
}

// Subscribe registers a topic and tickers for subscription. Must be called
// before Connect(). If tickers is empty or contains "*", subscribes to all.
func (c *Client) Subscribe(topic Topic, tickers ...string) {
	if len(tickers) == 0 {
		tickers = []string{"*"}
	}
	c.subs = append(c.subs, subscription{topic: topic, tickers: tickers})
}

// Connect dials the server and performs the full handshake synchronously:
//  1. TCP + WebSocket dial
//  2. Send auth, wait for auth_success (or auth_failed / max_connections)
//  3. Send subscriptions, wait for success confirmation for each
//
// On success, starts background read/write goroutines and returns nil.
// On failure, closes the connection and returns the error — no cleanup needed.
func (c *Client) Connect() error {
	// Dial.
	conn, resp, err := websocket.DefaultDialer.Dial(c.url, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.url, err)
	}
	if resp.StatusCode != 101 {
		conn.Close()
		return fmt.Errorf("server returned HTTP %d, expected 101", resp.StatusCode)
	}

	conn.SetReadLimit(maxMessageSize)
	if err := conn.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		conn.Close()
		return fmt.Errorf("set read deadline: %w", err)
	}
	c.conn = conn

	// --- Handshake phase: synchronous reads until auth + subscriptions confirmed ---

	// Read the initial "connected" status.
	if err := c.expectStatus("connected"); err != nil {
		c.conn.Close()
		return fmt.Errorf("waiting for connected status: %w", err)
	}

	// Send auth.
	if err := c.sendControl(actionAuth, c.apiKey); err != nil {
		c.conn.Close()
		return fmt.Errorf("send auth: %w", err)
	}

	// Wait for auth_success.
	if err := c.expectStatus("auth_success"); err != nil {
		c.conn.Close()
		return err // ErrAuthFailed or ErrConnectionLimit are returned unwrapped
	}

	// Send subscriptions and wait for each confirmation.
	for _, sub := range c.subs {
		params := buildSubParams(sub.topic, sub.tickers)
		if err := c.sendControl(actionSubscribe, params); err != nil {
			c.conn.Close()
			return fmt.Errorf("send subscribe for %s: %w", sub.topic, err)
		}
		if err := c.expectStatus("success"); err != nil {
			c.conn.Close()
			return fmt.Errorf("subscribe %s: %w", sub.topic, err)
		}
	}

	// --- Handshake complete. Switch to streaming mode. ---

	// Set up pong handler for keepalive.
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	// Reset read deadline for streaming (pong handler will maintain it).
	if err := conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		c.conn.Close()
		return fmt.Errorf("set streaming read deadline: %w", err)
	}

	go c.readLoop()
	go c.writeLoop()

	return nil
}

// Output returns the channel of incoming data messages. Each message is an
// individual JSON object (e.g., a single trade or bar), not the server's
// array wrapper.
func (c *Client) Output() <-chan json.RawMessage {
	return c.outCh
}

// Err returns the fatal error channel. At most one error is ever sent.
func (c *Client) Err() <-chan error {
	return c.errCh
}

// Done returns a channel that is closed when the read loop exits (connection
// lost or Close() called).
func (c *Client) Done() <-chan struct{} {
	return c.done
}

// Close terminates the WebSocket connection and stops background goroutines.
// Safe to call multiple times.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		if c.conn != nil {
			c.conn.Close()
		}
	})
}

// --- Internal: handshake helpers ---

// sendControl marshals and sends a control message (auth or subscribe).
func (c *Client) sendControl(act action, params string) error {
	msg, err := json.Marshal(controlMessage{Action: act, Params: params})
	if err != nil {
		return err
	}
	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return c.conn.WriteMessage(websocket.TextMessage, msg)
}

// expectStatus reads messages until it finds a status message matching want.
// Returns nil on match. Returns a sentinel error for auth_failed and
// max_connections. Returns a generic error for unexpected statuses.
func (c *Client) expectStatus(want string) error {
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read during handshake: %w", err)
		}

		msgs, err := parseMessages(data)
		if err != nil {
			return fmt.Errorf("parse during handshake: %w", err)
		}

		for _, msg := range msgs {
			var cm controlMessage
			if err := json.Unmarshal(msg, &cm); err != nil {
				continue
			}

			// Only process status messages during handshake.
			if cm.EventType != "status" {
				continue
			}

			switch cm.Status {
			case want:
				return nil
			case "auth_failed":
				return ErrAuthFailed
			case "max_connections":
				log.Error("[massive/ws] %s: %s", cm.Status, cm.Message)
				return ErrConnectionLimit
			case "connected", "auth_success", "success":
				// Expected status but not the one we're waiting for; keep reading.
				continue
			case "error":
				return fmt.Errorf("server error: %s", cm.Message)
			default:
				log.Warn("[massive/ws] unexpected status %q during handshake: %s", cm.Status, cm.Message)
				continue
			}
		}
	}
}

// buildSubParams creates the comma-separated subscription parameter string
// (e.g., "AM.AAPL,AM.MSFT" or "AM.*").
func buildSubParams(topic Topic, tickers []string) string {
	prefix := topic.prefix()
	parts := make([]string, len(tickers))
	for i, t := range tickers {
		parts[i] = prefix + "." + t
	}
	return strings.Join(parts, ",")
}

// parseMessages unmarshals the server's wire format. The server sends either
// a JSON array of objects or a single object.
func parseMessages(data []byte) ([]json.RawMessage, error) {
	// Try array first (most common).
	var msgs []json.RawMessage
	if err := json.Unmarshal(data, &msgs); err == nil {
		return msgs, nil
	}
	// Fall back to single object.
	var single json.RawMessage
	if err := json.Unmarshal(data, &single); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}
	return []json.RawMessage{single}, nil
}

// --- Internal: streaming goroutines ---

// readLoop reads messages from the WebSocket and dispatches them to outCh.
// On any error, it pushes to errCh and closes done.
func (c *Client) readLoop() {
	defer func() {
		close(c.done)
	}()

	for {
		// Reset read deadline on every successful read (data acts as keepalive).
		c.conn.SetReadDeadline(time.Now().Add(pongWait))

		_, data, err := c.conn.ReadMessage()
		if err != nil {
			// Classify the error for the caller.
			if websocket.IsCloseError(err, websocket.ClosePolicyViolation) {
				c.pushErr(ErrConnectionLimit)
			} else if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.pushErr(fmt.Errorf("connection closed unexpectedly: %w", err))
			} else {
				c.pushErr(fmt.Errorf("read error: %w", err))
			}
			return
		}

		msgs, err := parseMessages(data)
		if err != nil {
			log.Warn("[massive/ws] failed to parse message: %v", err)
			continue
		}

		for _, msg := range msgs {
			// Check if this is a status message.
			var cm controlMessage
			if err := json.Unmarshal(msg, &cm); err == nil && cm.EventType == "status" {
				c.handleStreamingStatus(cm)
				continue
			}

			// Data message — push to output.
			select {
			case c.outCh <- msg:
			default:
				log.Warn("[massive/ws] output channel full, dropping message")
			}
		}
	}
}

// handleStreamingStatus processes status messages that arrive during streaming
// (after the handshake). Most are informational; max_connections is fatal.
func (c *Client) handleStreamingStatus(cm controlMessage) {
	switch cm.Status {
	case "max_connections":
		log.Error("[massive/ws] %s: %s", cm.Status, cm.Message)
		c.pushErr(ErrConnectionLimit)
		c.conn.Close() // force readLoop to exit
	case "error":
		log.Error("[massive/ws] server error: %s", cm.Message)
	case "success":
		log.Debug("[massive/ws] %s", cm.Message)
	default:
		log.Info("[massive/ws] status %q: %s", cm.Status, cm.Message)
	}
}

// writeLoop sends periodic pings to keep the connection alive.
func (c *Client) writeLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// pushErr sends an error to errCh without blocking (buffer size 1, first error wins).
func (c *Client) pushErr(err error) {
	select {
	case c.errCh <- err:
	default:
	}
}
