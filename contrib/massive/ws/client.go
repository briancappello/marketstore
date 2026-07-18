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

// errConnClosed is returned to control callers (UpdateSubscription) when the
// connection is gone before/while their control frame could be written.
var errConnClosed = errors.New("connection closed")

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

// Prefix returns the subscription/event prefix for a topic (e.g., "AM" for
// minute aggs). It is also the "ev" field value the server stamps on each
// streaming message, so callers can route a multiplexed stream by topic.
func (t Topic) Prefix() string {
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
	actionAuth        action = "auth"
	actionSubscribe   action = "subscribe"
	actionUnsubscribe action = "unsubscribe"
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

// controlReq is a request to write a control frame (subscribe/unsubscribe) on
// the live connection. It is sent on ctrlCh and serviced by writeLoop, keeping
// the write side single-goroutine.
type controlReq struct {
	act    action
	params string
	// result receives exactly one value: the result of writing the control
	// frame (nil on success, non-nil on write error). It MUST be created
	// buffered with capacity 1 by the sender so that writeLoop's single send
	// never blocks — even if the caller has already abandoned the request
	// (e.g. timed out or saw done close). Confirmation of the server's
	// "success" status is handled asynchronously by handleStreamingStatus.
	result chan error
}

// Client is a WebSocket client for the Massive real-time data API.
//
// After Connect(), the client is safe for concurrent UpdateSubscription /
// AddTickers / RemoveTickers calls: these are serialized via ctrlCh (writes
// flow through the single writeLoop goroutine) and subMu (guards the retained
// subscription set). Output / Err / Done / Close behave as before.
type Client struct {
	apiKey string
	url    string

	conn *websocket.Conn

	subMu sync.Mutex     // guards subs (mutated at runtime by UpdateSubscription)
	subs  []subscription // registered subscriptions

	ctrlCh chan controlReq // live control writes, drained by writeLoop

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
		ctrlCh: make(chan controlReq, 64),
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
	c.subMu.Lock()
	c.subs = append(c.subs, subscription{topic: topic, tickers: tickers})
	c.subMu.Unlock()
}

// UpdateSubscription adds or removes tickers for a topic on the LIVE
// connection. Safe to call after Connect() and concurrently. It updates the
// retained subscription set (so a later reconnect replays the current set) and
// writes the control frame. Returns an error if the connection is closed or the
// write fails. A nil return means the frame was written successfully; it does
// NOT mean the server has acked the (un)subscribe (acks are handled
// asynchronously and logged, see handleStreamingStatus).
func (c *Client) UpdateSubscription(act action, topic Topic, tickers ...string) error {
	if len(tickers) == 0 {
		return nil
	}

	// 1. Update the retained set so a reconnect replays the current state.
	c.subMu.Lock()
	switch act {
	case actionSubscribe:
		c.mergeTickersLocked(topic, tickers)
	case actionUnsubscribe:
		c.pruneTickersLocked(topic, tickers)
	}
	c.subMu.Unlock()

	// 2. Build params.
	params := buildSubParams(topic, tickers)

	// 3. Hand the control write to writeLoop and wait for the outcome. The
	// result channel is buffered size 1 so writeLoop's send never blocks even
	// if we stop waiting first (on done or timeout).
	req := controlReq{act: act, params: params, result: make(chan error, 1)}

	timer := time.NewTimer(writeWait)
	defer timer.Stop()

	select {
	case c.ctrlCh <- req:
	case <-c.done:
		return errConnClosed
	case <-timer.C:
		return fmt.Errorf("control write timed out queueing %s", act)
	}

	select {
	case err := <-req.result:
		return err
	case <-c.done:
		return errConnClosed
	case <-timer.C:
		return fmt.Errorf("control write timed out awaiting %s", act)
	}
}

// AddTickers subscribes additional tickers for a topic on the live connection.
func (c *Client) AddTickers(topic Topic, tickers ...string) error {
	return c.UpdateSubscription(actionSubscribe, topic, tickers...)
}

// RemoveTickers unsubscribes tickers for a topic on the live connection.
func (c *Client) RemoveTickers(topic Topic, tickers ...string) error {
	return c.UpdateSubscription(actionUnsubscribe, topic, tickers...)
}

// mergeTickersLocked adds tickers to the retained subscription for topic.
// Caller must hold subMu.
func (c *Client) mergeTickersLocked(topic Topic, tickers []string) {
	idx := -1
	for i := range c.subs {
		if c.subs[i].topic == topic {
			idx = i
			break
		}
	}
	if idx == -1 {
		c.subs = append(c.subs, subscription{topic: topic, tickers: append([]string(nil), tickers...)})
		return
	}
	existing := make(map[string]struct{}, len(c.subs[idx].tickers))
	for _, t := range c.subs[idx].tickers {
		existing[t] = struct{}{}
	}
	for _, t := range tickers {
		if _, ok := existing[t]; !ok {
			c.subs[idx].tickers = append(c.subs[idx].tickers, t)
			existing[t] = struct{}{}
		}
	}
}

// pruneTickersLocked removes tickers from the retained subscription for topic.
// Caller must hold subMu.
func (c *Client) pruneTickersLocked(topic Topic, tickers []string) {
	idx := -1
	for i := range c.subs {
		if c.subs[i].topic == topic {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}
	remove := make(map[string]struct{}, len(tickers))
	for _, t := range tickers {
		remove[t] = struct{}{}
	}
	kept := c.subs[idx].tickers[:0]
	for _, t := range c.subs[idx].tickers {
		if _, ok := remove[t]; !ok {
			kept = append(kept, t)
		}
	}
	c.subs[idx].tickers = kept
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

	// Send subscriptions and wait for each confirmation. Snapshot the retained
	// set under subMu (it may be mutated concurrently by UpdateSubscription
	// once streaming begins, but during the handshake we are the only writer;
	// the lock keeps the read race-free).
	c.subMu.Lock()
	subsSnapshot := make([]subscription, len(c.subs))
	copy(subsSnapshot, c.subs)
	c.subMu.Unlock()
	for _, sub := range subsSnapshot {
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

// RetainedTickers returns a copy of the retained tickers for a topic — the set
// that will be replayed on reconnect. It reflects runtime UpdateSubscription
// adds/removes. Returns nil if the topic has no subscription.
func (c *Client) RetainedTickers(topic Topic) []string {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	for i := range c.subs {
		if c.subs[i].topic == topic {
			out := make([]string, len(c.subs[i].tickers))
			copy(out, c.subs[i].tickers)
			return out
		}
	}
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
	prefix := topic.Prefix()
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
		log.Error("[massive/ws] server error: %s (params=%q)", cm.Message, cm.Params)
	case "success":
		log.Debug("[massive/ws] %s (params=%q)", cm.Message, cm.Params)
	default:
		log.Info("[massive/ws] status %q: %s", cm.Status, cm.Message)
	}
}

// writeLoop sends periodic pings to keep the connection alive and drains
// control requests (subscribe/unsubscribe) so all writes flow through a single
// goroutine. On exit it fails any pending control requests so their callers
// never park forever (see the TOCTOU note in UpdateSubscription).
func (c *Client) writeLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	// On exit, fail any control requests still buffered in ctrlCh. done is
	// closed only by readLoop, so there is a window where writeLoop has
	// returned (e.g. on its own write failure) but done is not yet closed; this
	// drain answers those requests immediately rather than letting them hang.
	defer c.failPendingControl()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case req := <-c.ctrlCh:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			err := c.sendControl(req.act, req.params)
			// Never blocks: result is buffered size 1, so this succeeds even
			// if the caller already gave up waiting.
			req.result <- err
			if err != nil {
				return // write failure tears down like a ping failure
			}
		}
	}
}

// failPendingControl drains ctrlCh non-blockingly and replies to each pending
// request with errConnClosed. Each result is buffered size 1, so every reply
// succeeds without blocking.
func (c *Client) failPendingControl() {
	for {
		select {
		case req := <-c.ctrlCh:
			req.result <- errConnClosed
		default:
			return
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
