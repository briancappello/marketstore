// Package stream implements websocket interface for streaming in the server core.
// This package is to handle the basic websocket connection handling and message
// routing.  The actual data is pushed by one of the plugins if configured. The
// main motivation of this separation is that the requirements for each streaming
// use case varies.  For particular streaming data handling, please see the document
// of each plugin.
//
// The only requirement in this layer is the server accepts the incoming connection
// and receives the "subscribe" request from the client.  The subscribe request
// must have a valid streaming channel format of TimeBucketKey with three elements
// in it.  Currently we do not check th existence of the requested key.
//
// A plugin can push a message by calling `Push`.  Each message data should be
// enclosed by the structure with "key" (TimeBucketKey string) and "data" (opaque)
// fields.
package stream

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/eapache/channels"
	"github.com/gobwas/glob"
	"github.com/gorilla/websocket"

	"github.com/alpacahq/marketstore/v4/metrics"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/utils/wscodec"
)

const (
	pongWait   = 60 * time.Second
	pingPeriod = 60 * time.Second * 9 / 10
)

var (
	catalog  *Catalog
	send     *channels.InfiniteChannel
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		Subprotocols: wscodec.Subprotocols,
	}
)

// Catalog maintains the set of active subscribers.
type Catalog struct {
	sync.RWMutex
	subs map[*Subscriber]struct{}
}

// Add a new subscriber to the catalog.
func (sc *Catalog) Add(sub *Subscriber) {
	sc.Lock()
	defer sc.Unlock()

	sc.subs[sub] = struct{}{}
}

// Remove a subscriber from the catalog.
func (sc *Catalog) Remove(sub *Subscriber) {
	sc.Lock()
	defer sc.Unlock()

	delete(sc.subs, sub)
}

// NewCatalog initializes the stream catalog.
func NewCatalog() *Catalog {
	return &Catalog{
		subs: map[*Subscriber]struct{}{},
	}
}

// Subscriber includes the connection, and streams to
// manage a given stream client.
type Subscriber struct {
	sync.RWMutex
	c       *websocket.Conn
	done    chan struct{}
	streams map[string]struct{}
	// codec is resolved from the negotiated subprotocol when the
	// connection is accepted and is never mutated afterwards, so it is
	// safe to read without holding the lock. The fan-out loop in stream()
	// depends on that.
	codec wscodec.Codec
	// catalog is the Catalog this subscriber was registered in, captured
	// at accept time. consume() removes itself from this Catalog rather
	// than the package global, so a subsequent Initialize() that swaps the
	// global does not race a still-running subscriber goroutine.
	catalog *Catalog
}

// Subscribed matches the subscriber's subscribed streams
// with the supplied timebucket key string.
func (s *Subscriber) Subscribed(itemKey string) bool {
	s.RLock()
	defer s.RUnlock()
	for stream := range s.streams {
		if g, err := glob.Compile(stream, '/'); err == nil {
			if g.Match(itemKey) {
				return true
			}
		}
	}
	return false
}

// SubscribedDirect matches the subscriber's subscribed streams
// with the supplied timebucket key string, but only considers
// subscription patterns that specify a concrete symbol (i.e., the
// first path segment contains no glob wildcards). This allows
// PushDirect payloads to reach subscribers who explicitly named a
// symbol while skipping wildcard-symbol subscribers like "*/1Min/OHLCV".
func (s *Subscriber) SubscribedDirect(itemKey string) bool {
	s.RLock()
	defer s.RUnlock()
	for stream := range s.streams {
		// Skip patterns with a wildcard in the symbol position (first segment).
		parts := strings.SplitN(stream, "/", 2)
		if len(parts) == 0 || strings.ContainsAny(parts[0], "*?[") {
			continue
		}
		if g, err := glob.Compile(stream, '/'); err == nil {
			if g.Match(itemKey) {
				return true
			}
		}
	}
	return false
}

// SubscribeMessage is an inbound message for the client
// to subscribe to streams.
type SubscribeMessage struct {
	Action string   `msgpack:"action" json:"action"`
	TBKs   []string `msgpack:"tbks" json:"tbks"`
}

// SubscribedMessage is the ack sent back to the client after a
// successful subscribe. The Action field is always "subscribed".
type SubscribedMessage struct {
	Action string   `msgpack:"action" json:"action"`
	TBKs   []string `msgpack:"tbks" json:"tbks"`
}

// ErrorMessage is used to report errors when a client
// subscribes to invalid streams.
type ErrorMessage struct {
	Error string `msgpack:"error" json:"error"`
}

func (s *Subscriber) handleOutbound(buf []byte) error {
	// prevents concurrent write to the websocket connection
	s.Lock()
	defer s.Unlock()
	return s.c.WriteMessage(s.codec.MessageType(), buf)
}

func (s *Subscriber) handleInbound(msg SubscribeMessage) ([]string, error) {
	if msg.Action != "subscribe" {
		return nil, fmt.Errorf("unknown action: %q (expected \"subscribe\")", msg.Action)
	}
	if len(msg.TBKs) > 0 {
		// prevents concurrent read/write of stream map
		s.Lock()
		defer s.Unlock()

		// validate each stream before modifying the subscriber's stream map
		m := map[string]struct{}{}
		for _, stream := range msg.TBKs {
			if !validStream(stream) {
				return nil, fmt.Errorf("%s is an invalid stream", stream)
			}
			m[stream] = struct{}{}
		}
		s.streams = m
	}
	return msg.TBKs, nil
}

func validStream(stream string) bool {
	g, err := glob.Compile("*/*/*", '/')
	if err != nil {
		return false
	}
	return g.Match(stream)
}

func (s *Subscriber) consume() {
	defer func() {
		s.c.Close()
		metrics.WSConnections.Dec()
		s.catalog.Remove(s)
		s.done <- struct{}{}
	}()

	metrics.WSConnections.Inc()
	s.c.SetPongHandler(func(string) error {
		return s.c.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		msgType, buf, err := s.c.ReadMessage()
		if err != nil {
			// Treat "no status" (1005) and "abnormal" (1006) closures as benign
			if !websocket.IsCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseNoStatusReceived,
				websocket.CloseAbnormalClosure,
			) {
				log.Error("unexpected websocket closure (%v)", err)
			}
			return
		}

		switch msgType {
		case websocket.TextMessage, websocket.BinaryMessage:
			m := SubscribeMessage{}

			if err = s.codec.Unmarshal(buf, &m); err != nil {
				log.Error("failed to unmarshal inbound stream message (%v)", err)
				continue
			}
			tbks, inboundErr := s.handleInbound(m)
			if inboundErr != nil {
				errBuf, _ := s.codec.Marshal(ErrorMessage{Error: inboundErr.Error()})
				if err := s.handleOutbound(errBuf); err != nil {
					log.Error("failed to send stream error message (%v)", err)
				}
			} else {
				ack, _ := s.codec.Marshal(SubscribedMessage{Action: "subscribed", TBKs: tbks})
				if err := s.handleOutbound(ack); err != nil {
					log.Error("failed to send stream subscribed ack (%v)", err)
				}
			}
		case websocket.CloseMessage:
			// Acknowledge the close frame as required by RFC 6455 (§ 5.5.1)
			// and give the peer a chance to receive it before we tear down
			// the connection. Without this, the peer reports code 1005/1006.
			_ = s.c.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				time.Now().Add(time.Second),
			)
			return
		}
	}
}

func (s *Subscriber) produce() {
	ticker := time.NewTicker(pingPeriod)
	for {
		select {
		case <-ticker.C:
			s.Lock()
			_ = s.c.WriteMessage(websocket.PingMessage, []byte{})
			s.Unlock()
		case <-s.done:
			return
		}
	}
}

// stream fans out payloads from sendCh to the subscribers in cat. Both are
// passed in rather than read from the package globals so that a subsequent
// Initialize(), which swaps those globals and starts a fresh stream()
// goroutine, cannot race this one.
func stream(sendCh *channels.InfiniteChannel, cat *Catalog) {
	for v := range sendCh.Out() {
		if v == nil {
			continue
		}
		payload, ok := v.(Payload)
		if !ok {
			log.Error("failed to cast payload (%v)", v)
			continue
		}

		// Encode lazily, once per format per payload. Subscribers sharing
		// an encoding share a buffer, so cost scales with the number of
		// distinct formats in use, not with the number of subscribers.
		// A format that fails to marshal is cached as a nil buffer so the
		// failure is not retried for every remaining subscriber.
		encoded := make(map[string][]byte, len(wscodec.Subprotocols))
		encode := func(c wscodec.Codec) ([]byte, bool) {
			if buf, seen := encoded[c.Name()]; seen {
				return buf, buf != nil
			}
			buf, err := c.Marshal(payload)
			if err != nil {
				log.Error("failed to marshal outbound stream payload as %s (%v)", c.Name(), err)
				encoded[c.Name()] = nil
				return nil, false
			}
			encoded[c.Name()] = buf
			return buf, true
		}

		cat.RLock()

		for s := range cat.subs {
			var matched bool
			if payload.Direct {
				matched = s.SubscribedDirect(payload.Key)
			} else {
				matched = s.Subscribed(payload.Key)
			}
			if !matched {
				continue
			}

			// s.codec is fixed at connection time and never mutated, so
			// reading it here without the subscriber lock is safe.
			buf, ok := encode(s.codec)
			if !ok {
				continue
			}
			if err := s.handleOutbound(buf); err != nil {
				log.Error("failed to stream outbound (%s)", err)
			}
		}

		cat.RUnlock()
	}
}

// Payload is used to send data over the websocket.
type Payload struct {
	Key  string      `msgpack:"key" json:"key"`
	Data interface{} `msgpack:"data" json:"data"`
	// Direct is an internal routing flag (not serialized over the wire).
	// When true, the payload is only delivered to subscribers whose
	// subscription pattern specifies a concrete symbol in the first
	// path segment (e.g. "AAPL/1Min/OHLCV"), skipping wildcard-symbol
	// subscribers (e.g. "*/1Min/OHLCV").
	Direct bool `msgpack:"-" json:"-"`
}

// Push sends data over the stream interface to all matching subscribers.
func Push(tbk io.TimeBucketKey, data interface{}) error {
	send.In() <- Payload{Key: tbk.GetItemKey(), Data: data}
	return nil
}

// PushDirect sends data over the stream interface, but only to
// subscribers that explicitly named the symbol (no wildcard in the
// symbol position of their subscription pattern). This is used by the
// watchlist trigger to deliver non-curated symbol data only to clients
// that specifically requested it.
func PushDirect(tbk io.TimeBucketKey, data interface{}) error {
	send.In() <- Payload{Key: tbk.GetItemKey(), Data: data, Direct: true}
	return nil
}

// Initialize builds the send channel as well as the cache, and
// must be called before any data flows over the stream interface.
func Initialize() {
	send = channels.NewInfiniteChannel()
	catalog = NewCatalog()

	go stream(send, catalog)
}

// Shutdown sends a WebSocket close frame to every connected subscriber
// and closes the underlying connections. It should be called during server
// shutdown so that clients receive a clean close (code 1000) rather than
// an abrupt TCP teardown.
func Shutdown() {
	if catalog == nil {
		return
	}

	catalog.RLock()
	subs := make([]*Subscriber, 0, len(catalog.subs))
	for s := range catalog.subs {
		subs = append(subs, s)
	}
	catalog.RUnlock()

	for _, s := range subs {
		// Send a close frame so the client sees code 1000 (normal closure).
		_ = s.c.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "server shutting down"),
			time.Now().Add(time.Second),
		)
		s.c.Close()
	}
}

// Handler hooks into the HTTP interface and handles the incoming
// streaming requests, and upgrades the connection.
func Handler(w http.ResponseWriter, r *http.Request) {
	// upgrade the socket
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("failed to upgrade stream socket (%s)", err)
		return
	}

	// build the subscriber, capturing the current catalog so consume()
	// removes itself from the same one it was added to.
	s := &Subscriber{
		c:       ws,
		done:    make(chan struct{}),
		codec:   wscodec.For(ws.Subprotocol()),
		catalog: catalog,
	}

	if s.c != nil {
		log.Info("new stream listener: %v", ws.RemoteAddr().String())
	}

	s.catalog.Add(s)

	// begin streaming
	go s.consume()
	go s.produce()
}
