package stream_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/utils"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend/stream"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func setup(t *testing.T) {
	t.Helper()

	rootDir := t.TempDir()
	cfg := utils.NewDefaultConfig(rootDir)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())
	stream.Initialize()
}

func TestStream(t *testing.T) {
	setup(t)

	srv := httptest.NewServer(http.HandlerFunc(stream.Handler))

	u, _ := url.Parse(srv.URL + "/ws")
	u.Scheme = "ws"

	conn, resp, err := websocket.DefaultDialer.Dial(u.String(), nil)
	defer func(conn *websocket.Conn) {
		err1 := resp.Body.Close()
		if err2 := conn.Close(); err1 != nil || err2 != nil {
			log.Error("failed to close websocket connection")
		}
	}(conn)
	assert.Nil(t, err)

	// AAPL 5Min bars & all daily bars
	streamKeys := []string{"AAPL/5Min/OHLCV", "*/1D/OHLCV"}

	streamCount := map[string]int{}
	for _, key := range streamKeys {
		streamCount[key] = 0
	}

	buf, err := msgpack.Marshal(stream.SubscribeMessage{Action: "subscribe", TBKs: streamKeys})
	assert.Nil(t, err)

	assert.Nil(t, conn.WriteMessage(websocket.BinaryMessage, buf))

	_, buf, err = conn.ReadMessage()
	assert.Nil(t, err)

	subRespMsg := &stream.SubscribedMessage{}
	err = msgpack.Unmarshal(buf, subRespMsg)
	assert.Nil(t, err)

	assert.Equal(t, "subscribed", subRespMsg.Action)
	assert.Equal(t, len(subRespMsg.TBKs), len(streamKeys))

	bufC := make(chan []byte, 1)
	go readRoutine(conn, bufC)

	// write data
	for i := 0; i < 2; i++ {
		tbk := io.NewTimeBucketKey("AAPL/5Min/OHLCV")
		err = stream.Push(*tbk, genColumns())
		assert.Nil(t, err)
	}

	tbk := io.NewTimeBucketKey("NVDA/1D/OHLCV")
	err = stream.Push(*tbk, genColumns())
	assert.Nil(t, err)

	total := 3 // "AAPL/5Min/OHLCV"=2, "NVDA/1D/OHLCV"=1
	count := 0

	timer := time.NewTimer(5 * time.Second)

	var receivedBufs [][]byte
	for {
		finished := false
		select {
		case buf, ok := <-bufC:
			if ok {
				receivedBufs = append(receivedBufs, buf)
				count++
				if count == total {
					conn.Close()
				}
			} else {
				finished = true
			}
		case <-timer.C:
			t.Fatalf("test timed out [%v]", streamCount)
		}
		if finished {
			break
		}
	}
	handlePayload(t, receivedBufs, map[string]int{"AAPL/5Min/OHLCV": 2, "*/1D/OHLCV": 1})
}

func readRoutine(conn *websocket.Conn, bufC chan []byte) {
	// read routine (handled in client code normally)
	for {
		msgType, buf, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				log.Error("unexpected websocket closure (%v)", err)
			}
			close(bufC)
			return
		}

		switch msgType {
		case websocket.TextMessage, websocket.BinaryMessage:
			bufC <- buf
		case websocket.CloseMessage:
			return
		}
	}
}

func genColumns() map[string]interface{} {
	return map[string]interface{}{
		"Open":   float32(1.0),
		"High":   float32(2.0),
		"Low":    float32(0.5),
		"Close":  float32(1.5),
		"Volume": int32(10),
		"Epoch":  int64(123456789),
	}
}

// connectAndSubscribe creates a new WS client, subscribes to the given keys,
// and returns the connection plus a buffered channel of received messages.
func connectAndSubscribe(t *testing.T, wsURL string, keys []string) (*websocket.Conn, chan []byte) {
	t.Helper()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	assert.Nil(t, err)

	buf, err := msgpack.Marshal(stream.SubscribeMessage{Action: "subscribe", TBKs: keys})
	assert.Nil(t, err)
	assert.Nil(t, conn.WriteMessage(websocket.BinaryMessage, buf))

	// Read subscription ack
	_, buf, err = conn.ReadMessage()
	assert.Nil(t, err)
	subResp := &stream.SubscribedMessage{}
	assert.Nil(t, msgpack.Unmarshal(buf, subResp))
	assert.Equal(t, "subscribed", subResp.Action)

	bufC := make(chan []byte, 100)
	go readRoutine(conn, bufC)
	return conn, bufC
}

// collectMessages reads from a channel until timeout, returning all received payloads.
func collectMessages(bufC chan []byte, timeout time.Duration) []stream.Payload {
	var payloads []stream.Payload
	timer := time.NewTimer(timeout)
	for {
		select {
		case buf, ok := <-bufC:
			if !ok {
				return payloads
			}
			var p stream.Payload
			if err := msgpack.Unmarshal(buf, &p); err == nil {
				payloads = append(payloads, p)
			}
		case <-timer.C:
			return payloads
		}
	}
}

// TestPushDirect verifies that PushDirect only delivers to subscribers
// that explicitly named the symbol, not to wildcard subscribers.
func TestPushDirect(t *testing.T) {
	setup(t)

	srv := httptest.NewServer(http.HandlerFunc(stream.Handler))
	defer srv.Close()
	u, _ := url.Parse(srv.URL + "/ws")
	u.Scheme = "ws"

	// Client A: wildcard subscriber (*/1Min/OHLCV)
	connA, bufA := connectAndSubscribe(t, u.String(), []string{"*/1Min/OHLCV"})
	defer connA.Close()

	// Client B: direct subscriber (AAPL/1Min/OHLCV)
	connB, bufB := connectAndSubscribe(t, u.String(), []string{"AAPL/1Min/OHLCV"})
	defer connB.Close()

	// Allow subscriptions to register
	time.Sleep(50 * time.Millisecond)

	// PushDirect -- should only reach client B
	tbk := io.NewTimeBucketKey("AAPL/1Min/OHLCV")
	err := stream.PushDirect(*tbk, genColumns())
	assert.Nil(t, err)

	// Client B should receive the message
	payloadsB := collectMessages(bufB, 500*time.Millisecond)
	assert.Equal(t, 1, len(payloadsB))
	assert.Equal(t, "AAPL/1Min/OHLCV", payloadsB[0].Key)

	// Client A should NOT receive any message
	payloadsA := collectMessages(bufA, 200*time.Millisecond)
	assert.Equal(t, 0, len(payloadsA))
}

// TestPushBroadcast verifies that normal Push delivers to both
// wildcard and direct subscribers.
func TestPushBroadcast(t *testing.T) {
	setup(t)

	srv := httptest.NewServer(http.HandlerFunc(stream.Handler))
	defer srv.Close()
	u, _ := url.Parse(srv.URL + "/ws")
	u.Scheme = "ws"

	// Client A: wildcard subscriber
	connA, bufA := connectAndSubscribe(t, u.String(), []string{"*/1Min/OHLCV"})
	defer connA.Close()

	// Client B: direct subscriber
	connB, bufB := connectAndSubscribe(t, u.String(), []string{"AAPL/1Min/OHLCV"})
	defer connB.Close()

	time.Sleep(50 * time.Millisecond)

	// Normal Push (broadcast) -- should reach BOTH clients
	tbk := io.NewTimeBucketKey("AAPL/1Min/OHLCV")
	err := stream.Push(*tbk, genColumns())
	assert.Nil(t, err)

	payloadsA := collectMessages(bufA, 500*time.Millisecond)
	assert.Equal(t, 1, len(payloadsA))
	assert.Equal(t, "AAPL/1Min/OHLCV", payloadsA[0].Key)

	payloadsB := collectMessages(bufB, 500*time.Millisecond)
	assert.Equal(t, 1, len(payloadsB))
	assert.Equal(t, "AAPL/1Min/OHLCV", payloadsB[0].Key)
}

// TestPushDirectWithPartialWildcard verifies that PushDirect delivers
// to a subscriber with a wildcard in a non-symbol position (e.g., AAPL/1Min/*).
func TestPushDirectWithPartialWildcard(t *testing.T) {
	setup(t)

	srv := httptest.NewServer(http.HandlerFunc(stream.Handler))
	defer srv.Close()
	u, _ := url.Parse(srv.URL + "/ws")
	u.Scheme = "ws"

	// Client subscribes to AAPL/1Min/* -- wildcard in attrgroup, but symbol is concrete
	conn, bufC := connectAndSubscribe(t, u.String(), []string{"AAPL/1Min/*"})
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	// PushDirect for AAPL -- should match because symbol position is concrete
	tbk := io.NewTimeBucketKey("AAPL/1Min/OHLCV")
	err := stream.PushDirect(*tbk, genColumns())
	assert.Nil(t, err)

	payloads := collectMessages(bufC, 500*time.Millisecond)
	assert.Equal(t, 1, len(payloads))
	assert.Equal(t, "AAPL/1Min/OHLCV", payloads[0].Key)

	// PushDirect for a different symbol should NOT match
	tbk2 := io.NewTimeBucketKey("NVDA/1Min/OHLCV")
	err = stream.PushDirect(*tbk2, genColumns())
	assert.Nil(t, err)

	payloads2 := collectMessages(bufC, 200*time.Millisecond)
	assert.Equal(t, 0, len(payloads2))
}

func handlePayload(t *testing.T, bufs [][]byte, expectedStreamKeyCount map[string]int) {
	t.Helper()

	streamCount := make(map[string]int)
	for streamKey := range expectedStreamKeyCount {
		streamCount[streamKey] = 0
	}

	for _, buf := range bufs {
		var payload *stream.Payload
		err2 := msgpack.Unmarshal(buf, &payload)
		assert.Nil(t, err2)

		payload.Key = strings.Replace(payload.Key, "NVDA", "*", 1)
		if count, ok := streamCount[payload.Key]; !ok {
			t.Fatalf("invalid stream key in payload: %v", *payload)
		} else {
			streamCount[payload.Key] = count + 1
		}
	}

	for streamKey := range expectedStreamKeyCount {
		count, ok := streamCount[streamKey]
		assert.True(t, ok)
		assert.Equal(t, expectedStreamKeyCount[streamKey], count)
	}
}

var setupOnce sync.Once

// setupShared initialises the executor and stream globals once for all tests
// that use it. stream.Initialize reassigns the package-level send channel and
// catalog; the new push-delivery tests keep a connection open across a Push,
// so they share a single stable setup instead of each resetting the globals.
func setupShared(t *testing.T) {
	t.Helper()
	setupOnce.Do(func() { setup(t) })
}

// dialWith opens a client connection to the given stream server, offering
// the supplied subprotocols, and returns the connection plus the
// subprotocol the server selected.
func dialWith(t *testing.T, srv *httptest.Server, subprotocols []string) (*websocket.Conn, string) {
	t.Helper()

	u, _ := url.Parse(srv.URL + "/ws")
	u.Scheme = "ws"

	dialer := websocket.Dialer{Subprotocols: subprotocols}
	conn, resp, err := dialer.Dial(u.String(), nil)
	assert.Nil(t, err)
	if resp != nil {
		defer resp.Body.Close()
	}
	return conn, conn.Subprotocol()
}

func TestStreamSubprotocolNegotiation(t *testing.T) {
	setupShared(t)
	srv := httptest.NewServer(http.HandlerFunc(stream.Handler))
	defer srv.Close()

	cases := []struct {
		name   string
		offer  []string
		expect string
	}{
		{"no offer falls back to msgpack", nil, ""},
		{"msgpack offer", []string{"msgpack"}, "msgpack"},
		{"json offer", []string{"json"}, "json"},
		// Server preference order decides: msgpack is listed first.
		{"both offered prefers msgpack", []string{"json", "msgpack"}, "msgpack"},
		{"unknown offer negotiates nothing", []string{"cbor"}, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, got := dialWith(t, srv, tc.offer)
			defer conn.Close()
			assert.Equal(t, tc.expect, got)
		})
	}
}

func TestStreamJSONSubscribeAck(t *testing.T) {
	setupShared(t)
	srv := httptest.NewServer(http.HandlerFunc(stream.Handler))
	defer srv.Close()

	conn, negotiated := dialWith(t, srv, []string{"json"})
	defer conn.Close()
	assert.Equal(t, "json", negotiated)

	streamKeys := []string{"AAPL/5Min/OHLCV"}
	buf, err := json.Marshal(stream.SubscribeMessage{Action: "subscribe", TBKs: streamKeys})
	assert.Nil(t, err)
	// A JSON client sends a text frame.
	assert.Nil(t, conn.WriteMessage(websocket.TextMessage, buf))

	msgType, buf, err := conn.ReadMessage()
	assert.Nil(t, err)
	assert.Equal(t, websocket.TextMessage, msgType)

	// Field names on the wire must be the json tags, not Go field names.
	var raw map[string]any
	assert.Nil(t, json.Unmarshal(buf, &raw))
	assert.Equal(t, "subscribed", raw["action"])
	assert.NotContains(t, string(buf), `"Action"`)

	var ack stream.SubscribedMessage
	assert.Nil(t, json.Unmarshal(buf, &ack))
	assert.Equal(t, streamKeys, ack.TBKs)
}

func TestStreamJSONErrorMessage(t *testing.T) {
	setupShared(t)
	srv := httptest.NewServer(http.HandlerFunc(stream.Handler))
	defer srv.Close()

	conn, _ := dialWith(t, srv, []string{"json"})
	defer conn.Close()

	// handleInbound rejects any action other than "subscribe".
	buf, err := json.Marshal(stream.SubscribeMessage{Action: "unsubscribe", TBKs: []string{"AAPL/5Min/OHLCV"}})
	assert.Nil(t, err)
	assert.Nil(t, conn.WriteMessage(websocket.TextMessage, buf))

	msgType, buf, err := conn.ReadMessage()
	assert.Nil(t, err)
	assert.Equal(t, websocket.TextMessage, msgType)

	var errMsg stream.ErrorMessage
	assert.Nil(t, json.Unmarshal(buf, &errMsg))
	assert.Contains(t, errMsg.Error, "unknown action")
}

func TestStreamMsgpackUnchangedByNegotiation(t *testing.T) {
	// Regression guard: a client that negotiates nothing must still get
	// binary msgpack frames, exactly as before this work.
	setupShared(t)
	srv := httptest.NewServer(http.HandlerFunc(stream.Handler))
	defer srv.Close()

	conn, negotiated := dialWith(t, srv, nil)
	defer conn.Close()
	assert.Equal(t, "", negotiated)

	streamKeys := []string{"AAPL/5Min/OHLCV"}
	buf, err := msgpack.Marshal(stream.SubscribeMessage{Action: "subscribe", TBKs: streamKeys})
	assert.Nil(t, err)
	assert.Nil(t, conn.WriteMessage(websocket.BinaryMessage, buf))

	msgType, buf, err := conn.ReadMessage()
	assert.Nil(t, err)
	assert.Equal(t, websocket.BinaryMessage, msgType)

	var ack stream.SubscribedMessage
	assert.Nil(t, msgpack.Unmarshal(buf, &ack))
	assert.Equal(t, "subscribed", ack.Action)
	assert.Equal(t, streamKeys, ack.TBKs)
}
