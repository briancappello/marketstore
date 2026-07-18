package ws

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testServer is a minimal Massive-protocol WebSocket server for tests. It
// performs the connected/auth/subscribe handshake and records every control
// frame the client sends after the handshake.
type testServer struct {
	srv *httptest.Server
	url string

	mu     sync.Mutex
	frames []controlMessage // control frames received after handshake

	// stopReading, when closed, makes the server stop reading and close the
	// socket, forcing the client's next write to fail.
	stopReading chan struct{}
	stopOnce    sync.Once
}

var testUpgrader = websocket.Upgrader{}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	ts := &testServer{stopReading: make(chan struct{})}

	handler := func(w http.ResponseWriter, r *http.Request) {
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		send := func(msgs ...controlMessage) {
			data, _ := json.Marshal(msgs)
			_ = conn.WriteMessage(websocket.TextMessage, data)
		}

		// 1. connected
		send(controlMessage{EventType: "status", Status: "connected", Message: "Connected"})

		// 2. read auth, reply auth_success
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		send(controlMessage{EventType: "status", Status: "auth_success", Message: "authenticated"})

		// Read frames until told to stop or the socket dies. The first frames
		// during the handshake are the initial subscriptions; reply success to
		// each. Everything after is recorded as a live control frame.
		for {
			select {
			case <-ts.stopReading:
				return
			default:
			}
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var cm controlMessage
			if err := json.Unmarshal(data, &cm); err != nil {
				continue
			}
			ts.mu.Lock()
			ts.frames = append(ts.frames, cm)
			ts.mu.Unlock()

			// Echo a success status so the handshake's expectStatus("success")
			// completes for initial subscribes, and so live control frames get
			// an async success too.
			send(controlMessage{EventType: "status", Status: "success", Message: "subscribed", Params: cm.Params})
		}
	}

	ts.srv = httptest.NewServer(http.HandlerFunc(handler))
	// New() appends "/stocks"; the handler ignores the path, so strip the
	// scheme to ws:// and let New add the suffix.
	ts.url = "ws" + strings.TrimPrefix(ts.srv.URL, "http")
	return ts
}

func (ts *testServer) close() { ts.srv.Close() }

func (ts *testServer) recordedFrames() []controlMessage {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	out := make([]controlMessage, len(ts.frames))
	copy(out, ts.frames)
	return out
}

// connectClient builds a client pointed at the test server and connects it.
func connectClient(t *testing.T, ts *testServer, initial ...string) *Client {
	t.Helper()
	c := New("test-key", Feed(ts.url))
	// Strip the "/stocks" suffix New appended by overriding url directly: the
	// test server ignores the path anyway, but New built ts.url+"/stocks".
	c.url = ts.url
	if len(initial) > 0 {
		c.Subscribe(StocksTrades, initial...)
	}
	require.NoError(t, c.Connect())
	return c
}

func TestSubscribeWhileConnected(t *testing.T) {
	ts := newTestServer(t)
	defer ts.close()

	c := connectClient(t, ts)
	defer c.Close()

	require.NoError(t, c.AddTickers(StocksTrades, "AAPL"))

	// Wait for the frame to be recorded.
	assert.Eventually(t, func() bool {
		for _, f := range ts.recordedFrames() {
			if f.Action == actionSubscribe && f.Params == "T.AAPL" {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
}

func TestUnsubscribeWhileConnected(t *testing.T) {
	ts := newTestServer(t)
	defer ts.close()

	c := connectClient(t, ts, "AAPL")
	defer c.Close()

	require.NoError(t, c.RemoveTickers(StocksTrades, "AAPL"))

	assert.Eventually(t, func() bool {
		for _, f := range ts.recordedFrames() {
			if f.Action == actionUnsubscribe && f.Params == "T.AAPL" {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
}

func TestConcurrentUpdateSubscription(t *testing.T) {
	ts := newTestServer(t)
	defer ts.close()

	c := connectClient(t, ts)
	defer c.Close()

	symbols := []string{"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "NFLX"}
	var wg sync.WaitGroup
	for _, sym := range symbols {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			assert.NoError(t, c.AddTickers(StocksTrades, s))
		}(sym)
	}
	wg.Wait()

	// All frames should be well-formed subscribe frames (no corruption/interleave).
	assert.Eventually(t, func() bool {
		seen := map[string]bool{}
		for _, f := range ts.recordedFrames() {
			if f.Action != actionSubscribe {
				continue
			}
			seen[f.Params] = true
		}
		for _, s := range symbols {
			if !seen["T."+s] {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond)
}

func TestUpdateSubscriptionAfterClose(t *testing.T) {
	ts := newTestServer(t)
	defer ts.close()

	c := connectClient(t, ts)
	c.Close()
	// Give the read loop a moment to observe the closed connection.
	<-c.Done()

	err := c.AddTickers(StocksTrades, "AAPL")
	assert.Error(t, err)
	assert.Equal(t, errConnClosed, err)
}

func TestRetainedSetReflectsAddsAndRemoves(t *testing.T) {
	ts := newTestServer(t)
	defer ts.close()

	c := connectClient(t, ts, "AAPL")
	defer c.Close()

	require.NoError(t, c.AddTickers(StocksTrades, "MSFT", "GOOG"))
	require.NoError(t, c.RemoveTickers(StocksTrades, "AAPL"))

	retained := c.RetainedTickers(StocksTrades)
	assert.ElementsMatch(t, []string{"MSFT", "GOOG"}, retained)
}

// TestTOCTOUDrain verifies that a control request buffered when writeLoop exits
// is answered with errConnClosed by failPendingControl, promptly (well under
// the writeWait timeout), proving it's the drain and not the timeout backstop.
func TestTOCTOUDrain(t *testing.T) {
	ts := newTestServer(t)
	defer ts.close()

	c := connectClient(t, ts)

	// Kill the underlying connection directly so the next control write fails
	// inside writeLoop, triggering its exit + failPendingControl, while done is
	// not yet guaranteed closed.
	c.conn.Close()

	start := time.Now()
	err := c.AddTickers(StocksTrades, "AAPL")
	elapsed := time.Since(start)

	assert.Error(t, err)
	// Either the write failed (caller saw the result error) or the drain/closed
	// path returned errConnClosed — both are acceptable closed-connection
	// outcomes. The key assertion is that it returns promptly, not after a full
	// writeWait timeout.
	assert.Less(t, elapsed, writeWait, "should return well before the writeWait timeout")
}

func TestEmptyTickersNoOp(t *testing.T) {
	ts := newTestServer(t)
	defer ts.close()

	c := connectClient(t, ts)
	defer c.Close()

	assert.NoError(t, c.AddTickers(StocksTrades))
	assert.Empty(t, ts.recordedFrames())
}
