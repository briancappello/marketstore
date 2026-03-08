package replayworker

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	msgpack "github.com/vmihailenco/msgpack"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	// minStepMs is the minimum non-zero step interval in milliseconds.
	// Any step > 0 and < minStepMs is clamped to minStepMs.
	minStepMs = 10
)

// Time layouts accepted for the start/end fields.
var timeLayouts = []string{
	"2006-01-02 15:04:05-07:00",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02T15:04:05-07:00",
	"2006-01-02T15:04:05Z07:00",
	time.RFC3339,
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// SubscribeMessage is the inbound message from the client to request
// a historical data replay.
type SubscribeMessage struct {
	Action string   `msgpack:"action"`
	TBKs   []string `msgpack:"tbks"`
	Start  string   `msgpack:"start"`
	End    string   `msgpack:"end"`
	Step   int      `msgpack:"step"`
}

// Payload is the outbound data message, matching the live stream format.
type Payload struct {
	Key  string      `msgpack:"key"`
	Data interface{} `msgpack:"data"`
}

// SubscribedMessage is the ack sent back after a successful subscribe.
type SubscribedMessage struct {
	Action string   `msgpack:"action"`
	TBKs   []string `msgpack:"tbks"`
}

// EndMessage signals the completion of a replay session.
type EndMessage struct {
	Action string `msgpack:"action"`
}

// ErrorMessage reports an error to the client.
type ErrorMessage struct {
	Error string `msgpack:"error"`
}

// QueryFunc is a function that queries historical data for a single TBK
// within a time range. It is used as a seam for testing.
type QueryFunc func(tbk *io.TimeBucketKey, start, end time.Time) (*io.ColumnSeries, error)

// Session manages a single replay WebSocket connection.
type Session struct {
	mu      sync.Mutex
	conn    *websocket.Conn
	done    chan struct{}
	queryFn QueryFunc
}

// NewSession creates a new replay session for the given WebSocket connection.
func NewSession(conn *websocket.Conn) *Session {
	return &Session{
		conn:    conn,
		done:    make(chan struct{}),
		queryFn: defaultQueryRange,
	}
}

// NewSessionWithQuery creates a replay session with a custom query function.
// This is primarily used for testing.
func NewSessionWithQuery(conn *websocket.Conn, qf QueryFunc) *Session {
	return &Session{
		conn:    conn,
		done:    make(chan struct{}),
		queryFn: qf,
	}
}

// Run is the main session loop. It reads subscribe messages, executes
// the replay, then sends an end message and closes the connection.
// The session supports retries: if a subscribe message is invalid or
// there is no data, an error is sent and the session waits for the
// next message. Once a replay starts, client disconnect is detected
// via a background goroutine.
func (s *Session) Run() {
	defer s.close()

	for {
		_, buf, err := s.conn.ReadMessage()
		if err != nil {
			// Client disconnected or error — exit silently.
			return
		}

		var msg SubscribeMessage
		if err = msgpack.Unmarshal(buf, &msg); err != nil {
			s.sendError(fmt.Sprintf("invalid message format: %v", err))
			continue
		}

		// Validate the message before starting the replay.
		if vErr := s.validateSubscribe(msg); vErr != nil {
			s.sendError(vErr.Error())
			continue
		}

		// Send subscribed ack before beginning replay.
		s.sendSubscribed(msg.TBKs)

		// Start a disconnect watcher now that we're about to begin
		// the replay loop (we won't be reading messages anymore).
		go s.watchDisconnect()

		if err = s.executeReplay(msg); err != nil {
			s.sendError(err.Error())
			return
		}

		// Replay completed successfully — send end and close.
		s.sendEnd()
		return
	}
}

// validateSubscribe checks that the subscribe message is well-formed.
func (s *Session) validateSubscribe(msg SubscribeMessage) error {
	if msg.Action != "subscribe" {
		return fmt.Errorf("unknown action: %q (expected \"subscribe\")", msg.Action)
	}
	if len(msg.TBKs) == 0 {
		return fmt.Errorf("tbks must not be empty")
	}

	if _, err := parseTime(msg.Start); err != nil {
		return fmt.Errorf("invalid start time %q: %w", msg.Start, err)
	}

	// An empty End means "up to the latest available data".
	if msg.End != "" {
		if _, err := parseTime(msg.End); err != nil {
			return fmt.Errorf("invalid end time %q: %w", msg.End, err)
		}

		start, _ := parseTime(msg.Start)
		end, _ := parseTime(msg.End)
		if !end.After(start) {
			return fmt.Errorf("end time must be after start time")
		}
	}

	return nil
}

// executeReplay queries historical data and streams it to the client
// at the pace specified by the step interval.
func (s *Session) executeReplay(msg SubscribeMessage) error {
	start, _ := parseTime(msg.Start)
	var end time.Time
	if msg.End != "" {
		end, _ = parseTime(msg.End)
	} else {
		end = time.Now()
	}
	step := normalizeStep(msg.Step)

	// Query historical data for each TBK.
	type tbkData struct {
		itemKey string
		epochs  []int64
		rows    []map[string]interface{}
	}

	// Expand any multi-symbol TBKs (e.g. "AAPL,MSFT/1Min/OHLCV") into
	// individual TBKs before querying.
	expandedTBKs := expandTBKs(msg.TBKs)

	var allData []tbkData
	for _, tbk := range expandedTBKs {
		cs, qErr := s.queryFn(tbk, start, end)
		if qErr != nil {
			return fmt.Errorf("query failed for %s: %w", tbk.GetItemKey(), qErr)
		}
		if cs == nil || cs.Len() == 0 {
			return fmt.Errorf("no data found for %s in range [%s, %s]",
				tbk.GetItemKey(), msg.Start, msg.End)
		}

		epochs := cs.GetEpoch()
		rows := extractRows(cs)

		allData = append(allData, tbkData{
			itemKey: tbk.GetItemKey(),
			epochs:  epochs,
			rows:    rows,
		})
	}

	// Build a unified, sorted timeline of unique epochs across all TBKs.
	epochSet := make(map[int64]struct{})
	for _, td := range allData {
		for _, ep := range td.epochs {
			epochSet[ep] = struct{}{}
		}
	}

	sortedEpochs := make([]int64, 0, len(epochSet))
	for ep := range epochSet {
		sortedEpochs = append(sortedEpochs, ep)
	}
	sort.Slice(sortedEpochs, func(i, j int) bool {
		return sortedEpochs[i] < sortedEpochs[j]
	})

	// Build per-TBK epoch→row index maps for O(1) lookup during replay.
	type epochIndex struct {
		itemKey string
		byEpoch map[int64]int
		rows    []map[string]interface{}
	}
	indexes := make([]epochIndex, len(allData))
	for i, td := range allData {
		m := make(map[int64]int, len(td.epochs))
		for j, ep := range td.epochs {
			m[ep] = j
		}
		indexes[i] = epochIndex{
			itemKey: td.itemKey,
			byEpoch: m,
			rows:    td.rows,
		}
	}

	// Stream bars in epoch order.
	var sleepDuration time.Duration
	if step > 0 {
		sleepDuration = time.Duration(step) * time.Millisecond
	}

	for _, epoch := range sortedEpochs {
		// Check for client disconnect.
		select {
		case <-s.done:
			return nil
		default:
		}

		// Send all TBKs that have a bar at this epoch.
		for _, idx := range indexes {
			rowIdx, ok := idx.byEpoch[epoch]
			if !ok {
				continue
			}
			payload := Payload{
				Key:  idx.itemKey,
				Data: idx.rows[rowIdx],
			}
			if err := s.sendPayload(payload); err != nil {
				return fmt.Errorf("write failed: %w", err)
			}
		}

		// Sleep between time steps.
		if sleepDuration > 0 {
			select {
			case <-s.done:
				return nil
			case <-time.After(sleepDuration):
			}
		}
	}

	return nil
}

// expandTBKs takes a list of TBK strings which may contain comma-separated
// symbols (e.g. "AAPL,MSFT/1Min/OHLCV") and expands them into individual
// TimeBucketKeys (e.g. "AAPL/1Min/OHLCV", "MSFT/1Min/OHLCV").
// Single-symbol TBKs pass through unchanged.
func expandTBKs(tbkStrs []string) []*io.TimeBucketKey {
	var expanded []*io.TimeBucketKey
	for _, tbkStr := range tbkStrs {
		tbk := io.NewTimeBucketKey(tbkStr)
		symbols := tbk.GetMultiItemInCategory("Symbol")
		if len(symbols) <= 1 {
			expanded = append(expanded, tbk)
			continue
		}
		// Build individual TBKs for each symbol, preserving the
		// timeframe and attribute group.
		tf := tbk.GetItemInCategory("Timeframe")
		ag := tbk.GetItemInCategory("AttributeGroup")
		for _, sym := range symbols {
			expanded = append(expanded,
				io.NewTimeBucketKey(fmt.Sprintf("%s/%s/%s", sym, tf, ag)))
		}
	}
	return expanded
}

// defaultQueryRange queries historical data for a single TBK within a time range
// using the executor and planner.
func defaultQueryRange(tbk *io.TimeBucketKey, start, end time.Time) (*io.ColumnSeries, error) {
	cDir := executor.ThisInstance.CatalogDir

	q := planner.NewQuery(cDir)
	q.AddTargetKey(tbk)
	q.SetRange(start, end)
	q.SetRowLimit(io.FIRST, math.MaxInt32)

	parsed, err := q.Parse()
	if err != nil {
		return nil, fmt.Errorf("query parse: %w", err)
	}

	scanner, err := executor.NewReader(parsed)
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}

	csm, err := scanner.Read()
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}

	cs := csm[*tbk]
	return cs, nil
}

// extractRows converts a ColumnSeries into a slice of maps, one per row.
// Each map contains column name → value pairs (e.g. {"Epoch": 12345, "Open": 100.5}).
func extractRows(cs *io.ColumnSeries) []map[string]interface{} {
	nRows := cs.Len()
	rows := make([]map[string]interface{}, nRows)

	columns := cs.GetColumns()

	for i := 0; i < nRows; i++ {
		row := make(map[string]interface{}, len(columns))
		for name, col := range columns {
			s := reflect.ValueOf(col)
			if s.Kind() == reflect.Slice && i < s.Len() {
				row[name] = s.Index(i).Interface()
			}
		}
		rows[i] = row
	}

	return rows
}

// sendPayload marshals and writes a Payload to the WebSocket.
func (s *Session) sendPayload(p Payload) error {
	buf, err := msgpack.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteMessage(websocket.BinaryMessage, buf)
}

// sendSubscribed sends a SubscribedMessage ack to the client.
func (s *Session) sendSubscribed(tbks []string) {
	msg := SubscribedMessage{Action: "subscribed", TBKs: tbks}
	buf, err := msgpack.Marshal(msg)
	if err != nil {
		log.Error("[streamreplay] marshal subscribed message: %v", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.conn.WriteMessage(websocket.BinaryMessage, buf); err != nil {
		log.Error("[streamreplay] send subscribed message: %v", err)
	}
}

// sendEnd sends an EndMessage and logs.
func (s *Session) sendEnd() {
	msg := EndMessage{Action: "end"}
	buf, err := msgpack.Marshal(msg)
	if err != nil {
		log.Error("[streamreplay] marshal end message: %v", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.conn.WriteMessage(websocket.BinaryMessage, buf); err != nil {
		log.Error("[streamreplay] send end message: %v", err)
	}
}

// sendError sends an ErrorMessage to the client.
func (s *Session) sendError(errMsg string) {
	msg := ErrorMessage{Error: errMsg}
	buf, err := msgpack.Marshal(msg)
	if err != nil {
		log.Error("[streamreplay] marshal error message: %v", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.conn.WriteMessage(websocket.BinaryMessage, buf); err != nil {
		log.Error("[streamreplay] send error message: %v", err)
	}
}

// watchDisconnect reads from the WebSocket to detect when the client
// disconnects. When the read fails (connection closed), it signals done.
func (s *Session) watchDisconnect() {
	for {
		_, _, err := s.conn.ReadMessage()
		if err != nil {
			close(s.done)
			return
		}
	}
}

// close terminates the WebSocket connection.
func (s *Session) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "replay complete"),
		time.Now().Add(time.Second),
	)
	s.conn.Close()
}

// parseTime attempts to parse a time string using multiple accepted layouts.
func parseTime(value string) (time.Time, error) {
	for _, layout := range timeLayouts {
		t, err := time.Parse(layout, value)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse time %q", value)
}

// normalizeStep clamps the step value. A step of 0 means no delay.
// Any positive step below minStepMs is raised to minStepMs.
func normalizeStep(step int) int {
	if step < 0 {
		return 0
	}
	if step > 0 && step < minStepMs {
		return minStepMs
	}
	return step
}
