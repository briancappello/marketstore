package executor_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type FakeTrigger struct {
	calledWith [][]interface{}
	fireC      chan struct{}
	toPanic    bool
}

func NewFakeTrigger(toPanic bool) *FakeTrigger {
	return &FakeTrigger{
		fireC:   make(chan struct{}),
		toPanic: toPanic,
	}
}

// Fire sends a message to fireC channel when a record is triggered.
func (t *FakeTrigger) Fire(keyPath string, records []trigger.Record) {
	defer func() { t.fireC <- struct{}{} }()

	if t.toPanic {
		panic("panic")
	}
	t.calledWith = append(t.calledWith, []interface{}{keyPath, records})
}

func TestTriggerPluginDispatcher(t *testing.T) {
	t.Parallel()

	type record struct {
		keyPath string
	}
	tests := []struct {
		name              string
		trigger           *FakeTrigger
		on                string
		records           []record
		wantCalledWith    string
		wantCalledWithLen int
	}{
		{
			name:    "only records that match the keypath should be triggered",
			trigger: NewFakeTrigger(false),
			on:      "AAPL/1Min/OHLCV",
			records: []record{
				{keyPath: "AAPL/1Min/OHLCV/2017.bin"},
				{keyPath: "TSLA/1Min/OHLCV/2017.bin"},
			},
			wantCalledWith:    "AAPL/1Min/OHLCV/2017.bin",
			wantCalledWithLen: 1,
		},
		{
			name:    "recovered when panic is triggered",
			trigger: NewFakeTrigger(true),
			on:      "AAPL/1Min/OHLCV",
			records: []record{
				{keyPath: "AAPL/1Min/OHLCV/2017.bin"},
			},
			wantCalledWith:    "AAPL/1Min/OHLCV/2017.bin",
			wantCalledWithLen: 0,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// --- given ---
			matchers := []*trigger.Matcher{trigger.NewMatcher(tt.trigger, tt.on)}
			tpd := executor.StartNewTriggerPluginDispatcher(matchers)
			fakeBuffer, ok := io.SwapSliceData([]int64{0, 5}, byte(0)).([]byte)
			assert.True(t, ok)

			// --- when
			for _, r := range tt.records {
				tpd.AppendRecord(r.keyPath, wal.OffsetIndexBuffer(fakeBuffer).IndexAndPayload())
			}
			tpd.DispatchRecords()

			<-tt.trigger.fireC // wait until fired

			// --- then ---
			assert.Equal(t, len(tt.trigger.calledWith), tt.wantCalledWithLen)
			if tt.wantCalledWithLen > 0 {
				calledWith2, ok := tt.trigger.calledWith[0][0].(string)
				require.True(t, ok)
				assert.Equal(t, calledWith2, tt.wantCalledWith)
			}
		})
	}
}

// drainTrigger consumes Fire signals so the dispatcher's fire goroutines do
// not block on the unbuffered fireC channel.
type drainTrigger struct {
	fireC chan struct{}
	done  chan struct{}
}

func newDrainTrigger() *drainTrigger {
	d := &drainTrigger{fireC: make(chan struct{}, 1024), done: make(chan struct{})}
	return d
}

func (d *drainTrigger) Fire(_ string, _ []trigger.Record) {
	select {
	case d.fireC <- struct{}{}:
	default:
	}
}

// TestTriggerPluginDispatcher_ConcurrentAppendDispatch exercises the data
// race that the mu mutex in TriggerPluginDispatcher guards against: many
// goroutines calling AppendRecord concurrently with DispatchRecords (which
// swaps out and nils the underlying map). Run with -race to detect the
// unsynchronized map access. Without the mutex this fails under -race
// (concurrent map read/write or a panic from "concurrent map writes").
func TestTriggerPluginDispatcher_ConcurrentAppendDispatch(t *testing.T) {
	t.Parallel()

	trig := newDrainTrigger()
	matchers := []*trigger.Matcher{trigger.NewMatcher(trig, "AAPL/1Min/OHLCV")}
	tpd := executor.StartNewTriggerPluginDispatcher(matchers)

	fakeBuffer, ok := io.SwapSliceData([]int64{0, 5}, byte(0)).([]byte)
	require.True(t, ok)
	rec := wal.OffsetIndexBuffer(fakeBuffer).IndexAndPayload()

	const (
		writers          = 8
		appendsPerWriter = 500
		dispatchRounds   = 200
	)

	var wg sync.WaitGroup

	// Writers continuously append records.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			keyPath := "AAPL/1Min/OHLCV/2017.bin" + strconv.Itoa(id)
			for i := 0; i < appendsPerWriter; i++ {
				tpd.AppendRecord(keyPath, rec)
			}
		}(w)
	}

	// A concurrent dispatcher repeatedly swaps and drains the map.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < dispatchRounds; i++ {
			tpd.DispatchRecords()
		}
	}()

	wg.Wait()
	// Final dispatch to flush anything left after the writers finished.
	tpd.DispatchRecords()
}
