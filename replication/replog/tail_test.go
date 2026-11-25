package replog_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/replog"
)

func TestReadersInsideHotTailNeverTouchDisk(t *testing.T) {
	l, err := replog.Open(t.TempDir(), replog.Options{SegmentBytes: 400, HotTailBytes: 1 << 20})
	require.NoError(t, err)
	defer l.Close()
	appendN(t, l, 1, 200)

	es := readAll(t, l, 150)
	require.Len(t, es, 51)
	require.Equal(t, int64(0), l.DiskReads())
}

func TestReadersBehindHotTailGetEveryEntryInOrder(t *testing.T) {
	// A tail of ~10 entries: older reads must come from disk.
	l, err := replog.Open(t.TempDir(), replog.Options{SegmentBytes: 400, HotTailBytes: 450})
	require.NoError(t, err)
	defer l.Close()
	appendN(t, l, 1, 200)
	require.Greater(t, l.HotTailFirstSeq(), uint64(150), "tail must be bounded by bytes")

	es := readAll(t, l, 1)
	require.Len(t, es, 200)
	for i, e := range es {
		require.Equal(t, uint64(i+1), e.Seq)
		require.Equal(t, payload(i+1), e.Data)
	}
	require.Greater(t, l.DiskReads(), int64(0))
}

// A reader that waits on Notify sees every entry an appender produces, in
// order, while the appender never waits for it.
func TestNotifyWakesTailingReaders(t *testing.T) {
	l, err := replog.Open(t.TempDir(), replog.Options{SegmentBytes: 4000, HotTailBytes: 2000})
	require.NoError(t, err)
	defer l.Close()

	const n = 5000
	const readers = 3
	var wg sync.WaitGroup
	got := make([][]uint64, readers)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			next := uint64(1)
			deadline := time.After(30 * time.Second)
			for next <= n {
				ch := l.Notify()
				es, err := l.Read(next, 100, 0)
				if err != nil {
					t.Errorf("read %d: %v", next, err)
					return
				}
				for _, e := range es {
					got[r] = append(got[r], e.Seq)
				}
				if len(es) > 0 {
					next = es[len(es)-1].Seq + 1
					continue
				}
				select {
				case <-ch:
				case <-deadline:
					t.Errorf("reader %d stalled at %d", r, next)
					return
				}
			}
		}(r)
	}
	appendN(t, l, 1, n)
	wg.Wait()
	for r := 0; r < readers; r++ {
		require.Len(t, got[r], n)
		for i, s := range got[r] {
			require.Equal(t, uint64(i+1), s)
		}
	}
}
