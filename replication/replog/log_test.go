package replog_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/replog"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }

func payload(i int) []byte { return []byte(fmt.Sprintf("tg-%06d", i)) }

func appendN(t *testing.T, l *replog.Log, from, n int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		_, err := l.Append(int64(1000+i), payload(i))
		require.NoError(t, err)
	}
}

func readAll(t *testing.T, l *replog.Log, from uint64) []replog.Entry {
	t.Helper()
	var out []replog.Entry
	for {
		es, err := l.Read(from, 7, 0)
		require.NoError(t, err)
		if len(es) == 0 {
			return out
		}
		out = append(out, es...)
		from = es[len(es)-1].Seq + 1
	}
}

func segmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	require.NoError(t, err)
	sort.Strings(m)
	return m
}

func TestAppendAssignsConsecutiveSequences(t *testing.T) {
	l, err := replog.Open(t.TempDir(), replog.Options{SegmentBytes: 400})
	require.NoError(t, err)
	defer l.Close()

	for i := 1; i <= 50; i++ {
		seq, err := l.Append(int64(1000+i), payload(i))
		require.NoError(t, err)
		require.Equal(t, uint64(i), seq)
	}
	es := readAll(t, l, 1)
	require.Len(t, es, 50)
	for i, e := range es {
		require.Equal(t, uint64(i+1), e.Seq)
		require.Equal(t, int64(1001+i), e.TGID)
		require.Equal(t, payload(i+1), e.Data)
	}
	require.Equal(t, uint64(1), l.FirstSeq())
	require.Equal(t, uint64(50), l.LastSeq())
	require.Greater(t, len(segmentFiles(t, l.Dir())), 1, "small segments must rotate")
}

func TestReopenContinuesTheSequence(t *testing.T) {
	dir := t.TempDir()
	l, err := replog.Open(dir, replog.Options{SegmentBytes: 400})
	require.NoError(t, err)
	appendN(t, l, 1, 30)
	require.NoError(t, l.Close())

	l, err = replog.Open(dir, replog.Options{SegmentBytes: 400})
	require.NoError(t, err)
	defer l.Close()
	require.Equal(t, uint64(30), l.LastSeq())
	require.Equal(t, int64(1030), l.LastTGID())
	seq, err := l.Append(2000, []byte("after restart"))
	require.NoError(t, err)
	require.Equal(t, uint64(31), seq)

	es := readAll(t, l, 1)
	require.Len(t, es, 31)
	require.Equal(t, []byte("after restart"), es[30].Data)
}

// A process killed mid-append leaves a torn entry at the end of the active
// segment. Open must drop it, and continue from the last whole entry.
func TestOpenTruncatesTornAndCorruptTail(t *testing.T) {
	for _, damage := range []string{"torn", "corrupt"} {
		t.Run(damage, func(t *testing.T) {
			dir := t.TempDir()
			l, err := replog.Open(dir, replog.Options{})
			require.NoError(t, err)
			appendN(t, l, 1, 10)
			require.NoError(t, l.Close())

			files := segmentFiles(t, dir)
			require.Len(t, files, 1)
			b, err := os.ReadFile(files[0])
			require.NoError(t, err)
			entry := len(b) / 10
			switch damage {
			case "torn":
				b = b[:len(b)-entry/2]
			case "corrupt":
				b[len(b)-1] ^= 0xff
			}
			require.NoError(t, os.WriteFile(files[0], b, 0o600))

			l, err = replog.Open(dir, replog.Options{})
			require.NoError(t, err)
			defer l.Close()
			require.Equal(t, uint64(9), l.LastSeq())
			seq, err := l.Append(5000, []byte("x"))
			require.NoError(t, err)
			require.Equal(t, uint64(10), seq)
			es := readAll(t, l, 1)
			require.Len(t, es, 10)
			require.Equal(t, []byte("x"), es[9].Data)
		})
	}
}

func TestRetentionBySize(t *testing.T) {
	dir := t.TempDir()
	l, err := replog.Open(dir, replog.Options{SegmentBytes: 400, MaxBytes: 1200})
	require.NoError(t, err)
	defer l.Close()
	appendN(t, l, 1, 100)
	require.NoError(t, l.Sync())
	before := l.FirstSeq()
	l.EnforceRetention()

	require.LessOrEqual(t, l.Bytes(), int64(1200)+400)
	require.Greater(t, l.FirstSeq(), before)
	require.Equal(t, uint64(100), l.LastSeq())

	_, err = l.Read(1, 1, 0)
	var gap *replog.GapError
	require.True(t, errors.As(err, &gap))
	require.Equal(t, replog.GapRetention, gap.Reason)
	require.Equal(t, l.FirstSeq(), gap.Next)
	require.Len(t, readAll(t, l, gap.Next), int(100-gap.Next+1))

	// Deleted segments are gone from disk too, and a reopen agrees.
	require.NoError(t, l.Close())
	l2, err := replog.Open(dir, replog.Options{})
	require.NoError(t, err)
	defer l2.Close()
	require.Equal(t, gap.Next, l2.FirstSeq())
	require.Equal(t, uint64(100), l2.LastSeq())
}

func TestRetentionByAgeIgnoresReaders(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)}
	l, err := replog.Open(t.TempDir(), replog.Options{SegmentBytes: 400, MaxAge: 48 * time.Hour, Now: clock.Now})
	require.NoError(t, err)
	defer l.Close()

	appendN(t, l, 1, 20) // old
	oldLast := l.LastSeq()
	clock.t = clock.t.Add(72 * time.Hour)
	appendN(t, l, 21, 20) // new
	require.NoError(t, l.Sync())
	l.EnforceRetention()

	require.Greater(t, l.FirstSeq(), uint64(1))
	require.LessOrEqual(t, l.FirstSeq(), oldLast+1, "only the old entries' segments go")
	require.GreaterOrEqual(t, l.OldestWallclock(), clock.t.Add(-72*time.Hour).UnixNano())
	for _, e := range readAll(t, l, l.FirstSeq()) {
		if e.Seq > oldLast {
			require.Equal(t, clock.t.UnixNano(), e.Wallclock)
		}
	}
}

// Sealed segments are indexed lazily on first read; reads that start in the
// middle of a segment, or span several, must still return every entry once.
func TestReadsSpanSealedSegmentsAfterReopen(t *testing.T) {
	dir := t.TempDir()
	l, err := replog.Open(dir, replog.Options{SegmentBytes: 2000})
	require.NoError(t, err)
	appendN(t, l, 1, 500)
	require.NoError(t, l.Close())

	l, err = replog.Open(dir, replog.Options{SegmentBytes: 2000})
	require.NoError(t, err)
	defer l.Close()
	for _, from := range []uint64{1, 63, 64, 65, 200, 499, 500} {
		es := readAll(t, l, from)
		require.Len(t, es, int(500-from+1), "from %d", from)
		for i, e := range es {
			require.Equal(t, from+uint64(i), e.Seq)
			require.Equal(t, payload(int(e.Seq)), e.Data)
		}
	}
	es, err := l.Read(501, 10, 0)
	require.NoError(t, err)
	require.Empty(t, es)
}

func TestBytesFromCountsEntriesToTheEnd(t *testing.T) {
	dir := t.TempDir()
	l, err := replog.Open(dir, replog.Options{SegmentBytes: 1000})
	require.NoError(t, err)
	appendN(t, l, 1, 300) // every entry is 32 + 9 bytes
	const size = 41
	check := func(l *replog.Log) {
		for _, seq := range []uint64{1, 2, 63, 64, 65, 150, 300} {
			got, err := l.BytesFrom(seq)
			require.NoError(t, err)
			require.Equal(t, int64(300-seq+1)*size, got, "from %d", seq)
		}
		got, err := l.BytesFrom(301)
		require.NoError(t, err)
		require.Zero(t, got)
	}
	check(l)
	require.NoError(t, l.Close())
	l, err = replog.Open(dir, replog.Options{SegmentBytes: 1000})
	require.NoError(t, err)
	defer l.Close()
	check(l)
}
