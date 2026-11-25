package replog_test

import (
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/replog"
)

// diskFull fails segment writes while full is set.
type diskFull struct{ full atomic.Bool }

func (d *diskFull) writeAt(f *os.File, b []byte, off int64) (int, error) {
	if d.full.Load() {
		return 0, syscall.ENOSPC
	}
	return f.WriteAt(b, off)
}

func TestFailedAppendBreaksLogAndRecoveryLeavesHole(t *testing.T) {
	disk := &diskFull{}
	l, err := replog.Open(t.TempDir(), replog.Options{WriteAt: disk.writeAt})
	require.NoError(t, err)
	defer l.Close()
	appendN(t, l, 1, 10)

	disk.full.Store(true)
	for i := 0; i < 3; i++ {
		_, err := l.Append(int64(5000+i), []byte("lost"))
		require.ErrorIs(t, err, syscall.ENOSPC)
	}
	broken, from := l.Broken()
	require.True(t, broken)
	require.Equal(t, uint64(11), from)

	// While broken, a reader at the break waits: it is not told a gap whose
	// end is unknown.
	es, err := l.Read(11, 10, 0)
	require.NoError(t, err)
	require.Empty(t, es)

	disk.full.Store(false)
	seq, err := l.Append(6000, []byte("back"))
	require.NoError(t, err)
	require.Equal(t, uint64(14), seq, "lost appends consume their sequences")
	broken, _ = l.Broken()
	require.False(t, broken)

	_, err = l.Read(11, 10, 0)
	var gap *replog.GapError
	require.True(t, errors.As(err, &gap))
	require.Equal(t, replog.GapBroken, gap.Reason)
	require.Equal(t, uint64(14), gap.Next)
	es, err = l.Read(14, 10, 0)
	require.NoError(t, err)
	require.Len(t, es, 1)
	require.Equal(t, []byte("back"), es[0].Data)
	es, err = l.Read(1, 100, 0)
	require.NoError(t, err)
	require.Len(t, es, 10)
}

// A master restarted while its log is broken must not hand out the lost
// sequences again: a replica told about them would otherwise skip new ones.
func TestRestartWhileBrokenNeverReusesSequences(t *testing.T) {
	dir := t.TempDir()
	disk := &diskFull{}
	l, err := replog.Open(dir, replog.Options{WriteAt: disk.writeAt})
	require.NoError(t, err)
	appendN(t, l, 1, 10)
	disk.full.Store(true)
	_, err = l.Append(99, []byte("lost"))
	require.Error(t, err)
	require.NoError(t, l.Close())

	l, err = replog.Open(dir, replog.Options{})
	require.NoError(t, err)
	defer l.Close()
	broken, from := l.Broken()
	require.True(t, broken)
	require.Equal(t, uint64(11), from)

	seq, err := l.Append(100, []byte("after restart"))
	require.NoError(t, err)
	require.Equal(t, uint64(12), seq)

	_, err = l.Read(11, 1, 0)
	var gap *replog.GapError
	require.True(t, errors.As(err, &gap))
	require.Equal(t, replog.GapBroken, gap.Reason)
	require.Equal(t, uint64(12), gap.Next)

	// Recovered and durable: a further restart keeps the hole and the
	// sequence.
	require.NoError(t, l.Close())
	l2, err := replog.Open(dir, replog.Options{})
	require.NoError(t, err)
	defer l2.Close()
	broken, _ = l2.Broken()
	require.False(t, broken)
	require.Equal(t, uint64(12), l2.LastSeq())
	_, err = l2.Read(11, 1, 0)
	require.True(t, errors.As(err, &gap))
	require.Equal(t, uint64(12), gap.Next)
}
