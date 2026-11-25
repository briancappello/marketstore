package executor_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var tickDSV = io.NewDataShapeVector([]string{"Price", "Size"}, []io.EnumElementType{io.FLOAT32, io.FLOAT32})

func addTickBucket(t *testing.T, rootDir string, metadata *executor.InstanceMetadata, key string) {
	t.Helper()
	tbk := io.NewTimeBucketKey(key)
	tbi := io.NewTimeBucketInfo(*utils.TimeframeFromString("1Sec"), tbk.GetPathToYearFiles(rootDir),
		"Test", int16(2026), tickDSV, io.VARIABLE)
	require.NoError(t, metadata.CatalogDir.AddTimeBucket(tbk, tbi))
}

// writeTicks queues n ticks for key at second ts, each with its own price.
func writeTicks(t *testing.T, metadata *executor.InstanceMetadata, key string, ts time.Time, n int, price0 float32) {
	t.Helper()
	tbi, err := metadata.CatalogDir.GetLatestTimeBucketInfoFromKey(io.NewTimeBucketKey(key))
	require.NoError(t, err)
	w, err := executor.NewWriter(metadata.CatalogDir, metadata.WALFile)
	require.NoError(t, err)
	type row struct {
		Epoch       int64
		Price, Size float32
	}
	var buf []byte
	times := make([]time.Time, n)
	for i := 0; i < n; i++ {
		times[i] = ts.Add(time.Duration(i) * time.Millisecond)
		buf, _ = io.Serialize(buf, row{times[i].Unix(), price0 + float32(i), 1})
	}
	require.NoError(t, w.WriteRecords(times, buf, tickDSV, tbi))
}

func readPrices(t *testing.T, metadata *executor.InstanceMetadata, key string, from time.Time) []float32 {
	t.Helper()
	q := planner.NewQuery(metadata.CatalogDir)
	q.AddTargetKey(io.NewTimeBucketKey(key))
	q.SetRange(from.Add(-time.Hour), from.Add(time.Hour))
	parsed, err := q.Parse()
	require.NoError(t, err)
	r, err := executor.NewReader(parsed)
	require.NoError(t, err)
	csm, err := r.Read()
	require.NoError(t, err)
	for _, cs := range csm {
		p, ok := cs.GetColumn("Price").([]float32)
		require.True(t, ok)
		return p
	}
	return nil
}

func yearFile(rootDir, key string) string {
	return filepath.Join(rootDir, key, "2026.bin")
}

// replayWALSnapshot replays walBytes as a WAL file left behind by a crashed
// process.
func replayWALSnapshot(t *testing.T, rootDir string, walBytes []byte) {
	t.Helper()
	walBytes = append([]byte(nil), walBytes...)
	for i := 0; i < 8; i++ { // a different owning instance, so we may take it over
		walBytes[3+i] = 1
	}
	path := filepath.Join(rootDir, "CrashedWAL")
	require.NoError(t, os.WriteFile(path, walBytes, 0o600))
	wf, err := executor.TakeOverWALFile(path)
	require.NoError(t, err)
	require.NoError(t, wf.Replay(false))
}

// TestWALReplayVariableWritesAreIdempotent kills the process (by snapshotting
// the WAL and the data files) before, in the middle of and after a transaction
// group's primary writes, and checks that WAL replay leaves every tick exactly
// once. Before replay was made idempotent, the "after" case doubled every tick
// written since the last checkpoint.
func TestWALReplayVariableWritesAreIdempotent(t *testing.T) {
	const (
		aapl = "AAPL/1Sec/TRADE"
		msft = "MSFT/1Sec/TRADE"
	)
	t0 := time.Date(2026, 11, 24, 15, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)

	for _, crash := range []string{"before primary", "mid primary", "after primary"} {
		t.Run(crash, func(t *testing.T) {
			rootDir, _, metadata := setup(t)
			addTickBucket(t, rootDir, metadata, aapl)
			addTickBucket(t, rootDir, metadata, msft)

			// Checkpointed history, so the crashed group continues existing
			// blocks (t0) as well as starting new ones (t1).
			writeTicks(t, metadata, aapl, t0, 10, 0)
			writeTicks(t, metadata, msft, t0, 10, 0)
			require.NoError(t, metadata.WALFile.FlushToWAL())
			require.NoError(t, metadata.WALFile.CreateCheckpoint())

			pre := map[string][]byte{}
			for _, k := range []string{aapl, msft} {
				b, err := os.ReadFile(yearFile(rootDir, k))
				require.NoError(t, err)
				pre[k] = b
			}

			// The group that is in flight when the process dies: two write
			// requests to the same AAPL second, plus new seconds.
			writeTicks(t, metadata, aapl, t0, 60, 100)
			writeTicks(t, metadata, aapl, t0, 40, 200)
			writeTicks(t, metadata, aapl, t1, 50, 300)
			writeTicks(t, metadata, msft, t0, 100, 100)
			writeTicks(t, metadata, msft, t1, 50, 300)
			require.NoError(t, metadata.WALFile.FlushToWAL())

			want := map[string][]float32{
				aapl: readPrices(t, metadata, aapl, t0),
				msft: readPrices(t, metadata, msft, t0),
			}
			require.Len(t, want[aapl], 160)
			require.Len(t, want[msft], 160)

			fi, err := metadata.WALFile.FilePtr.Stat()
			require.NoError(t, err)
			walBytes := make([]byte, fi.Size())
			_, err = metadata.WALFile.FilePtr.ReadAt(walBytes, 0)
			require.NoError(t, err)

			switch crash {
			case "before primary":
				for k, b := range pre {
					require.NoError(t, os.WriteFile(yearFile(rootDir, k), b, 0o600))
				}
			case "mid primary": // AAPL reached primary, MSFT did not
				require.NoError(t, os.WriteFile(yearFile(rootDir, msft), pre[msft], 0o600))
			}

			replayWALSnapshot(t, rootDir, walBytes)

			require.Equal(t, want[aapl], readPrices(t, metadata, aapl, t0))
			require.Equal(t, want[msft], readPrices(t, metadata, msft, t0))
		})
	}
}

// TestSameSlotVariableWritesMatchSeparateFlushes checks that writes to the
// same variable block committed in one transaction group (which the WAL merges
// into one write) read back exactly as when each is flushed on its own.
func TestSameSlotVariableWritesMatchSeparateFlushes(t *testing.T) {
	rootDir, _, metadata := setup(t)
	const together, apart = "AAA/1Sec/TRADE", "BBB/1Sec/TRADE"
	addTickBucket(t, rootDir, metadata, together)
	addTickBucket(t, rootDir, metadata, apart)
	t0 := time.Date(2026, 11, 24, 15, 0, 0, 0, time.UTC)

	writeTicks(t, metadata, together, t0, 5, 0)
	writeTicks(t, metadata, apart, t0, 5, 0)
	require.NoError(t, metadata.WALFile.FlushToWAL())

	// The new rows share timestamps with existing ones, so the on-disk sort
	// by interval ticks has to interleave them.
	writeTicks(t, metadata, together, t0, 7, 100)
	writeTicks(t, metadata, together, t0.Add(time.Millisecond), 3, 200)
	require.NoError(t, metadata.WALFile.FlushToWAL())

	writeTicks(t, metadata, apart, t0, 7, 100)
	require.NoError(t, metadata.WALFile.FlushToWAL())
	writeTicks(t, metadata, apart, t0.Add(time.Millisecond), 3, 200)
	require.NoError(t, metadata.WALFile.FlushToWAL())

	got := readPrices(t, metadata, together, t0)
	require.Len(t, got, 15)
	require.Equal(t, readPrices(t, metadata, apart, t0), got)
}
