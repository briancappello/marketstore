package sessionfacts

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var ny, _ = time.LoadLocation("America/New_York")

func et(y int, m time.Month, d, hh, mm int) time.Time { return time.Date(y, m, d, hh, mm, 0, 0, ny) }

// startServer stands in for the running server: an America/New_York
// instance with a WAL, over root.
func startServer(t *testing.T, root string) {
	t.Helper()
	cfg := utils.NewDefaultConfig(root)
	cfg.BackgroundSync = false
	cfg.Timezone = ny
	utils.InstanceConfig = *cfg
	c := di.NewContainer(cfg)
	walFile, err := executor.NewWALFile(c.GetAbsRootDir(), c.GetInitInstanceID(),
		&executor.NopReplicationSender{}, false, &sync.WaitGroup{},
		executor.StartNewTriggerPluginDispatcherWithWorkers(nil, 1), executor.NewTransactionPipe())
	require.NoError(t, err)
	executor.NewInstanceSetup(c.GetCatalogDir(), walFile)
}

func writeMinute(t *testing.T, sym string, at time.Time, price float32, vol int64) {
	t.Helper()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{at.Unix()})
	for _, c := range []string{"Open", "High", "Low", "Close"} {
		cs.AddColumn(c, []float32{price})
	}
	cs.AddColumn("Volume", []int64{vol})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKey(sym + "/1Min/OHLCV"), cs)
	require.NoError(t, executor.WriteCSM(csm, false))
}

func readRows(t *testing.T, sym string) []sessionfacts.Row {
	t.Helper()
	rows, err := sessionfacts.Read(executor.ThisInstance.CatalogDir, sym, et(2026, 9, 1, 0, 0), et(2026, 9, 30, 0, 0))
	require.NoError(t, err)
	return rows
}

func hashFacts(t *testing.T, root string) [32]byte {
	t.Helper()
	h := sha256.New()
	for _, sym := range []string{"AA", "BB"} {
		b, err := os.ReadFile(filepath.Join(root, sym, "1D", "SESSIONS", "2026.bin"))
		require.NoError(t, err)
		h.Write(b)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// R2 + R5: the offline rebuild writes the same rows as the live job, and a
// second run changes nothing.
func TestOfflineRebuildMatchesLiveAndIsIdempotent(t *testing.T) {
	root := t.TempDir()
	startServer(t, root)
	writeMinute(t, "AA", et(2026, 9, 21, 8, 0), 10, 100)
	writeMinute(t, "AA", et(2026, 9, 21, 12, 0), 11, 2_000)
	writeMinute(t, "AA", et(2026, 9, 22, 17, 0), 12, 30)
	writeMinute(t, "BB", et(2026, 9, 22, 10, 0), 50, 7)

	// The live job, as the leader runs it after each day.
	live, err := sessionfacts.NewService(sessionfacts.Config{
		Write: executor.WriteCSM, StateDir: sessionfacts.StateDirFor(root),
		Symbols: func() []string { return []string{"AA", "BB"} }, SafetyDays: -1,
	})
	require.NoError(t, err)
	for _, d := range []int{21, 22} {
		ds, err2 := calendar.Nasdaq.SessionBounds(2026, 9, d)
		require.NoError(t, err2)
		require.NoError(t, live.RunDaily(ds))
	}
	liveAA, liveBB := readRows(t, "AA"), readRows(t, "BB")
	require.Len(t, liveAA, 2)
	liveHash := hashFacts(t, root)

	// Delete the facts and rebuild them offline.
	for _, sym := range []string{"AA", "BB"} {
		require.NoError(t, os.RemoveAll(filepath.Join(root, sym, "1D")))
	}
	o := options{configPath: filepath.Join(t.TempDir(), "absent.yml"), dir: root,
		timezone: "America/New_York", from: "2026-09-21", to: "2026-09-22"}
	require.NoError(t, runOffline(o))
	assert.Equal(t, liveAA, readRows(t, "AA"), "rebuilt rows equal the live rows")
	assert.Equal(t, liveBB, readRows(t, "BB"))
	assert.Equal(t, liveHash, hashFacts(t, root), "byte-identical files")

	require.NoError(t, runOffline(o))
	assert.Equal(t, liveHash, hashFacts(t, root), "a second run changes nothing")
}

func TestOfflineRebuildRefusesReplicaConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "mkts.yml")
	require.NoError(t, os.WriteFile(cfg, []byte(
		"root_directory: data\ntimezone: America/New_York\nreplication:\n  master_host: \"taichi:5996\"\n"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "data"), 0o755))
	err := runOffline(options{configPath: cfg, from: "2026-09-21", to: "2026-09-22"})
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "replica"), err.Error())
}

func TestResolveRelativeRootAndOverrides(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "mkts.yml")
	require.NoError(t, os.WriteFile(cfg, []byte("root_directory: data\ntimezone: America/New_York\n"), 0o600))

	r, err := resolve(options{configPath: cfg})
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "data"), r.root)
	assert.Equal(t, "America/New_York", r.tz.String())
	assert.False(t, r.replica)

	r, err = resolve(options{configPath: cfg, dir: "/x", timezone: "UTC"})
	require.NoError(t, err)
	assert.Equal(t, "/x", r.root)
	assert.Equal(t, "UTC", r.tz.String())
}
