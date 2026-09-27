package connect

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/cmd/connect/session"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func writeConfig(t *testing.T, dir, body string) string {
	t.Helper()
	p := filepath.Join(dir, "mkts.yml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

func requireNoResponseErrors(t *testing.T, resp frontend.MultiServerResponse) {
	t.Helper()
	for _, r := range resp.Responses {
		require.Empty(t, r.Error)
	}
}

// restoreTimezone puts the process-wide timezone back after a test.
func restoreTimezone(t *testing.T) {
	t.Helper()
	prev := utils.InstanceConfig.Timezone
	t.Cleanup(func() { utils.InstanceConfig.Timezone = prev })
}

func TestResolveTimezone(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	t.Run("explicit timezone wins over config", func(t *testing.T) {
		cfg := writeConfig(t, t.TempDir(), "timezone: \"Asia/Tokyo\"\n")
		loc, warned, err := resolveTimezone(cfg, "America/New_York")
		require.NoError(t, err)
		assert.False(t, warned)
		assert.Equal(t, ny.String(), loc.String())
	})

	t.Run("explicit config", func(t *testing.T) {
		cfg := writeConfig(t, t.TempDir(), "root_directory: data\ntimezone: \"America/New_York\"\n")
		loc, warned, err := resolveTimezone(cfg, "")
		require.NoError(t, err)
		assert.False(t, warned)
		assert.Equal(t, ny.String(), loc.String())
	})

	t.Run("mkts.yml in working directory", func(t *testing.T) {
		dir := t.TempDir()
		writeConfig(t, dir, "timezone: \"America/New_York\"\n")
		t.Chdir(dir)
		loc, warned, err := resolveTimezone("", "")
		require.NoError(t, err)
		assert.False(t, warned)
		assert.Equal(t, ny.String(), loc.String())
	})

	t.Run("nothing found falls back to UTC with a warning", func(t *testing.T) {
		t.Chdir(t.TempDir())
		loc, warned, err := resolveTimezone("", "")
		require.NoError(t, err)
		assert.True(t, warned)
		assert.Equal(t, time.UTC, loc)
	})

	t.Run("missing explicit config is an error", func(t *testing.T) {
		_, _, err := resolveTimezone(filepath.Join(t.TempDir(), "nope.yml"), "")
		assert.Error(t, err)
	})

	t.Run("invalid timezone is an error", func(t *testing.T) {
		_, _, err := resolveTimezone("", "Not/AZone")
		assert.Error(t, err)
	})
}

// TestLocalModeDecodesInConfiguredTimezone writes a bar the way a server
// configured for America/New_York does, then opens the directory in local
// mode the way `marketstore connect --config` does. The bar must come back
// with the epoch it was written with. Before --config existed, local mode
// always decoded in UTC and the bar came back 5 hours early.
func TestLocalModeDecodesInConfiguredTimezone(t *testing.T) {
	restoreTimezone(t)
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	dataDir := t.TempDir()
	const key = "TZTEST/1Min/OHLCV"
	// 2026-09-23 09:30 EDT, the regular open.
	written := time.Date(2026, 9, 23, 13, 30, 0, 0, time.UTC).Unix()

	// Write as the server would.
	utils.InstanceConfig.Timezone = ny
	w, err := session.NewLocalAPIClient(dataDir)
	require.NoError(t, err)
	var resp frontend.MultiServerResponse
	require.NoError(t, w.Create(&frontend.MultiCreateRequest{Requests: []frontend.CreateRequest{{
		Key:         key + ":Symbol/Timeframe/AttributeGroup",
		ColumnTypes: []string{"f4", "f4", "f4", "f4", "i8"},
		ColumnNames: []string{"Open", "High", "Low", "Close", "Volume"},
	}}}, &resp))
	requireNoResponseErrors(t, resp)

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{written})
	cs.AddColumn("Open", []float32{341.075})
	cs.AddColumn("High", []float32{341.61})
	cs.AddColumn("Low", []float32{339.852})
	cs.AddColumn("Close", []float32{339.89})
	cs.AddColumn("Volume", []int64{758026})
	nds, err := io.NewNumpyDataset(cs)
	require.NoError(t, err)
	nmds, err := io.NewNumpyMultiDataset(nds, *io.NewTimeBucketKey(key))
	require.NoError(t, err)
	resp = frontend.MultiServerResponse{}
	require.NoError(t, w.Write(&frontend.MultiWriteRequest{Requests: []frontend.WriteRequest{{Data: nmds}}}, &resp))
	requireNoResponseErrors(t, resp)

	// A fresh CLI process starts with the UTC default, then resolves the
	// timezone from the server config before opening the directory.
	utils.InstanceConfig.Timezone = time.UTC
	cfg := writeConfig(t, t.TempDir(), "timezone: \"America/New_York\"\n")
	require.NoError(t, configureTimezone(cfg, ""))

	r, err := session.NewLocalAPIClient(dataDir)
	require.NoError(t, err)
	start := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	csm, err := r.Show(io.NewTimeBucketKey(key), &start, &end)
	require.NoError(t, err)
	got := csm[*io.NewTimeBucketKey(key)]
	require.NotNil(t, got)
	require.Equal(t, 1, got.Len())
	assert.Equal(t, written, got.GetEpoch()[0], "bar decoded at %v, want %v",
		time.Unix(got.GetEpoch()[0], 0).UTC(), time.Unix(written, 0).UTC())
}
