package executor_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
)

// TestCachedFP_FailedOpenDoesNotPoisonTheCache is the regression test for a
// crash seen on a live replica during WAL replay.
//
// A failed open used to leave the cache describing a file it no longer held:
// the previous handle had been closed and the field overwritten with nil, but
// the cached NAME was left untouched because the assignment that updates it
// comes after the error return. The next request for that name matched on name
// alone and handed back a nil *os.File together with a nil error. Every method
// on a nil *os.File reports os.ErrInvalid, so the failure surfaced much later
// as "invalid argument" from a write, with nothing pointing back to here.
//
// This was latent for as long as the first failed open aborted the whole
// replay. Once replay learned to skip a deleted bucket and carry on, the
// poisoned cache became reachable and took the process down with a panic.
func TestCachedFP_FailedOpenDoesNotPoisonTheCache(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	existing := filepath.Join(dir, "existing.bin")
	require.Nil(t, os.WriteFile(existing, make([]byte, 64), 0o600))
	missing := filepath.Join(dir, "missing.bin")

	cfp := executor.NewCachedFP()
	t.Cleanup(func() { _ = cfp.Close() })

	// Populate the cache with a good handle.
	fp, err := cfp.GetFP(existing)
	require.Nil(t, err)
	require.NotNil(t, fp)

	// Ask for a file that is not there. This must fail...
	_, err = cfp.GetFP(missing)
	require.Error(t, err)

	// ...and must not have left the cache claiming to hold `existing`.
	fp, err = cfp.GetFP(existing)
	require.Nil(t, err, "a previous failed open must not affect a later valid one")
	require.NotNil(t, fp, "a nil handle with a nil error is the bug this guards")

	// The handle must actually be usable, not merely non-nil.
	_, err = fp.WriteAt([]byte{1, 2, 3, 4}, 0)
	assert.Nil(t, err, "the cached handle must be live, not a closed or nil file")
}

// TestCachedFP_RepeatedFailuresAreStable covers the replay shape directly:
// several missing files interleaved with good ones, which is what a pass over a
// WAL holding writes for deleted buckets looks like.
func TestCachedFP_RepeatedFailuresAreStable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	good := make([]string, 3)
	for i := range good {
		good[i] = filepath.Join(dir, string(rune('a'+i))+".bin")
		require.Nil(t, os.WriteFile(good[i], make([]byte, 64), 0o600))
	}
	missing := filepath.Join(dir, "gone.bin")

	cfp := executor.NewCachedFP()
	t.Cleanup(func() { _ = cfp.Close() })

	for round := 0; round < 3; round++ {
		for _, path := range good {
			fp, err := cfp.GetFP(path)
			require.Nil(t, err, "round %d: %s", round, path)
			require.NotNil(t, fp, "round %d: %s", round, path)
			_, err = fp.WriteAt([]byte{byte(round)}, 0)
			require.Nil(t, err, "round %d: %s must be writable", round, path)

			_, err = cfp.GetFP(missing)
			require.Error(t, err, "round %d: the missing file must keep failing", round)
		}
	}
}

// TestCachedFP_CloseIsIdempotent guards against a double close after the
// cache-clearing change.
func TestCachedFP_CloseIsIdempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	require.Nil(t, os.WriteFile(path, make([]byte, 8), 0o600))

	cfp := executor.NewCachedFP()
	_, err := cfp.GetFP(path)
	require.Nil(t, err)

	assert.Nil(t, cfp.Close())
	assert.Nil(t, cfp.Close(), "a second Close must be a no-op, not a double close")
}
