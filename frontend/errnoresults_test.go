package frontend_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// TestExecuteQueryNoDataIsMatchableSentinel is the load-bearing test for the
// ErrNoResults sentinel.
//
// "This bucket holds nothing" and "this read failed" are different answers, and
// two callers depend on telling them apart: REST maps the former to a 404 or an
// empty list, and the replication backfill treats it as an empty local bucket
// so its row diff can classify every master row as missing.
//
// Before the sentinel, both callers had to string-match the message. The
// replica's reconciler did not, so an empty bucket was misread as a failed read
// and the whole lookback window was rewritten blind -- for precisely the
// buckets where comparison mattered most.
//
// If ExecuteQuery ever stops wrapping ErrNoResults, this fails here rather than
// silently degrading the replica back to blind writes.
func TestExecuteQueryNoDataIsMatchableSentinel(t *testing.T) {
	_, _, _, q := setup(t)

	// A symbol that does not exist in the dummy catalog.
	_, err := q.ExecuteQuery(
		io.NewTimeBucketKey("NOSUCHSYMBOL/1Min/OHLCV"),
		time.Unix(1, 0).UTC(), time.Now().UTC(),
		0, false, nil,
	)

	require.Error(t, err, "querying an absent bucket must report something")
	assert.True(t, errors.Is(err, frontend.ErrNoResults),
		"a no-data result must be matchable with errors.Is, not only by message text; got %v", err)
}

// TestErrNoResultsSurvivesWrapping guards the property callers actually rely
// on: the sentinel stays matchable through additional context wrapping, so
// intermediate layers may add detail without breaking classification.
func TestErrNoResultsSurvivesWrapping(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("reading local window for AAPL/1Sec/OHLCV: %w",
		fmt.Errorf("%w: Target: AAPL/1Sec/OHLCV", frontend.ErrNoResults))

	assert.True(t, errors.Is(wrapped, frontend.ErrNoResults))
	assert.False(t, errors.Is(errors.New("some other failure"), frontend.ErrNoResults),
		"an unrelated error must not be mistaken for a no-data result")
}

// TestErrNoResultsMessageUnchanged pins the rendered text. Operators grep for
// this string in logs, and an older client matches on it, so the move to a
// sentinel must not have altered what it reads like.
func TestErrNoResultsMessageUnchanged(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("%w: Target: %v, start, end: %v,%v limitRecordCount: %v",
		frontend.ErrNoResults, "AAPL/1Sec/OHLCV", 1, 2, 0)

	assert.Contains(t, err.Error(), "no results returned from query: Target: AAPL/1Sec/OHLCV")
}
