package start

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
)

type fakeSource struct {
	res bgworker.WatchlistResult
	err error
	q   bgworker.WatchlistQuery
}

func (f *fakeSource) ListWatchlistNames() []string                                      { return nil }
func (f *fakeSource) GetWatchlistRanking(string) []bgworker.WatchlistRankingEntry       { return nil }
func (f *fakeSource) AllWatchlistRankings() map[string][]bgworker.WatchlistRankingEntry { return nil }
func (f *fakeSource) Rankings(q bgworker.WatchlistQuery) (bgworker.WatchlistResult, error) {
	f.q = q
	return f.res, f.err
}

// The adapter translates the plugin's error classes into the frontend's,
// and passes queries and window metadata through unchanged.
func TestWatchlistAdapterRankings(t *testing.T) {
	start := time.Unix(1_790_000_000, 0)
	src := &fakeSource{res: bgworker.WatchlistResult{Lists: []bgworker.WatchlistList{{
		Name: "PCT", Basis: "session", Session: "regular", TradingDate: "2026-09-21",
		WindowStart: start, WindowEnd: start.Add(time.Hour), Complete: true,
		Entries: []bgworker.WatchlistRankingEntry{{Symbol: "AAPL", Rank: 1,
			Fields: []bgworker.WatchlistRankingField{{Key: "pct_change", Value: 1.5}}}},
	}}}}
	a := &watchlistAdapter{src: src}

	res, err := a.Rankings(frontend.WatchlistQuery{Names: []string{"PCT"}, Session: "regular", AsOf: "2026-09-21"})
	require.NoError(t, err)
	assert.Equal(t, bgworker.WatchlistQuery{Names: []string{"PCT"}, Session: "regular", AsOf: "2026-09-21"}, src.q)
	require.Len(t, res.Lists, 1)
	l := res.Lists[0]
	assert.Equal(t, "session", l.Basis)
	assert.True(t, l.WindowEnd.Equal(start.Add(time.Hour)))
	assert.True(t, l.Complete)
	assert.Equal(t, 1.5, l.Entries[0].Fields[0].Value)

	src.err = fmt.Errorf("%w: GAP_UP", bgworker.ErrWatchlistNotFound)
	_, err = a.Rankings(frontend.WatchlistQuery{})
	assert.ErrorIs(t, err, frontend.ErrWatchlistNotFound)
	assert.Contains(t, err.Error(), "GAP_UP")

	src.err = fmt.Errorf("%w: not a trading day", bgworker.ErrWatchlistInvalid)
	_, err = a.Rankings(frontend.WatchlistQuery{})
	assert.ErrorIs(t, err, frontend.ErrWatchlistInvalid)

	src.err = errors.New("disk on fire")
	_, err = a.Rankings(frontend.WatchlistQuery{})
	assert.False(t, errors.Is(err, frontend.ErrWatchlistInvalid) || errors.Is(err, frontend.ErrWatchlistNotFound))
}
