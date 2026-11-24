package repair

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill/rest"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// DefaultTolerance is the relative price difference above which a stored bar
// counts as adjusted. The smallest share change Massive reports is a few
// percent (stock dividends), while REST and flat-file prices of the same raw
// bar differ by rounding only (REST rounds to 4 decimals).
const DefaultTolerance = 0.01

// Bar is the price part of a stored OHLCV bar.
type Bar struct {
	Open, High, Low, Close float64
}

// Differs reports whether any price of b differs from the raw bar by more
// than tol (relative).
func (b Bar) Differs(raw api.AggResult, tol float64) bool {
	off := func(stored, want float64) bool {
		if want == 0 {
			return stored != 0
		}
		return math.Abs(stored-want)/math.Abs(want) > tol
	}
	return off(b.Open, raw.Open) || off(b.High, raw.High) || off(b.Low, raw.Low) || off(b.Close, raw.Close)
}

// Differing returns the raw bars whose stored bar exists and differs by more
// than tol. Raw bars with no stored bar are not returned: the repair fixes
// stored bars, it does not fill gaps.
func Differing(stored map[int64]Bar, raw []api.AggResult, tol float64) []api.AggResult {
	var out []api.AggResult
	for _, r := range raw {
		b, ok := stored[r.EpochMilliseconds/1000]
		if ok && b.Differs(r, tol) {
			out = append(out, r)
		}
	}
	return out
}

// Rewriter is a Fetcher that rewrites only the stored bars that differ from
// the raw bars, so that bars from higher-precision sources stay untouched.
type Rewriter struct {
	Client    *http.Client
	Store     *GRPCStore
	Writer    backfill.Writer
	Limit     int
	Tolerance float64
}

// Fetch implements Fetcher. It returns the number of bars rewritten.
func (rw *Rewriter) Fetch(ctx context.Context, symbol, timeframe string, from, to time.Time) (int, error) {
	raw, err := rest.FetchBarsWindow(ctx, rw.Client, symbol, timeframe, from, to, rw.Limit)
	if err != nil {
		return 0, err
	}
	if len(raw) == 0 {
		return 0, nil
	}
	tbk := symbol + "/" + timeframe + "/OHLCV"
	stored, err := rw.Store.Bars(ctx, tbk, from, to)
	if err != nil {
		return 0, err
	}
	changed := Differing(stored, raw, rw.Tolerance)
	if len(changed) == 0 {
		return 0, nil
	}
	if err := rw.Writer.WriteCSM(barsCSM(tbk, changed), false); err != nil {
		return 0, fmt.Errorf("write %s: %w", tbk, err)
	}
	return len(changed), nil
}

func barsCSM(tbk string, bars []api.AggResult) io.ColumnSeriesMap {
	n := len(bars)
	ep := make([]int64, n)
	o, h, l, c := make([]float32, n), make([]float32, n), make([]float32, n), make([]float32, n)
	v := make([]int64, n)
	for i, b := range bars {
		ep[i] = b.EpochMilliseconds / 1000
		o[i], h[i], l[i], c[i] = float32(b.Open), float32(b.High), float32(b.Low), float32(b.Close)
		v[i] = int64(math.Round(b.Volume))
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", ep)
	cs.AddColumn("Open", o)
	cs.AddColumn("High", h)
	cs.AddColumn("Low", l)
	cs.AddColumn("Close", c)
	cs.AddColumn("Volume", v)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKeyFromString(tbk), cs)
	return csm
}
