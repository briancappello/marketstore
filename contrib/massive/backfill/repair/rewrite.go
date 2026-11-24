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
	"github.com/alpacahq/marketstore/v4/corpaction"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// DefaultTolerance is the relative error allowed when matching a stored
// price against a split-adjusted raw price. REST and flat-file prices of the
// same raw bar differ by rounding only (REST rounds to 4 decimals).
const DefaultTolerance = 0.01

// Bar is the price part of a stored OHLCV bar.
type Bar struct {
	Open, High, Low, Close float64
}

// AdjustmentFactors returns the factors a bar at t could have been adjusted
// by: the cumulative products of the price factors of the splits after t, in
// date order. Massive adjusts for every split up to the time it serves the
// bar, so a stored adjusted bar matches one of these products.
func AdjustmentFactors(splits []corpaction.Event, t time.Time) []float64 {
	var out []float64
	f := 1.0
	for _, e := range splits {
		if e.Effective.After(t) {
			f *= e.PriceFactor
			out = append(out, f)
		}
	}
	return out
}

// IsAdjusted reports whether the stored bar equals the raw bar divided by one
// of the factors, within tol, and does not already equal the raw bar.
func (b Bar) IsAdjusted(raw api.AggResult, factors []float64, tol float64) bool {
	near := func(got, want float64) bool {
		if want == 0 {
			return got == 0
		}
		return math.Abs(got-want)/math.Abs(want) <= tol
	}
	if raw.Close <= 0 || near(b.Close, raw.Close) {
		return false
	}
	for _, f := range factors {
		if near(b.Close, raw.Close/f) && near(b.Open, raw.Open/f) {
			return true
		}
	}
	return false
}

// Adjusted returns the raw bars whose stored bar is a split-adjusted copy of
// them. splits must be the symbol's splits that have taken effect, sorted by
// date. Raw bars with no stored bar are not returned: the repair fixes stored
// bars, it does not fill gaps.
func Adjusted(stored map[int64]Bar, raw []api.AggResult, splits []corpaction.Event, tol float64) []api.AggResult {
	var out []api.AggResult
	for _, r := range raw {
		epoch := r.EpochMilliseconds / 1000
		b, ok := stored[epoch]
		if !ok {
			continue
		}
		if b.IsAdjusted(r, AdjustmentFactors(splits, time.Unix(epoch, 0)), tol) {
			out = append(out, r)
		}
	}
	return out
}

// Rewriter is a Fetcher that rewrites only the stored bars that are
// split-adjusted copies of the raw bars. Raw bars stay untouched, so bars from
// higher-precision sources (flat files) are kept as stored.
type Rewriter struct {
	Client    *http.Client
	Store     *GRPCStore
	Writer    backfill.Writer
	Limit     int
	Tolerance float64
	// Splits holds each symbol's splits that have taken effect, by date.
	Splits map[string][]corpaction.Event
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
	changed := Adjusted(stored, raw, rw.Splits[symbol], rw.Tolerance)
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
