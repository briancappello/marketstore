package adjust

import (
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/corpaction"
	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

const (
	calcSplit       = "split"
	calcDividend    = "dividend"
	roundToDecimals = 4
	decimal         = 10
)

var (
	requiredColumns []io.DataShape

	optionalColumns []io.DataShape

	initArgs []io.DataShape

	// RounderNum sets the precision of adjusted float prices (4 decimals).
	RounderNum = math.Pow(decimal, roundToDecimals)
)

// priceColumns are divided by the cumulative price factor.
var priceColumns = map[string]struct{}{
	"Open": {}, "High": {}, "Low": {}, "Close": {}, "VWAP": {},
	"Price": {}, "BidPrice": {}, "AskPrice": {},
}

// sizeColumns are multiplied by the cumulative volume factor.
var sizeColumns = map[string]struct{}{
	"Volume": {}, "Size": {}, "BidSize": {}, "AskSize": {},
}

// Adjust puts unadjusted stored data on the current share basis, using the
// events in the corporate-event store (see package corpaction).
type Adjust struct {
	uda.AggInterface

	AdjustDividend bool
	AdjustSplit    bool

	CatalogDir *catalog.Directory
	// Now returns the query time. Events effective after it are ignored.
	// Defaults to time.Now.
	Now func() time.Time

	output *io.ColumnSeries
}

func (adj *Adjust) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}

func (adj *Adjust) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}

func (adj *Adjust) GetInitArgs() []io.DataShape {
	return initArgs
}

func (adj *Adjust) New(_ *functions.ArgumentMap, args ...interface{}) (uda.AggInterface, error) {
	rn := &Adjust{
		CatalogDir: adj.CatalogDir,
		Now:        adj.Now,
	}

	err := rn.init(args...)

	return rn, err
}

func (adj *Adjust) init(args ...interface{}) error {
	if len(args) == 0 {
		adj.AdjustSplit = true
		adj.AdjustDividend = true
		return nil
	}
	adj.AdjustSplit = false
	adj.AdjustDividend = false
	for _, arg := range args {
		switch _arg := arg.(type) {
		case []string:
			for _, p := range _arg {
				adj.setArg(p)
			}
		case string:
			adj.setArg(_arg)
		}
	}
	return nil
}

func (adj *Adjust) setArg(p string) {
	switch strings.ToLower(p) {
	case calcSplit:
		adj.AdjustSplit = true
	case calcDividend:
		adj.AdjustDividend = true
	}
}

// kinds returns the event kinds the arguments select. Dividends have no
// event kind yet, so they select nothing.
func (adj *Adjust) kinds() []corpaction.Kind {
	var kinds []corpaction.Kind
	if adj.AdjustSplit {
		kinds = append(kinds, corpaction.Splits)
	}
	return kinds
}

func (adj *Adjust) Accum(tbk io.TimeBucketKey, _ *functions.ArgumentMap, cols io.ColumnInterface,
) (*io.ColumnSeries, error) {
	epochs, ok := cols.GetColumn("Epoch").([]int64)
	if !ok {
		return nil, errors.New("adjust: Input data must have an Epoch column")
	}

	shapes := cols.GetDataShapes()
	out := io.NewColumnSeries()
	adj.output = out

	var events []corpaction.Event
	if kinds := adj.kinds(); len(kinds) > 0 && adj.CatalogDir != nil {
		var err error
		events, err = cachedEvents(adj.CatalogDir, tbk.GetItemInCategory("Symbol"), kinds)
		if err != nil {
			return nil, err
		}
	}
	events = effectiveBy(events, adj.now())

	if len(events) == 0 {
		for _, ds := range shapes {
			out.AddColumn(ds.Name, cols.GetColumn(ds.Name))
		}
		return out, nil
	}

	priceFactors, volumeFactors := rowFactors(epochs, events)
	for _, ds := range shapes {
		col := cols.GetColumn(ds.Name)
		if _, isPrice := priceColumns[ds.Name]; isPrice {
			col = scale(col, priceFactors, true)
		} else if _, isSize := sizeColumns[ds.Name]; isSize {
			col = scale(col, volumeFactors, false)
		}
		out.AddColumn(ds.Name, col)
	}
	return out, nil
}

func (adj *Adjust) Output() *io.ColumnSeries {
	if adj.output == nil {
		return io.NewColumnSeries()
	}
	return adj.output
}

func (adj *Adjust) now() time.Time {
	if adj.Now != nil {
		return adj.Now()
	}
	return time.Now()
}

// effectiveBy drops events that have not taken effect at now.
func effectiveBy(events []corpaction.Event, now time.Time) []corpaction.Event {
	out := make([]corpaction.Event, 0, len(events))
	for _, e := range events {
		if !e.Effective.After(now) {
			out = append(out, e)
		}
	}
	return out
}

// rowFactors returns, for each row, the product of the factors of every event
// that takes effect after the row's timestamp. events must be sorted by
// effective time.
func rowFactors(epochs []int64, events []corpaction.Event) (price, volume []float64) {
	n := len(events)
	pSuffix := make([]float64, n+1)
	vSuffix := make([]float64, n+1)
	pSuffix[n], vSuffix[n] = 1, 1
	for i := n - 1; i >= 0; i-- {
		pSuffix[i] = pSuffix[i+1] * events[i].PriceFactor
		vSuffix[i] = vSuffix[i+1] * events[i].VolumeFactor
	}
	price = make([]float64, len(epochs))
	volume = make([]float64, len(epochs))
	for r, epoch := range epochs {
		idx := sort.Search(n, func(i int) bool { return events[i].Effective.Unix() > epoch })
		price[r], volume[r] = pSuffix[idx], vSuffix[idx]
	}
	return price, volume
}

// scale returns a copy of col with each value divided (prices) or multiplied
// (sizes) by its row factor. Unsupported column types are returned unchanged.
func scale(col interface{}, factors []float64, divide bool) interface{} {
	apply := func(v float64, f float64) float64 {
		if divide {
			return v / f
		}
		return v * f
	}
	switch c := col.(type) {
	case []float32:
		out := make([]float32, len(c))
		for i, v := range c {
			x := apply(float64(v), factors[i])
			if divide {
				x = roundPrice(x)
			}
			out[i] = float32(x)
		}
		return out
	case []float64:
		out := make([]float64, len(c))
		for i, v := range c {
			x := apply(v, factors[i])
			if divide {
				x = roundPrice(x)
			}
			out[i] = x
		}
		return out
	case []int64:
		out := make([]int64, len(c))
		for i, v := range c {
			out[i] = int64(math.Round(apply(float64(v), factors[i])))
		}
		return out
	case []uint64:
		out := make([]uint64, len(c))
		for i, v := range c {
			out[i] = uint64(math.Round(apply(float64(v), factors[i])))
		}
		return out
	default:
		return col
	}
}

func roundPrice(x float64) float64 {
	return math.Round(x*RounderNum) / RounderNum
}
