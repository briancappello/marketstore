// Package corpaction stores corporate events (splits today, other kinds later)
// per symbol in a provider-neutral format.
//
// Each kind lives in its own fixed-length bucket, <SYMBOL>/1D/<KIND>, so that
// rewriting an event replaces the stored row for that effective date. Every
// kind shares the same columns, and consumers only need PriceFactor and
// VolumeFactor to put pre-event data on the post-event basis.
package corpaction

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// Kind is a category of corporate event. Each kind is stored in its own
// attribute group.
type Kind int

const (
	// Splits covers forward splits, reverse splits and stock dividends.
	Splits Kind = iota + 1
)

// AllKinds lists every supported kind.
var AllKinds = []Kind{Splits}

// AttributeGroup returns the bucket attribute group name for the kind.
func (k Kind) AttributeGroup() string {
	switch k {
	case Splits:
		return "SPLITS"
	default:
		return ""
	}
}

func (k Kind) String() string { return k.AttributeGroup() }

// Subtype is the variant of an event within its kind.
type Subtype byte

const (
	SubtypeUnknown Subtype = iota
	ForwardSplit
	ReverseSplit
	StockDividend
)

// Timeframe is the bucket timeframe used for every event kind.
const Timeframe = "1D"

// Column names shared by every event kind.
const (
	ColPriceFactor  = "PriceFactor"
	ColVolumeFactor = "VolumeFactor"
	ColSplitFrom    = "SplitFrom"
	ColSplitTo      = "SplitTo"
	ColSubtype      = "Subtype"
)

// EffectiveLocation is the timezone in which effective dates start.
var EffectiveLocation = mustLoadLocation("America/New_York")

func mustLoadLocation(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(fmt.Sprintf("corpaction: load location %s: %v", name, err))
	}
	return loc
}

// Event is one corporate event for one symbol.
type Event struct {
	Symbol string
	Kind   Kind
	// Effective is 00:00 America/New_York of the first trading date on the
	// new basis. Data timestamped before it is on the old basis.
	Effective    time.Time
	PriceFactor  float64
	VolumeFactor float64
	SplitFrom    float64
	SplitTo      float64
	Subtype      Subtype
}

// IsNeutral reports whether the event has no effect on adjusted data.
func (e Event) IsNeutral() bool {
	return e.PriceFactor == 1 && e.VolumeFactor == 1
}

// EffectiveDate returns 00:00 America/New_York of the given calendar date.
func EffectiveDate(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, EffectiveLocation)
}

// ParseEffectiveDate parses a YYYY-MM-DD date as 00:00 America/New_York.
func ParseEffectiveDate(s string) (time.Time, error) {
	t, err := time.ParseInLocation("2006-01-02", s, EffectiveLocation)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse effective date %q: %w", s, err)
	}
	return t, nil
}

// NewSplit builds a SPLITS event. from and to are the published ratio (old
// shares, new shares) and must both be positive.
func NewSplit(symbol string, effective time.Time, from, to float64, subtype Subtype) (Event, error) {
	if symbol == "" {
		return Event{}, fmt.Errorf("split: empty symbol")
	}
	if !(from > 0) || !(to > 0) || math.IsInf(from, 0) || math.IsInf(to, 0) {
		return Event{}, fmt.Errorf("split %s: ratio %v:%v must be positive", symbol, from, to)
	}
	if subtype == SubtypeUnknown {
		return Event{}, fmt.Errorf("split %s: unknown subtype", symbol)
	}
	y, m, d := effective.In(EffectiveLocation).Date()
	factor := to / from
	return Event{
		Symbol:       symbol,
		Kind:         Splits,
		Effective:    EffectiveDate(y, m, d),
		PriceFactor:  factor,
		VolumeFactor: factor,
		SplitFrom:    from,
		SplitTo:      to,
		Subtype:      subtype,
	}, nil
}

// TimeBucketKey returns the bucket key that stores a symbol's events of a kind.
func TimeBucketKey(symbol string, kind Kind) *io.TimeBucketKey {
	return io.NewTimeBucketKeyFromString(symbol + "/" + Timeframe + "/" + kind.AttributeGroup())
}

type bucketID struct {
	symbol string
	kind   Kind
}

// EncodeCSM converts events into one column series per bucket, sorted by
// effective date. Events of the same symbol, kind and effective date are
// merged into one row whose factors are the product of theirs.
func EncodeCSM(events []Event) (io.ColumnSeriesMap, error) {
	grouped := map[bucketID]map[int64]Event{}
	for _, e := range events {
		if e.Kind.AttributeGroup() == "" {
			return nil, fmt.Errorf("event %s: unknown kind %d", e.Symbol, e.Kind)
		}
		if e.Symbol == "" {
			return nil, fmt.Errorf("event: empty symbol")
		}
		y, m, d := e.Effective.In(EffectiveLocation).Date()
		e.Effective = EffectiveDate(y, m, d)
		id := bucketID{symbol: e.Symbol, kind: e.Kind}
		byDay, ok := grouped[id]
		if !ok {
			byDay = map[int64]Event{}
			grouped[id] = byDay
		}
		epoch := e.Effective.Unix()
		if prev, dup := byDay[epoch]; dup {
			e = merge(prev, e)
		}
		byDay[epoch] = e
	}

	csm := io.NewColumnSeriesMap()
	for id, byDay := range grouped {
		epochs := make([]int64, 0, len(byDay))
		for epoch := range byDay {
			epochs = append(epochs, epoch)
		}
		sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })

		n := len(epochs)
		priceFactors := make([]float64, n)
		volumeFactors := make([]float64, n)
		froms := make([]float64, n)
		tos := make([]float64, n)
		subtypes := make([]byte, n)
		for i, epoch := range epochs {
			e := byDay[epoch]
			priceFactors[i] = e.PriceFactor
			volumeFactors[i] = e.VolumeFactor
			froms[i] = e.SplitFrom
			tos[i] = e.SplitTo
			subtypes[i] = byte(e.Subtype)
		}
		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", epochs)
		cs.AddColumn(ColPriceFactor, priceFactors)
		cs.AddColumn(ColVolumeFactor, volumeFactors)
		cs.AddColumn(ColSplitFrom, froms)
		cs.AddColumn(ColSplitTo, tos)
		cs.AddColumn(ColSubtype, subtypes)
		csm.AddColumnSeries(*TimeBucketKey(id.symbol, id.kind), cs)
	}
	return csm, nil
}

// merge combines two events with the same effective date. The factors
// multiply; the published ratio and subtype of the first event are kept
// unless it was neutral.
func merge(a, b Event) Event {
	if a.IsNeutral() {
		return b
	}
	if b.IsNeutral() {
		return a
	}
	out := a
	out.PriceFactor = a.PriceFactor * b.PriceFactor
	out.VolumeFactor = a.VolumeFactor * b.VolumeFactor
	out.SplitFrom = a.SplitFrom * b.SplitFrom
	out.SplitTo = a.SplitTo * b.SplitTo
	return out
}

// Neutralize returns a factor-1.0 row for every stored, non-neutral event
// whose (symbol, kind, effective date) is absent from fresh. Writing the
// result next to fresh removes withdrawn events without deleting rows.
func Neutralize(stored, fresh []Event) []Event {
	present := map[bucketID]map[int64]struct{}{}
	for _, e := range fresh {
		id := bucketID{symbol: e.Symbol, kind: e.Kind}
		if present[id] == nil {
			present[id] = map[int64]struct{}{}
		}
		present[id][dayEpoch(e.Effective)] = struct{}{}
	}
	var out []Event
	for _, e := range stored {
		if e.IsNeutral() {
			continue
		}
		if _, ok := present[bucketID{symbol: e.Symbol, kind: e.Kind}][dayEpoch(e.Effective)]; ok {
			continue
		}
		out = append(out, Event{
			Symbol:       e.Symbol,
			Kind:         e.Kind,
			Effective:    e.Effective,
			PriceFactor:  1,
			VolumeFactor: 1,
			Subtype:      e.Subtype,
		})
	}
	return out
}

func dayEpoch(t time.Time) int64 {
	y, m, d := t.In(EffectiveLocation).Date()
	return EffectiveDate(y, m, d).Unix()
}
