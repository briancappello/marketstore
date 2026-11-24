// Package splitsync turns Massive split results into corporate-event store
// writes for the symbols MarketStore tracks.
package splitsync

import (
	"fmt"
	"sort"

	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/corpaction"
)

// Skipped is a Massive result that could not be mapped to an event.
type Skipped struct {
	Split  api.Split
	Reason string
}

// Plan is the outcome of mapping one full split history.
type Plan struct {
	// Events holds every event to write, grouped by symbol in symbol order:
	// the fresh events plus a neutralized row for every stored event that
	// Massive no longer returns.
	Events      []corpaction.Event
	Written     int
	Neutralized int
	// Unchanged counts symbols whose stored events already match.
	Unchanged int
	Skipped   []Skipped
	// Symbols lists the symbols that have at least one event to write.
	Symbols []string
}

// Subtype maps a Massive adjustment_type to an event subtype.
func Subtype(adjustmentType string) (corpaction.Subtype, bool) {
	switch adjustmentType {
	case "forward_split":
		return corpaction.ForwardSplit, true
	case "reverse_split":
		return corpaction.ReverseSplit, true
	case "stock_dividend":
		return corpaction.StockDividend, true
	default:
		return corpaction.SubtypeUnknown, false
	}
}

// ToEvent maps one Massive split result to a SPLITS event.
func ToEvent(s api.Split) (corpaction.Event, error) {
	subtype, ok := Subtype(s.AdjustmentType)
	if !ok {
		return corpaction.Event{}, fmt.Errorf("unknown adjustment_type %q", s.AdjustmentType)
	}
	effective, err := corpaction.ParseEffectiveDate(s.ExecutionDate)
	if err != nil {
		return corpaction.Event{}, err
	}
	return corpaction.NewSplit(s.Ticker, effective, s.SplitFrom, s.SplitTo, subtype)
}

// Build maps a full split history (results) to the writes that bring the
// tracked symbols' SPLITS buckets in line with it.
//
// tracked reports whether a symbol is tracked; with wildcard set, every
// symbol in results is tracked too. stored holds the events already stored
// for tracked symbols (corpaction.LoadAll), keyed by symbol.
func Build(results []api.Split, tracked func(string) bool, wildcard bool,
	stored map[string][]corpaction.Event,
) Plan {
	var plan Plan
	fresh := map[string][]corpaction.Event{}
	for _, s := range results {
		if !wildcard && !tracked(s.Ticker) {
			continue
		}
		e, err := ToEvent(s)
		if err != nil {
			plan.Skipped = append(plan.Skipped, Skipped{Split: s, Reason: err.Error()})
			continue
		}
		fresh[e.Symbol] = append(fresh[e.Symbol], e)
	}

	symbols := map[string]struct{}{}
	for sym := range fresh {
		symbols[sym] = struct{}{}
	}
	for sym := range stored {
		symbols[sym] = struct{}{}
	}
	ordered := make([]string, 0, len(symbols))
	for sym := range symbols {
		ordered = append(ordered, sym)
	}
	sort.Strings(ordered)

	for _, sym := range ordered {
		neutral := corpaction.Neutralize(stored[sym], fresh[sym])
		if len(fresh[sym]) == 0 && len(neutral) == 0 {
			continue
		}
		if len(neutral) == 0 && sameEvents(stored[sym], fresh[sym]) {
			plan.Unchanged++
			continue
		}
		plan.Events = append(plan.Events, fresh[sym]...)
		plan.Events = append(plan.Events, neutral...)
		plan.Written += len(fresh[sym])
		plan.Neutralized += len(neutral)
		plan.Symbols = append(plan.Symbols, sym)
	}
	return plan
}

type rowKey struct {
	epoch                   int64
	price, volume, from, to float64
	subtype                 corpaction.Subtype
}

func key(e corpaction.Event) rowKey {
	return rowKey{e.Effective.Unix(), e.PriceFactor, e.VolumeFactor, e.SplitFrom, e.SplitTo, e.Subtype}
}

// sameEvents reports whether every fresh event is already stored as is.
func sameEvents(stored, fresh []corpaction.Event) bool {
	have := make(map[rowKey]struct{}, len(stored))
	for _, e := range stored {
		have[key(e)] = struct{}{}
	}
	for _, e := range fresh {
		if _, ok := have[key(e)]; !ok {
			return false
		}
	}
	return true
}
