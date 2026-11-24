package corpaction

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// Load returns a symbol's events of the given kinds, sorted by effective
// time. Neutralized events are left out. A symbol without a bucket returns no
// events and no error.
func Load(catalogDir *catalog.Directory, symbol string, kinds ...Kind) ([]Event, error) {
	events, err := LoadAll(catalogDir, symbol, kinds...)
	if err != nil {
		return nil, err
	}
	out := events[:0]
	for _, e := range events {
		if !e.IsNeutral() {
			out = append(out, e)
		}
	}
	return out, nil
}

// LoadAll is Load including neutralized events.
func LoadAll(catalogDir *catalog.Directory, symbol string, kinds ...Kind) ([]Event, error) {
	if catalogDir == nil {
		return nil, fmt.Errorf("corpaction: nil catalog directory")
	}
	if len(kinds) == 0 {
		kinds = AllKinds
	}
	var events []Event
	for _, kind := range kinds {
		kindEvents, err := loadKind(catalogDir, symbol, kind)
		if err != nil {
			return nil, err
		}
		events = append(events, kindEvents...)
	}
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Effective.Before(events[j].Effective)
	})
	return events, nil
}

func loadKind(catalogDir *catalog.Directory, symbol string, kind Kind) ([]Event, error) {
	tbk := TimeBucketKey(symbol, kind)
	if _, err := catalogDir.GetLatestTimeBucketInfoFromKey(tbk); err != nil {
		// No bucket for this symbol and kind.
		return nil, nil
	}

	query := planner.NewQuery(catalogDir)
	query.AddTargetKey(tbk)
	query.SetRange(time.Unix(0, 0), time.Unix(math.MaxInt32, 0))
	parsed, err := query.Parse()
	if err != nil {
		if err.Error() == "no files returned from query parse" {
			return nil, nil
		}
		return nil, fmt.Errorf("parse %s query: %w", tbk, err)
	}
	reader, err := executor.NewReader(parsed)
	if err != nil {
		return nil, fmt.Errorf("create %s reader: %w", tbk, err)
	}
	csm, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", tbk, err)
	}
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return nil, nil
	}
	return decode(symbol, kind, cs)
}

func decode(symbol string, kind Kind, cs *io.ColumnSeries) ([]Event, error) {
	epochs, ok1 := cs.GetColumn("Epoch").([]int64)
	priceFactors, ok2 := cs.GetColumn(ColPriceFactor).([]float64)
	volumeFactors, ok3 := cs.GetColumn(ColVolumeFactor).([]float64)
	froms, ok4 := cs.GetColumn(ColSplitFrom).([]float64)
	tos, ok5 := cs.GetColumn(ColSplitTo).([]float64)
	subtypes, ok6 := cs.GetColumn(ColSubtype).([]byte)
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6) {
		return nil, fmt.Errorf("%s/%s: unexpected column layout %v", symbol, kind, cs.GetColumnNames())
	}
	events := make([]Event, len(epochs))
	for i, epoch := range epochs {
		events[i] = Event{
			Symbol:       symbol,
			Kind:         kind,
			Effective:    storedDay(epoch),
			PriceFactor:  priceFactors[i],
			VolumeFactor: volumeFactors[i],
			SplitFrom:    froms[i],
			SplitTo:      tos[i],
			Subtype:      Subtype(subtypes[i]),
		}
	}
	return events, nil
}

// storedDay converts the epoch a 1D bucket returns, which is midnight of the
// stored day in the server timezone, back to 00:00 America/New_York of that
// calendar day.
func storedDay(epoch int64) time.Time {
	y, m, d := io.ToSystemTimezone(time.Unix(epoch, 0)).Date()
	return EffectiveDate(y, m, d)
}
