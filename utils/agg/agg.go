// Package agg provides on-the-fly aggregation of OHLCV bar data.
package agg

import (
	"fmt"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// Aggregate aggregates the source ColumnSeries to the target timeframe.
// The source data must be OHLCV bar data with at least Open, High, Low, Close columns.
// Volume column is optional.
func Aggregate(cs *io.ColumnSeries, targetTimeframe string) (*io.ColumnSeries, error) {
	if cs == nil || cs.Len() == 0 {
		return io.NewColumnSeries(), nil
	}

	timeWindow, err := utils.CandleDurationFromString(targetTimeframe)
	if err != nil {
		return nil, fmt.Errorf("invalid target timeframe %q: %w", targetTimeframe, err)
	}

	params := getParams(cs.Exists("Volume"))
	accumGroup := newAccumGroup(cs, params)

	ts, err := cs.GetTime()
	if err != nil {
		return nil, fmt.Errorf("get time from column series: %w", err)
	}

	outEpoch := make([]int64, 0)

	groupKey := timeWindow.Truncate(ts[0])
	groupStart := 0

	// Accumulate inputs. Since the input is ordered by time,
	// it is just to slice by correct boundaries.
	for i, t := range ts {
		if !timeWindow.IsWithin(t, groupKey) {
			// Emit new row and re-init aggState
			outEpoch = append(outEpoch, groupKey.Unix())
			if err := accumGroup.apply(groupStart, i); err != nil {
				return nil, fmt.Errorf("apply to group. groupStart=%d, i=%d: %w", groupStart, i, err)
			}
			groupKey = timeWindow.Truncate(t)
			groupStart = i
		}
	}

	// Accumulate any remaining values
	outEpoch = append(outEpoch, groupKey.Unix())
	if err := accumGroup.apply(groupStart, len(ts)); err != nil {
		return nil, fmt.Errorf("apply to group. groupStart=%d, i=%d: %w", groupStart, len(ts), err)
	}

	// Finalize output
	outCs := io.NewColumnSeries()
	outCs.AddColumn("Epoch", outEpoch)
	accumGroup.addColumns(outCs)

	return outCs, nil
}

func getParams(volumeColExists bool) []accumParam {
	params := []accumParam{
		{"Open", "first", "Open"},
		{"High", "max", "High"},
		{"Low", "min", "Low"},
		{"Close", "last", "Close"},
	}
	if volumeColExists {
		params = append(params, accumParam{"Volume", "sum", "Volume"})
	}
	return params
}
