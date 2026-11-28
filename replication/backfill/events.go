package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/corpaction"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// eventHorizon is how far past now an event bucket is read. Corporate actions
// are announced weeks or months before their effective date.
const eventHorizon = 2 * 366 * 24 * time.Hour

// isEventBucket reports whether tbk holds dated events rather than bars: a
// corporate-action bucket (<SYMBOL>/1D/SPLITS, ...). Its rows are complete when
// written and are often dated in the future, so neither the closed-bar rule
// nor a window ending at now applies to it.
func isEventBucket(tbk string) bool {
	key := io.NewTimeBucketKey(tbk)
	if key == nil {
		return false
	}
	group := key.GetItemInCategory("AttributeGroup")
	for _, k := range corpaction.AllKinds {
		if group == k.AttributeGroup() {
			return true
		}
	}
	return false
}

// backfillEvents makes an event bucket equal to the master's over its whole
// range, future dates included. Event buckets are small, so every pass
// compares everything and writes only the rows that differ; the watermark is
// kept at or below now, as it means "synced up to".
func backfillEvents(ctx context.Context, api MasterAPI, readLocal ReadFunc, write WriteFunc, wm *Watermarks,
	tbk string, now int64,
) (rows int, advanced bool, err error) {
	prev := wm.Get(tbk)
	end := now + int64(eventHorizon.Seconds())
	csm, err := api.QueryRange(ctx, tbk, 1, end)
	if err != nil {
		return 0, false, err
	}
	if CSMRows(csm) == 0 {
		return 0, false, nil
	}
	if readLocal != nil {
		local, rerr := readLocal(ctx, tbk, 1, end)
		if rerr != nil {
			// A bucket the replica does not have yet reads as an error: write
			// all of it.
			log.Debug("[replication-backfill] %s: local read failed, writing every row: %v", tbk, rerr)
		} else {
			needed := map[io.TimeBucketKey]map[int64]struct{}{}
			for key, d := range CSMRowDiff(csm, local) {
				recordRowDiff(d)
				recordSkipReason(diffReason(d), tbk)
				if d.SchemaIssue != "" {
					log.Warn("[replication-backfill] %s: cannot compare with master (%s); leaving it untouched",
						tbk, d.SchemaIssue)
					continue
				}
				set := map[int64]struct{}{}
				for _, e := range d.NeedsWrite() {
					set[e] = struct{}{}
				}
				if len(set) > 0 {
					needed[key] = set
				}
			}
			csm = FilterCSMByBucket(csm, func(key io.TimeBucketKey, e int64) bool {
				_, ok := needed[key][e]
				return ok
			})
		}
	}
	if rows = CSMRows(csm); rows > 0 {
		if err := write(csm, false); err != nil {
			return 0, false, fmt.Errorf("write %s: %w", tbk, err)
		}
	}
	if err := wm.Set(tbk, now); err != nil {
		return rows, false, err
	}
	return rows, now > prev, nil
}
