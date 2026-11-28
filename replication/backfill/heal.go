package backfill

import (
	"context"
	"fmt"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// HealRange re-pulls [from, to] (inclusive, unix seconds) of one fixed bucket
// from the master and writes the rows that differ from what is on disk. Unlike
// BackfillBucket it is not anchored at the watermark and leaves it alone: it
// heals one range named by the master's change ledger, however old.
//
// Only fixed buckets may be healed this way; re-pulling a variable bucket
// appends, so it would duplicate rows.
func HealRange(ctx context.Context, api MasterAPI, readLocal ReadFunc, write WriteFunc,
	tbk string, from, to, now int64,
) (rows int, err error) {
	if from > to {
		return 0, nil
	}
	csm, err := api.QueryRange(ctx, tbk, from, to)
	if err != nil {
		return 0, err
	}
	// Events (corporate actions) are complete when written, and are often
	// dated in the future: only bars wait for their period to close.
	if cutoff, ok := lastClosedBarEpoch(tbk, now); ok && !isEventBucket(tbk) {
		closed := func(e int64) bool { return e <= cutoff }
		csm = FilterCSM(csm, closed)
	}
	if CSMRows(csm) == 0 {
		return 0, nil
	}
	if readLocal != nil {
		local, rerr := readLocal(ctx, tbk, from, to)
		if rerr != nil {
			log.Warn("[replication-heal] local read failed for %s, rewriting [%d, %d] blind: %v", tbk, from, to, rerr)
		} else {
			needed := map[io.TimeBucketKey]map[int64]struct{}{}
			for key, d := range CSMRowDiff(csm, local) {
				if d.SchemaIssue != "" {
					log.Warn("[replication-heal] %s: cannot compare with master (%s); leaving it untouched",
						tbk, d.SchemaIssue)
					continue
				}
				epochs := d.NeedsWrite()
				if len(epochs) == 0 {
					continue
				}
				set := make(map[int64]struct{}, len(epochs))
				for _, e := range epochs {
					set[e] = struct{}{}
				}
				needed[key] = set
			}
			csm = FilterCSMByBucket(csm, func(key io.TimeBucketKey, e int64) bool {
				_, ok := needed[key][e]
				return ok
			})
			if CSMRows(csm) == 0 {
				return 0, nil
			}
		}
	}
	if err := write(csm, false); err != nil {
		return 0, fmt.Errorf("heal %s [%d, %d]: write: %w", tbk, from, to, err)
	}
	return CSMRows(csm), nil
}
