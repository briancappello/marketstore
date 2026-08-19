package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Driver reconciles the whole bucket set against the master.
type Driver struct {
	api         MasterAPI
	write       WriteFunc
	wm          *Watermarks
	parallelism int
	lookback    time.Duration
	isVariable  func(tbk string) bool
}

func NewDriver(api MasterAPI, write WriteFunc, wm *Watermarks, parallelism int, lookback time.Duration, isVariable func(tbk string) bool) *Driver {
	if parallelism <= 0 {
		parallelism = 8
	}
	return &Driver{api: api, write: write, wm: wm, parallelism: parallelism, lookback: lookback, isVariable: isVariable}
}

// Reconcile enumerates every bucket on the master and backfills each from its
// watermark up to now, concurrently. Per-bucket errors are logged, not fatal:
// a transient failure is retried on the next reconcile.
func (d *Driver) Reconcile(ctx context.Context, now int64) error {
	tbks, err := d.api.ListTBKs(ctx)
	if err != nil {
		return fmt.Errorf("enumerate buckets: %w", err)
	}
	wp := worker.NewWorkerPool(ctx, d.parallelism)
	for _, tbk := range tbks {
		tbk := tbk
		wp.Do(func() {
			if err := BackfillBucket(ctx, d.api, d.write, d.wm, tbk, now, d.lookback, d.isVariable(tbk)); err != nil {
				log.Warn("[replication-backfill] %s: %v", tbk, err)
			}
		})
	}
	wp.CloseAndWait()
	return nil
}
