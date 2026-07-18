package flatfiles

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/mapping"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	utilsio "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// tickWriteJob is a unit of work for the tick writer goroutines. Unlike the bar
// path, a single date may produce many jobs (one per matched symbol), so each
// job tracks whether it is the last for its date to drive high-water marking.
type tickWriteJob struct {
	csm     utilsio.ColumnSeriesMap
	date    time.Time
	dataset string
}

// BackfillTicks downloads tick flat files (trades/quotes) for each date,
// stream-parses them per symbol, and writes each symbol's CSM to MarketStore as
// variable-length records. Concurrency is across dates (download/parse), with
// small per-symbol writes flowing through a buffered channel to writer
// goroutines.
//
// dataType is "trades" or "quotes". exMap resolves exchange ids; roundLot
// resolves a symbol's round lot for quote-size normalization (may be nil for
// trades).
func BackfillTicks(
	ctx context.Context,
	s3Client *S3Client,
	w backfill.Writer,
	symbolSet map[string]bool,
	dataType string, // "trades" | "quotes"
	s3Prefix, s3DataType string,
	exMap *mapping.ExchangeMap,
	roundLot func(string) int,
	dates []time.Time,
	cfg BackfillConfig,
) (totalRows, totalSymbols int64, err error) {
	if len(dates) == 0 {
		return 0, 0, nil
	}

	// Tick-specific defaults: downloads are multi-GB so parallelism is low;
	// writes are small per-symbol so a couple of writers suffice.
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 3
	}
	if cfg.WriteConcurrency <= 0 {
		cfg.WriteConcurrency = 2
	}
	if cfg.WriteBufferSize <= 0 {
		cfg.WriteBufferSize = 64
	}

	log.Info("[flatfiles] backfilling %s: %d market days for %d symbols (downloads=%d, writers=%d, buffer=%d)",
		dataType, len(dates), len(symbolSet),
		cfg.Parallelism, cfg.WriteConcurrency, cfg.WriteBufferSize)

	startTime := time.Now()

	var processedDates int64
	progress := NewProgressReporter(dataType, int64(len(dates)),
		func() int64 { return atomic.LoadInt64(&processedDates) },
		0, cfg.ProgressBar)
	progress.Start()
	defer progress.Finish()

	downloadWP := worker.NewWorkerPool(ctx, cfg.Parallelism)
	writeCh := make(chan tickWriteJob, cfg.WriteBufferSize)

	var totalDownloadNs, totalWriteNs int64

	// High-water mark tracking.
	var (
		hwMu      sync.Mutex
		completed = make([]bool, len(dates))
		highWater = -1
	)
	dateIndex := make(map[time.Time]int, len(dates))
	for i, d := range dates {
		dateIndex[d] = i
	}
	markComplete := func(idx int) {
		atomic.AddInt64(&processedDates, 1)
		hwMu.Lock()
		completed[idx] = true
		newHW := highWater
		for newHW+1 < len(completed) && completed[newHW+1] {
			newHW++
		}
		advanced := newHW > highWater
		highWater = newHW
		hwMu.Unlock()
		if advanced && cfg.OnProgress != nil {
			cfg.OnProgress(dates[newHW])
		}
	}

	// Writer goroutines persist per-symbol CSMs as variable-length records.
	var writerWg sync.WaitGroup
	for i := 0; i < cfg.WriteConcurrency; i++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
			for job := range writeCh {
				writeStart := time.Now()
				if writeErr := w.WriteCSM(job.csm, true); writeErr != nil {
					log.Warn("[flatfiles] %s: failed to write %s: %v",
						job.date.Format(dateFormat), job.dataset, writeErr)
				}
				atomic.AddInt64(&totalWriteNs, int64(time.Since(writeStart)))
			}
		}()
	}

	// Dispatch one download+parse task per date.
	for _, date := range dates {
		select {
		case <-ctx.Done():
			downloadWP.CloseAndWait()
			close(writeCh)
			writerWg.Wait()
			return atomic.LoadInt64(&totalRows), atomic.LoadInt64(&totalSymbols), ctx.Err()
		default:
		}

		currentDate := date
		downloadWP.Do(func() {
			dlStart := time.Now()
			stats, ok := downloadAndParseTicks(ctx, s3Client, symbolSet, s3Prefix, s3DataType,
				dataType, exMap, roundLot, currentDate, func(csm utilsio.ColumnSeriesMap) {
					select {
					case writeCh <- tickWriteJob{csm: csm, date: currentDate, dataset: dataType}:
					case <-ctx.Done():
					}
				})
			atomic.AddInt64(&totalDownloadNs, int64(time.Since(dlStart)))
			if ok {
				atomic.AddInt64(&totalRows, int64(stats.RowsMatched))
				atomic.AddInt64(&totalSymbols, int64(stats.SymbolCount))
			}
			// Mark the date complete whether or not it had data, so the
			// checkpoint high-water mark advances over empty/missing dates.
			markComplete(dateIndex[currentDate])
		})
	}

	downloadWP.CloseAndWait()
	close(writeCh)
	writerWg.Wait()

	r := atomic.LoadInt64(&totalRows)
	s := atomic.LoadInt64(&totalSymbols)
	elapsed := time.Since(startTime)
	log.Info("[flatfiles] %s complete: %d dates, %d rows, %d symbol-writes in %s",
		dataType, atomic.LoadInt64(&processedDates), r, s, elapsed.Round(time.Millisecond))
	log.Info("[flatfiles] %s timing: download+parse=%s, write=%s (wall=%s)",
		dataType,
		time.Duration(atomic.LoadInt64(&totalDownloadNs)).Round(time.Millisecond),
		time.Duration(atomic.LoadInt64(&totalWriteNs)).Round(time.Millisecond),
		elapsed.Round(time.Millisecond))

	return r, s, nil
}

// downloadAndParseTicks spills the S3 object to a temp file, then
// stream-decompresses and parses it, invoking emit per symbol. Transient
// download/parse errors are retried with backoff (reusing the bar path's
// retry constants).
func downloadAndParseTicks(
	ctx context.Context,
	s3Client *S3Client,
	symbolSet map[string]bool,
	s3Prefix, s3DataType, dataType string,
	exMap *mapping.ExchangeMap,
	roundLot func(string) int,
	date time.Time,
	emit func(utilsio.ColumnSeriesMap),
) (ParseStats, bool) {
	dateStr := date.Format(dateFormat)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := jitteredDelay(retryDelays[attempt-1])
			log.Info("[flatfiles] %s: retrying %s (attempt %d/%d, backoff %s)",
				dateStr, dataType, attempt+1, maxRetries+1, delay.Round(time.Millisecond))
			select {
			case <-ctx.Done():
				return ParseStats{}, false
			case <-time.After(delay):
			}
		}

		path, cleanup, err := s3Client.DownloadToTempFile(ctx, s3Prefix, s3DataType, date)
		if err != nil {
			if ctx.Err() != nil {
				return ParseStats{}, false
			}
			if isNonRetryable(err) {
				log.Warn("[flatfiles] %s: %s not available: %v", dateStr, dataType, err)
				return ParseStats{}, false
			}
			if attempt < maxRetries {
				log.Warn("[flatfiles] %s: download %s failed (will retry): %v", dateStr, dataType, err)
				continue
			}
			log.Warn("[flatfiles] %s: failed to download %s after %d attempts: %v",
				dateStr, dataType, maxRetries+1, err)
			return ParseStats{}, false
		}

		reader, err := OpenTempGzip(path)
		if err != nil {
			cleanup()
			if attempt < maxRetries {
				log.Warn("[flatfiles] %s: open %s failed (will retry): %v", dateStr, dataType, err)
				continue
			}
			return ParseStats{}, false
		}

		var stats ParseStats
		switch dataType {
		case "trades":
			stats, err = ParseTradesStream(reader, symbolSet, exMap, date, emit)
		case "quotes":
			stats, err = ParseQuotesStream(reader, symbolSet, exMap, roundLot, date, emit)
		default:
			reader.Close()
			cleanup()
			log.Error("[flatfiles] unknown tick data type %q", dataType)
			return ParseStats{}, false
		}
		reader.Close()
		cleanup()

		if err != nil {
			if ctx.Err() != nil {
				return ParseStats{}, false
			}
			if attempt < maxRetries {
				log.Warn("[flatfiles] %s: parse %s failed (will retry): %v", dateStr, dataType, err)
				continue
			}
			log.Warn("[flatfiles] %s: failed to parse %s after %d attempts: %v",
				dateStr, dataType, maxRetries+1, err)
			return ParseStats{}, false
		}

		return stats, true
	}

	return ParseStats{}, false
}
