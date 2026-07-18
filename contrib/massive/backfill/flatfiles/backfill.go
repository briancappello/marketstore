package flatfiles

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/massiveconfig"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	utilsio "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const dateFormat = "2006-01-02"

// slowWriteThreshold is the per-date write duration above which a write is
// logged at info level. This surfaces slow persists (common for 1Min data
// covering the full symbol universe) so they are not mistaken for a hang.
const slowWriteThreshold = 5 * time.Second

// FlatFileType describes an S3 flat file data source: the S3 key prefix and
// the data type subdirectory within that prefix.
type FlatFileType struct {
	// S3Prefix is the top-level S3 key prefix (e.g., "us_stocks_sip" or "us_indices").
	S3Prefix string
	// S3DataType is the data type subdirectory (e.g., "day_aggs_v1" or "minute_aggs_v1").
	S3DataType string
	// Tick is true for tick-level data ("trades"/"quotes"), which routes to
	// BackfillTicks (variable-length, per-symbol streaming) rather than the
	// bar path BackfillDates. For bar types the map key is also the timeframe;
	// for tick types the key is a data-type name (the bucket uses 1Sec).
	Tick bool
}

// DataTypes maps query_start keys to their S3 flat file location.
// Only these data types are supported by the flat file backfiller.
//
// The bar keys ("1D", "1Min", "1D-index") are also MarketStore timeframes; the
// tick keys ("trades", "quotes") are data-type names and MUST route to
// BackfillTicks (see IsTickKey).
var DataTypes = map[string]FlatFileType{
	"1D":       {S3Prefix: massiveconfig.DefaultS3Prefix, S3DataType: "day_aggs_v1"},
	"1Min":     {S3Prefix: massiveconfig.DefaultS3Prefix, S3DataType: "minute_aggs_v1"},
	"1D-index": {S3Prefix: massiveconfig.DefaultS3IndicesPrefix, S3DataType: "day_aggs_v1"},
	"trades":   {S3Prefix: massiveconfig.DefaultS3Prefix, S3DataType: "trades_v1", Tick: true},
	"quotes":   {S3Prefix: massiveconfig.DefaultS3Prefix, S3DataType: "quotes_v1", Tick: true},
}

// IsTickKey reports whether a -from/query_start key refers to tick-level data
// ("trades"/"quotes") that must be processed by BackfillTicks.
func IsTickKey(key string) bool {
	ft, ok := DataTypes[key]
	return ok && ft.Tick
}

// ProgressFunc is called when the contiguous high-water mark of completed dates
// advances. The date argument is the latest date in the contiguous sequence of
// completed dates from the start of the list. This allows the caller to
// incrementally update a checkpoint file as progress is made.
type ProgressFunc func(date time.Time)

// BackfillConfig holds tunable parameters for BackfillDates.
type BackfillConfig struct {
	// Parallelism is the number of concurrent S3 download goroutines.
	// Defaults to runtime.NumCPU() if <= 0.
	Parallelism int

	// WriteConcurrency is the number of concurrent writer goroutines.
	// Defaults to 2 if <= 0.
	WriteConcurrency int

	// WriteBufferSize is the capacity of the buffered channel between
	// download workers and writer goroutines. A larger buffer decouples
	// download throughput from write throughput, at the cost of holding
	// more CSMs in memory. Each buffered slot holds one date's worth of
	// parsed bar data.
	// Defaults to 10 if <= 0.
	WriteBufferSize int

	// OnProgress is called whenever the contiguous high-water mark of
	// completed dates advances, allowing the caller to persist incremental
	// checkpoint state. May be nil.
	OnProgress ProgressFunc

	// ProgressBar controls the interactive progress bar rendered to stderr:
	// "always" forces it on, "never" disables it, and "auto" (or "") enables
	// it only when stderr is a terminal. The bar shows percent complete,
	// processing rate (dates/sec), and ETA.
	ProgressBar string
}

// writeJob is a unit of work for the writer goroutines.
type writeJob struct {
	csm       utilsio.ColumnSeriesMap
	date      time.Time
	timeframe string
	dateIdx   int // index in the dates slice for high-water tracking
}

// BackfillDates downloads flat files from S3 for each date in the provided list,
// parses them, filters by symbolSet, and writes all matched bars to MarketStore
// via the provided writer.
//
// Downloads happen concurrently (up to cfg.Parallelism goroutines). Parsed CSMs
// are sent through a buffered channel (capacity cfg.WriteBufferSize) to a pool
// of cfg.WriteConcurrency writer goroutines. This decouples download throughput
// from write throughput, preventing network stalls when writes are slow.
//
// s3Prefix is the top-level S3 key prefix (e.g., "us_stocks_sip" or "us_indices").
//
// Returns the total rows matched and total symbol-writes across all dates.
func BackfillDates(
	ctx context.Context,
	s3Client *S3Client,
	w backfill.Writer,
	symbolSet map[string]bool,
	timeframe string, // "1D", "1Min", or "1D-index"
	s3Prefix string, // "us_stocks_sip" or "us_indices"
	s3DataType string, // "day_aggs_v1" or "minute_aggs_v1"
	dates []time.Time,
	cfg BackfillConfig,
) (totalRows, totalSymbols int64, err error) {
	if len(dates) == 0 {
		return 0, 0, nil
	}

	// Apply defaults.
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 8
	}
	if cfg.WriteConcurrency <= 0 {
		cfg.WriteConcurrency = 2
	}
	if cfg.WriteBufferSize <= 0 {
		cfg.WriteBufferSize = 10
	}

	log.Info("[flatfiles] backfilling %s: %d market days for %d symbols (downloads=%d, writers=%d, buffer=%d)",
		timeframe, len(dates), len(symbolSet),
		cfg.Parallelism, cfg.WriteConcurrency, cfg.WriteBufferSize)

	startTime := time.Now()

	var processedDates int64

	// Interactive progress bar (rendered to stderr; see progress.go). It polls
	// processedDates on a timer so it advances smoothly even when downloads
	// stall. Disabled automatically when stderr is not a terminal unless
	// ProgressBar == "always".
	progress := NewProgressReporter(timeframe, int64(len(dates)),
		func() int64 { return atomic.LoadInt64(&processedDates) },
		0, cfg.ProgressBar)
	progress.Start()
	defer progress.Finish()

	downloadWP := worker.NewWorkerPool(ctx, cfg.Parallelism)

	// Buffered channel decouples download workers from writer goroutines.
	// Download workers can continue fetching while writers are busy flushing.
	writeCh := make(chan writeJob, cfg.WriteBufferSize)

	// --- Timing stats (atomic, nanoseconds) ---
	var totalDownloadNs int64
	var totalWriteNs int64

	// --- High-water mark tracking ---
	var (
		hwMu      sync.Mutex
		completed = make([]bool, len(dates))
		highWater = -1
	)

	dateIndex := make(map[time.Time]int, len(dates))
	for i, d := range dates {
		dateIndex[d] = i
	}

	// markComplete records that the date at index idx has been fully handled
	// (persisted, or skipped because it had no data) and advances the
	// contiguous high-water mark / checkpoint. It is the single place that
	// increments processedDates, so the progress bar reflects DURABLY-WRITTEN
	// dates rather than merely downloaded/enqueued ones.
	//
	// It is called from the writer goroutines (after a successful WriteCSM)
	// and from the download workers for dates that produced no data.
	markComplete := func(idx int) {
		n := atomic.AddInt64(&processedDates, 1)

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

		// When the interactive bar is active it already conveys progress,
		// so suppress the redundant periodic count log to avoid noise.
		if !progress.Active() && (n%50 == 0 || n == int64(len(dates))) {
			log.Info("[flatfiles] %s: processed %d/%d dates (%d rows, %d symbol-writes)",
				timeframe, n, len(dates),
				atomic.LoadInt64(&totalRows), atomic.LoadInt64(&totalSymbols))
		}
	}

	// Start writer goroutines.
	var writerWg sync.WaitGroup
	for i := 0; i < cfg.WriteConcurrency; i++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
			for job := range writeCh {
				// Persisting a single date can take a long time for large
				// CSMs (e.g. 1Min for the full symbol universe writes one file
				// per symbol). Log start+duration so a slow write is visibly
				// distinguishable from a hang, and so there is activity before
				// the first date's progress tick.
				log.Debug("[flatfiles] %s: writing %s (%d symbols)...",
					job.date.Format(dateFormat), job.timeframe, len(job.csm))
				writeStart := time.Now()
				if writeErr := w.WriteCSM(job.csm, false); writeErr != nil {
					log.Warn("[flatfiles] %s: failed to write %s: %v",
						job.date.Format(dateFormat), job.timeframe, writeErr)
				}
				writeDur := time.Since(writeStart)
				atomic.AddInt64(&totalWriteNs, int64(writeDur))
				if writeDur > slowWriteThreshold {
					log.Info("[flatfiles] %s: wrote %s (%d symbols) in %s",
						job.date.Format(dateFormat), job.timeframe, len(job.csm),
						writeDur.Round(time.Millisecond))
				}
				// Count the date only after it is durably written.
				markComplete(job.dateIdx)
			}
		}()
	}

	// Dispatch download workers.
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
			// Download.
			dlStart := time.Now()
			csm, stats, ok := downloadAndParse(ctx, s3Client, symbolSet, s3Prefix, s3DataType, timeframe, currentDate)
			dlDuration := time.Since(dlStart)
			if !ok {
				return
			}

			atomic.AddInt64(&totalDownloadNs, int64(dlDuration))
			atomic.AddInt64(&totalRows, int64(stats.RowsMatched))
			atomic.AddInt64(&totalSymbols, int64(stats.SymbolCount))

			idx := dateIndex[currentDate]

			// Send to writer channel (buffered, may block if buffer is full).
			// The date is counted as complete by the writer goroutine once it
			// is persisted. Dates with no matching data are never enqueued, so
			// they must be counted here instead.
			if len(csm) > 0 {
				select {
				case writeCh <- writeJob{
					csm:       csm,
					date:      currentDate,
					timeframe: timeframe,
					dateIdx:   idx,
				}:
				case <-ctx.Done():
					return
				}
			} else {
				markComplete(idx)
			}
		})
	}

	// Wait for all downloads to finish, then close the write channel
	// so writer goroutines drain and exit.
	downloadWP.CloseAndWait()
	close(writeCh)
	writerWg.Wait()

	r := atomic.LoadInt64(&totalRows)
	s := atomic.LoadInt64(&totalSymbols)
	elapsed := time.Since(startTime)

	log.Info("[flatfiles] %s complete: %d dates, %d rows, %d symbol-writes in %s",
		timeframe, atomic.LoadInt64(&processedDates), r, s, elapsed.Round(time.Millisecond))
	log.Info("[flatfiles] %s timing: download+parse=%s, write=%s (wall=%s)",
		timeframe,
		time.Duration(atomic.LoadInt64(&totalDownloadNs)).Round(time.Millisecond),
		time.Duration(atomic.LoadInt64(&totalWriteNs)).Round(time.Millisecond),
		elapsed.Round(time.Millisecond))

	return r, s, nil
}

// MarketDays returns the list of market days in the range [start, end] (inclusive).
func MarketDays(start, end time.Time) []time.Time {
	var dates []time.Time
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		if calendar.Nasdaq.IsMarketDay(d) {
			dates = append(dates, d)
		}
	}
	return dates
}

const maxRetries = 3

// retryDelays defines the base backoff between retries. Each delay is
// jittered by +-25% to prevent thundering herd when multiple workers
// fail and retry simultaneously.
var retryDelays = [maxRetries]time.Duration{
	1 * time.Second,
	3 * time.Second,
	10 * time.Second,
}

// jitteredDelay returns d with +-25% uniform random jitter applied.
func jitteredDelay(d time.Duration) time.Duration {
	// jitter range: [0.75*d, 1.25*d]
	jitter := time.Duration(rand.Int63n(int64(d)/2)) - d/4 //nolint:gosec // jitter does not need crypto rand
	return d + jitter
}

// isNonRetryable returns true if the error represents a permanent failure that
// should not be retried (e.g., 404 Not Found, 403 Forbidden).
func isNonRetryable(err error) bool {
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		code := respErr.HTTPStatusCode()
		// 4xx errors (except 429 Too Many Requests) are permanent.
		return code >= 400 && code < 500 && code != 429
	}
	return false
}

// downloadAndParse downloads a flat file from S3, decompresses it, and parses it.
// On transient errors (I/O errors during download or parse), the entire operation
// is retried up to maxRetries times with exponential backoff.
// Returns the parsed CSM, stats, and whether the operation succeeded.
func downloadAndParse(
	ctx context.Context,
	s3Client *S3Client,
	symbolSet map[string]bool,
	s3Prefix, s3DataType, timeframe string,
	date time.Time,
) (utilsio.ColumnSeriesMap, ParseStats, bool) {
	dateStr := date.Format(dateFormat)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := jitteredDelay(retryDelays[attempt-1])
			log.Info("[flatfiles] %s: retrying %s (attempt %d/%d, backoff %s)", dateStr, timeframe, attempt+1, maxRetries+1, delay.Round(time.Millisecond))
			select {
			case <-ctx.Done():
				return nil, ParseStats{}, false
			case <-time.After(delay):
			}
		}

		reader, err := s3Client.DownloadWithPrefix(ctx, s3Prefix, s3DataType, date)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ParseStats{}, false
			}
			// Don't retry permanent HTTP errors (404 Not Found, 403 Forbidden, etc.).
			// Only transient errors (timeouts, connection resets, 500s) are worth retrying.
			if isNonRetryable(err) {
				log.Warn("[flatfiles] %s: %s not available: %v", dateStr, timeframe, err)
				return nil, ParseStats{}, false
			}
			if attempt < maxRetries {
				log.Warn("[flatfiles] %s: download %s failed (will retry): %v", dateStr, timeframe, err)
				continue
			}
			log.Warn("[flatfiles] %s: failed to download %s after %d attempts: %v", dateStr, timeframe, maxRetries+1, err)
			return nil, ParseStats{}, false
		}

		csm, stats, err := ParseAndWrite(reader, symbolSet, timeframe, date)
		reader.Close()

		if err != nil {
			if ctx.Err() != nil {
				return nil, ParseStats{}, false
			}
			if attempt < maxRetries {
				log.Warn("[flatfiles] %s: parse %s failed (will retry): %v", dateStr, timeframe, err)
				continue
			}
			log.Warn("[flatfiles] %s: failed to parse %s after %d attempts: %v", dateStr, timeframe, maxRetries+1, err)
			return nil, ParseStats{}, false
		}

		return csm, stats, true
	}

	return nil, ParseStats{}, false
}
