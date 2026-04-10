package flatfiles

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	utilsio "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const dateFormat = "2006-01-02"

// DataTypes maps query_start keys for bars to S3 flat file data type names.
// Only these data types are supported by the flat file backfiller.
var DataTypes = map[string]string{
	"1D":   "day_aggs_v1",
	"1Min": "minute_aggs_v1",
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
// Returns the total rows matched and total symbol-writes across all dates.
func BackfillDates(
	ctx context.Context,
	s3Client *S3Client,
	w backfill.Writer,
	symbolSet map[string]bool,
	timeframe string, // "1D" or "1Min"
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
	downloadWP := worker.NewWorkerPool(ctx, cfg.Parallelism)

	// Buffered channel decouples download workers from writer goroutines.
	// Download workers can continue fetching while writers are busy flushing.
	writeCh := make(chan writeJob, cfg.WriteBufferSize)

	var processedDates int64

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

	// Start writer goroutines.
	var writerWg sync.WaitGroup
	for i := 0; i < cfg.WriteConcurrency; i++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
			for job := range writeCh {
				writeStart := time.Now()
				if writeErr := w.WriteCSM(job.csm, false); writeErr != nil {
					log.Warn("[flatfiles] %s: failed to write %s: %v",
						job.date.Format(dateFormat), job.timeframe, writeErr)
				}
				atomic.AddInt64(&totalWriteNs, int64(time.Since(writeStart)))
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
			csm, stats, ok := downloadAndParse(ctx, s3Client, symbolSet, s3DataType, timeframe, currentDate)
			dlDuration := time.Since(dlStart)
			if !ok {
				return
			}

			atomic.AddInt64(&totalDownloadNs, int64(dlDuration))

			// Send to writer channel (buffered, may block if buffer is full).
			if len(csm) > 0 {
				idx := dateIndex[currentDate]
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
			}

			n := atomic.AddInt64(&processedDates, 1)
			atomic.AddInt64(&totalRows, int64(stats.RowsMatched))
			atomic.AddInt64(&totalSymbols, int64(stats.SymbolCount))

			// Advance the contiguous high-water mark.
			idx := dateIndex[currentDate]
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

			if n%50 == 0 || n == int64(len(dates)) {
				log.Info("[flatfiles] %s: processed %d/%d dates (%d rows, %d symbol-writes)",
					timeframe, n, len(dates),
					atomic.LoadInt64(&totalRows), atomic.LoadInt64(&totalSymbols))
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

// retryDelays defines the backoff between retries.
var retryDelays = [maxRetries]time.Duration{
	1 * time.Second,
	3 * time.Second,
	10 * time.Second,
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
	s3DataType, timeframe string,
	date time.Time,
) (utilsio.ColumnSeriesMap, ParseStats, bool) {
	dateStr := date.Format(dateFormat)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			log.Info("[flatfiles] %s: retrying %s (attempt %d/%d)", dateStr, timeframe, attempt+1, maxRetries+1)
			select {
			case <-ctx.Done():
				return nil, ParseStats{}, false
			case <-time.After(retryDelays[attempt-1]):
			}
		}

		reader, err := s3Client.Download(ctx, s3DataType, date)
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
