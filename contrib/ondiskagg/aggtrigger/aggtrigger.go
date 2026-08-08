package aggtrigger

// OnDiskAgg implements a trigger to downsample base timeframe data
// and write to disk.  Underlying data schema is expected at least
// - Open:float32 or float64
// - High:float32 or float64
// - Low:float32 or float64
// - Close:float32 or float64
// optionally,
// - Volume:one of float32, float64, or int32
//
// Example:
// 	triggers:
// 	  - module: ondiskagg.so
// 	    on: */1Min/OHLCV
// 	    config:
// 	      filter: "nasdaq"
// 	      destinations:
// 	        - 5Min
// 	        - 15Min
// 	        - 1H
// 	        - 1D
//
// destinations are downsample target time windows.  Optionally, if filter
// is set to "nasdaq", it filters the scan data by NASDAQ market hours.

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/models"
	modelsenum "github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Config is the configuration for OnDiskAggTrigger you can define in
// marketstore's config file under triggers extension.
type Config struct {
	Destinations []string `json:"destinations"`
	Filter       string   `json:"filter"`
	// CurrentPeriodOnly, when true, restricts writes to destination windows of
	// >= 1 day to the period currently in progress. Completed days are left
	// untouched. See OnDiskAggTrigger.currentPeriodOnly.
	CurrentPeriodOnly bool `json:"current_period_only"`
}

// OnDiskAggTrigger is the main trigger.
type OnDiskAggTrigger struct {
	config       map[string]interface{}
	destinations timeframes
	// filter by market hours if this is "nasdaq"
	filter string
	// currentPeriodOnly suppresses daily-or-longer aggregates for periods that
	// have already completed.
	//
	// Rationale: when an authoritative vendor daily bar is backfilled into the
	// same bucket this trigger writes to, the two race. Every 1Min write fires
	// this trigger, executor.WriteCSM overwrites by epoch, and triggers run
	// asynchronously (executor/written.go fires each in its own goroutine), so
	// merely writing 1Min before 1D does NOT guarantee the vendor bar lands
	// last -- measured ~9% of daily bars were still overwritten that way.
	//
	// With this set, a 1Min write dated to a completed day produces no daily
	// bar at all, so the vendor bar can never be clobbered regardless of write
	// order or goroutine timing. The in-progress day is still aggregated live.
	currentPeriodOnly bool
	aggCache          *sync.Map
	// keyLocks serializes the aggCache read-modify-write per source bucket.
	// One mutex per TimeBucketKey, so different symbols still aggregate in
	// parallel.
	keyLocks *sync.Map
}

var _ trigger.Trigger = &OnDiskAggTrigger{}

func recast(config map[string]interface{}) *Config {
	data, _ := json.Marshal(config)
	ret := Config{}
	_ = json.Unmarshal(data, &ret)
	return &ret
}

// NewTrigger returns a new on-disk aggregate trigger based on the configuration.
func NewTrigger(conf map[string]interface{}) (trigger.Trigger, error) {
	config := recast(conf)

	if len(config.Destinations) == 0 {
		log.Warn("no destinations are configured\n")
		return nil, fmt.Errorf("plugin load error")
	}

	log.Info("%d destination(s) configured\n", len(config.Destinations))

	filter := config.Filter
	if filter != "" && filter != "nasdaq" {
		log.Error("filter value \"%s\" is not recognized\n", filter)
		filter = ""
	}

	var tfs timeframes

	for _, dest := range config.Destinations {
		tf := utils.TimeframeFromString(dest)
		if tf == nil {
			log.Error("invalid destination: %s", dest)
			return nil, errors.New("please specify valid timeframe for 'destinations' " +
				"in the aggtrigger config. dest=" + dest)
		}
		tfs = append(tfs, *tf)
	}

	if config.CurrentPeriodOnly {
		log.Info("current_period_only enabled: completed daily+ periods will not be re-aggregated\n")
	}

	return &OnDiskAggTrigger{
		config:            conf,
		destinations:      tfs,
		filter:            filter,
		currentPeriodOnly: config.CurrentPeriodOnly,
		aggCache:          &sync.Map{},
		keyLocks:          &sync.Map{},
	}, nil
}

// Fire implements trigger interface.
func (s *OnDiskAggTrigger) Fire(keyPath string, records []trigger.Record) {
	elements := strings.Split(keyPath, "/")
	tf := utils.NewTimeframe(elements[1])
	fileName := elements[len(elements)-1]
	year, err := strconv.ParseInt(strings.Replace(fileName, ".bin", "", 1), 10, 32)
	if err != nil {
		log.Error(fmt.Sprintf("failed to extract year from filename=%s: %v", keyPath, err.Error()))
		return
	}
	tbk := io.NewTimeBucketKey(strings.Join(elements[:len(elements)-1], "/"))

	// The dispatcher fires a goroutine per write batch (executor/written.go),
	// and the block below is a read-modify-write of aggCache: Load, union the
	// batch onto the cached series, then Store the result. sync.Map makes each
	// operation atomic but not the sequence, so two concurrent fires for the
	// same bucket both load cache C and each stores "C + own batch" -- the
	// loser's bars vanish from the daily aggregate. Serialize per bucket.
	mu := s.lockFor(tbk.String())
	mu.Lock()
	defer mu.Unlock()

	head := io.IndexToTime(
		records[0].Index(),
		tf.Duration,
		int16(year))

	tail := io.IndexToTime(
		records[len(records)-1].Index(),
		tf.Duration,
		int16(year))

	// query the upper bound since it will contain the most candles
	window, err := utils.CandleDurationFromString(s.destinations.UpperBound().String)
	if err != nil {
		log.Error(fmt.Sprintf("failed to find timeframe: %v", err.Error()))
		return
	}

	// check if we have a valid cache, if not, re-query
	if v, ok := s.aggCache.Load(tbk.String()); ok {
		c, ok := v.(*cachedAgg)
		if !ok {
			log.Error("failed to cast cached value", tbk.String())
			return
		}

		if !c.Valid(tail, head) {
			s.aggCache.Delete(tbk.String())

			goto Query
		}

		cs, err2 := trigger.RecordsToColumnSeries(
			*tbk, c.cs.GetDataShapes(),
			tf.Duration, int16(year), records)
		if err2 != nil {
			log.Error("[ondiskagg]failed to convert record to column series", err2.Error())
			return
		}

		// Argument order matters: ColumnSeriesUnion lets the RIGHT side win on
		// duplicate epochs, and the batch is fresher than the cache. The
		// upstream cascade rewrites the current source bar as more data lands
		// inside it (1Sec/OHLCV -> 1Min re-emits the same minute with a growing
		// cumulative volume), so putting the cache on the right pinned every
		// bar to the first version seen and the aggregate kept only a few
		// seconds of each minute.
		cs = io.ColumnSeriesUnion(&c.cs, cs)

		s.write(tbk, cs, tail, head, elements)

		return
	}

Query:
	csm, err := s.query(tbk, window, head, tail)
	if err != nil || csm == nil {
		log.Error("query error for %v (%v)\n", tbk.String(), err)
		return
	}

	if cs := (*csm)[*tbk]; cs != nil {
		s.write(tbk, cs, tail, head, elements)
	}
}

func (s *OnDiskAggTrigger) write(
	tbk *io.TimeBucketKey,
	cs *io.ColumnSeries,
	tail, head time.Time,
	elements []string,
) {
	for _, dest := range s.destinations {
		symbol := elements[0]
		attributeGroup := elements[2]
		if elements[2] == "TRADE" {
			attributeGroup = "OHLCV"
		}
		aggTbk := io.NewTimeBucketKeyFromString(symbol + "/" + dest.String + "/" + attributeGroup)

		if err := s.writeAggregates(aggTbk, tbk, *cs, dest, head, tail, symbol); err != nil {
			log.Error(
				"failed to write %v aggregates (%v)\n",
				tbk.String(),
				err)
			return
		}
	}
}

type cachedAgg struct {
	cs         io.ColumnSeries
	tail, head time.Time
}

func (c *cachedAgg) Valid(tail, head time.Time) bool {
	return tail.Unix() >= c.tail.Unix() && head.Unix() <= c.head.Unix()
}

func (s *OnDiskAggTrigger) writeAggregates(
	aggTbk, baseTbk *io.TimeBucketKey,
	cs io.ColumnSeries,
	dest utils.Timeframe,
	head, tail time.Time,
	symbol string,
) error {
	csm := io.NewColumnSeriesMap()

	window, err := utils.CandleDurationFromString(dest.String)
	if err != nil {
		return fmt.Errorf("timeframe not found in %s: %w", dest.String, err)
	}
	start := window.Truncate(head).Unix()
	end := window.Ceil(tail).Add(-time.Second).Unix()

	slc, err := io.SliceColumnSeriesByEpoch(cs, &start, &end)
	if err != nil {
		return err
	}

	if len(slc.GetEpoch()) == 0 {
		return nil
	}

	isDailyOrLonger := window.Duration() >= utils.Day

	// Suppress re-aggregation of periods that have already closed, so an
	// authoritative daily bar written by a backfiller is never overwritten.
	// Only the in-progress period is derived from the base timeframe.
	if s.currentPeriodOnly && isDailyOrLonger {
		currentStart := window.Truncate(time.Now().In(calendar.Nasdaq.Tz())).Unix()
		if end < currentStart {
			return nil
		}
		if start < currentStart {
			start = currentStart
			slc, err = io.SliceColumnSeriesByEpoch(cs, &start, &end)
			if err != nil {
				return err
			}
			if len(slc.GetEpoch()) == 0 {
				return nil
			}
		}
	}

	// decide whether to apply market-hour filter
	applyingFilter := false
	if s.filter == "nasdaq" && isDailyOrLonger {
		calendarTz := calendar.Nasdaq.Tz()
		if utils.InstanceConfig.Timezone.String() != calendarTz.String() {
			log.Warn("misconfiguration... system must be configure in %s\n", calendarTz)
		} else {
			applyingFilter = true
		}
	}

	// store when writing for upper bound
	if dest.Duration == s.destinations.UpperBound().Duration {
		defer func() {
			t := window.Truncate(tail)
			tEpoch := t.Unix()
			h := time.Unix(end, 0)

			cacheSlc, _ := io.SliceColumnSeriesByEpoch(cs, &tEpoch, &end)

			s.aggCache.Store(baseTbk.String(), &cachedAgg{
				cs:   cacheSlc,
				tail: t,
				head: h,
			})
		}()
	}

	var (
		cs2, tqSlc *io.ColumnSeries
		err2       error
	)
	// apply the filter
	if applyingFilter {
		// Daily+ bars must reflect the REGULAR session (09:30-16:00 ET).
		// EpochIsMarketOpen spans EXTENDED hours (04:00-20:00 ET) and is
		// therefore a no-op for vendor 1Min data, which is already confined to
		// that range -- it let pre/post-market prints set the daily high, low
		// and volume. applyingFilter is only ever true for daily-or-longer
		// windows, so the regular-session qualifier is always the right one
		// here.
		tqSlc = slc.ApplyTimeQual(calendar.Nasdaq.EpochIsRegularMarketOpen)

		// normally this will always be true, but when there are random bars
		// on the weekend, it won't be, so checking to avoid panic
		if len(tqSlc.GetEpoch()) > 0 {
			cs2, err2 = aggregate(tqSlc, aggTbk, baseTbk, symbol)
			if err2 != nil {
				return fmt.Errorf("ondisk aggregate, applyfilter=%v: %w", applyingFilter, err2)
			}
			csm.AddColumnSeries(*aggTbk, cs2)
		}
		return executor.WriteCSM(csm, false)
	}

	// not applying the filter
	cs2, err2 = aggregate(&slc, aggTbk, baseTbk, symbol)
	if err2 != nil {
		return fmt.Errorf("ondisk aggregate, applyfilter=%v: %w", applyingFilter, err2)
	}
	csm.AddColumnSeries(*aggTbk, cs2)

	return executor.WriteCSM(csm, false)
}

func getParams(volumeColExists bool) []accumParam {
	var params []accumParam
	params = []accumParam{
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

func convertCSToTrades(cs *io.ColumnSeries, symbol string) (*models.Trade, error) {
	trades := models.NewTrade(symbol, cs.Len())
	epochs := cs.GetEpoch()
	nanos, ok := cs.GetColumn("Nanoseconds").([]int32)
	prices, ok2 := cs.GetColumn("Price").([]float64)
	sizes, ok3 := cs.GetColumn("Size").([]float64)
	exchanges, ok4 := cs.GetColumn("Exchange").([]byte)
	tapeids, ok5 := cs.GetColumn("TapeID").([]byte)
	cond1, ok6 := cs.GetColumn("Cond1").([]byte)
	cond2, ok7 := cs.GetColumn("Cond2").([]byte)
	cond3, ok8 := cs.GetColumn("Cond3").([]byte)
	cond4, ok9 := cs.GetColumn("Cond4").([]byte)
	if !(ok && ok2 && ok3 && ok4 && ok5 && ok6 && ok7 && ok8 && ok9) {
		return nil, fmt.Errorf("convert ticks to bars. symbol=%s", symbol)
	}
	// Correction is optional: buckets written before the column was added will
	// not have it, so a missing/typed-mismatched column is tolerated (0).
	correction, _ := cs.GetColumn("Correction").([]byte)
	for i := range epochs {
		condition := []modelsenum.TradeCondition{
			modelsenum.TradeCondition(cond1[i]),
			modelsenum.TradeCondition(cond2[i]),
			modelsenum.TradeCondition(cond3[i]),
			modelsenum.TradeCondition(cond4[i]),
		}
		var corr byte
		if i < len(correction) {
			corr = correction[i]
		}
		trades.Add(
			epochs[i], int(nanos[i]),
			modelsenum.Price(prices[i]),
			sizes[i],
			modelsenum.Exchange(exchanges[i]),
			modelsenum.Tape(tapeids[i]),
			corr,
			condition...)
	}
	return trades, nil
}

func aggregate(cs *io.ColumnSeries, aggTbk, baseTbk *io.TimeBucketKey, symbol string) (*io.ColumnSeries, error) {
	timeWindow, err := utils.CandleDurationFromString(aggTbk.GetItemInCategory("Timeframe"))
	if err != nil {
		return nil, fmt.Errorf("timeframe not found from aggTbk=%v: %w", aggTbk, err)
	}

	suffix := fmt.Sprintf("/%s/%s", models.TradeTimeframe, models.TradeSuffix)
	if strings.HasSuffix(baseTbk.GetItemKey(), suffix) {
		// Ticks to bars
		trades, err := convertCSToTrades(cs, symbol)
		if err != nil {
			return nil, err
		}

		bar, err := models.FromTrades(trades, symbol, timeWindow.String)
		if err != nil {
			return nil, fmt.Errorf("get bar for ondiskagg: %w", err)
		}
		cs2 := bar.GetCs()

		return cs2, nil
	}
	// bars to bars
	params := getParams(cs.Exists("Volume"))

	accumGroup := newAccumGroup(cs, params)

	ts, _ := cs.GetTime()
	outEpoch := make([]int64, 0)

	groupKey := timeWindow.Truncate(ts[0])
	groupStart := 0
	// accumulate inputs.  Since the input is ordered by
	// time, it is just to slice by correct boundaries
	for i, t := range ts {
		if !timeWindow.IsWithin(t, groupKey) {
			// Emit new row and re-init aggState
			outEpoch = append(outEpoch, groupKey.Unix())
			if err := accumGroup.apply(groupStart, i); err != nil {
				return nil, fmt.Errorf("apply to group. groupStart=%d, i=%d:%w", groupStart, i, err)
			}
			groupKey = timeWindow.Truncate(t)
			groupStart = i
		}
	}
	// accumulate any remaining values if not yet
	outEpoch = append(outEpoch, groupKey.Unix())
	if err := accumGroup.apply(groupStart, len(ts)); err != nil {
		return nil, fmt.Errorf("apply to group. groupStart=%d, i=%d:%w", groupStart, len(ts), err)
	}

	// finalize output
	outCs := io.NewColumnSeries()
	outCs.AddColumn("Epoch", outEpoch)
	accumGroup.addColumns(outCs)
	return outCs, nil
}

func (s *OnDiskAggTrigger) query(
	tbk *io.TimeBucketKey,
	window *utils.CandleDuration,
	head, tail time.Time,
) (*io.ColumnSeriesMap, error) {
	cDir := executor.ThisInstance.CatalogDir

	start := window.Truncate(head)

	// TODO: adding 1 second is not needed once we support "<" operator
	end := window.Ceil(tail).Add(-time.Second)

	// Scan
	qs := frontend.NewQueryService(cDir)
	csm, err := qs.ExecuteQuery(tbk, start, end, 0, false, nil)
	if err != nil {
		return nil, err
	}

	return &csm, nil
}

// lockFor returns the mutex guarding aggregation state for a source bucket,
// creating it on first use.
func (s *OnDiskAggTrigger) lockFor(key string) *sync.Mutex {
	v, _ := s.keyLocks.LoadOrStore(key, &sync.Mutex{})
	mu, _ := v.(*sync.Mutex)
	return mu
}
