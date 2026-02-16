package backfill

import (
	"fmt"
	"net/http"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/models"
	modelsenum "github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const millisToSec = 1000

// NY is the New York timezone used for market-day calculations.
var NY, _ = time.LoadLocation("America/New_York")

// Bars fetches historical bar aggregates from the Massive REST API
// and writes them to MarketStore. The timeframe parameter specifies the
// bar frequency (e.g., "1Min", "5Min", "1H", "1D").
func Bars(
	client *http.Client,
	symbol string,
	timeframe string,
	from, to time.Time,
	limit int,
	adjusted bool,
	writerWP *worker.Pool,
) error {
	if from.IsZero() {
		from = time.Date(2014, 1, 1, 0, 0, 0, 0, NY)
	}
	if to.IsZero() {
		to = time.Now()
	}

	// Convert MarketStore timeframe to API timespan and multiplier
	apiTimespan, multiplier, err := timeframeToAPI(timeframe)
	if err != nil {
		return fmt.Errorf("invalid timeframe %q: %w", timeframe, err)
	}

	resp, err := api.GetHistoricAggregates(client, symbol, apiTimespan, multiplier, from, to, limit, adjusted)
	if err != nil {
		return err
	}

	if len(resp.Results) == 0 {
		return nil
	}

	model := models.NewBar(symbol, timeframe, len(resp.Results))
	for _, bar := range resp.Results {
		epoch := bar.EpochMilliseconds / millisToSec
		ts := time.Unix(epoch, 0)
		if ts.After(to) || ts.Before(from) {
			continue
		}
		model.Add(epoch,
			modelsenum.Price(bar.Open),
			modelsenum.Price(bar.High),
			modelsenum.Price(bar.Low),
			modelsenum.Price(bar.Close),
			modelsenum.Size(bar.Volume),
		)
	}

	writerWP.Do(func() {
		if err := model.Write(); err != nil {
			log.Error("[massive] failed to write %s bars for %s: %v", timeframe, symbol, err)
		}
	})

	return nil
}

// timeframeToAPI converts a MarketStore timeframe string (e.g., "1Min", "5Min", "1H", "1D")
// to Massive API timespan and multiplier values.
func timeframeToAPI(timeframe string) (apiTimespan string, multiplier int, err error) {
	// Validate the timeframe using CandleDurationFromString
	_, err = utils.CandleDurationFromString(timeframe)
	if err != nil {
		return "", 0, err
	}

	// Parse the timeframe string to extract multiplier and suffix
	// Timeframe format is: <number><suffix> (e.g., "1Min", "5Min", "1H", "1D")
	suffixes := []struct {
		suffix      string
		apiTimespan string
	}{
		{"Sec", "second"},
		{"Min", "minute"},
		{"H", "hour"},
		{"D", "day"},
		{"W", "week"},
		{"M", "month"},
		{"Y", "year"},
	}

	for _, s := range suffixes {
		if len(timeframe) > len(s.suffix) && timeframe[len(timeframe)-len(s.suffix):] == s.suffix {
			numStr := timeframe[:len(timeframe)-len(s.suffix)]
			var n int
			if _, err := fmt.Sscanf(numStr, "%d", &n); err != nil {
				return "", 0, fmt.Errorf("invalid multiplier in timeframe %q: %w", timeframe, err)
			}
			return s.apiTimespan, n, nil
		}
	}

	return "", 0, fmt.Errorf("unsupported timeframe: %s", timeframe)
}

// Trades fetches historical tick-level trades from the Massive REST API
// for each market day in the from/to range and writes them to MarketStore.
func Trades(
	client *http.Client,
	symbol string,
	from, to time.Time,
	limit int,
	writerWP *worker.Pool,
) error {
	const hoursInDay = 24

	allTrades := make([]api.TradeTick, 0)
	for date := from; to.After(date); date = date.Add(hoursInDay * time.Hour) {
		if !calendar.Nasdaq.IsMarketDay(date) {
			continue
		}
		resp, err := api.GetHistoricTrades(client, symbol, date, limit)
		if err != nil {
			return err
		}
		allTrades = append(allTrades, resp.Results...)
	}

	if len(allTrades) == 0 {
		return nil
	}

	model := models.NewTrade(symbol, len(allTrades))
	for i := range allTrades {
		tick := &allTrades[i]
		timestamp := time.Unix(0, tick.SIPTimestamp)

		// Convert Massive condition codes to MarketStore enum values.
		conditions := make([]modelsenum.TradeCondition, len(tick.Conditions))
		for j, c := range tick.Conditions {
			conditions[j] = modelsenum.TradeCondition(c)
		}

		model.Add(
			timestamp.Unix(), timestamp.Nanosecond(),
			modelsenum.Price(tick.Price),
			modelsenum.Size(tick.Size),
			modelsenum.Exchange(tick.Exchange),
			modelsenum.Tape(tick.Tape),
			conditions...,
		)
	}

	writerWP.Do(func() {
		if err := model.Write(); err != nil {
			log.Error("[massive] failed to write trades for %s: %v", symbol, err)
		}
	})

	return nil
}

// Quotes fetches historical NBBO quotes from the Massive REST API
// for each market day in the from/to range and writes them to MarketStore.
func Quotes(
	client *http.Client,
	symbol string,
	from, to time.Time,
	limit int,
	writerWP *worker.Pool,
) error {
	const hoursInDay = 24

	allQuotes := make([]api.QuoteTick, 0)
	for date := from; to.After(date); date = date.Add(hoursInDay * time.Hour) {
		if !calendar.Nasdaq.IsMarketDay(date) {
			continue
		}
		resp, err := api.GetHistoricQuotes(client, symbol, date, limit)
		if err != nil {
			return err
		}
		allQuotes = append(allQuotes, resp.Results...)
	}

	if len(allQuotes) == 0 {
		return nil
	}

	model := models.NewQuote(symbol, len(allQuotes))
	for _, tick := range allQuotes {
		timestamp := time.Unix(0, tick.SIPTimestamp)

		var cond modelsenum.QuoteCondition
		if len(tick.Conditions) > 0 {
			cond = modelsenum.QuoteCondition(tick.Conditions[0])
		}

		model.Add(
			timestamp.Unix(), timestamp.Nanosecond(),
			tick.BidPrice, tick.AskPrice,
			int(tick.BidSize), int(tick.AskSize),
			modelsenum.Exchange(tick.BidExchange),
			modelsenum.Exchange(tick.AskExchange),
			cond,
		)
	}

	writerWP.Do(func() {
		if err := model.Write(); err != nil {
			log.Error("[massive] failed to write quotes for %s: %v", symbol, err)
		}
	})

	return nil
}
