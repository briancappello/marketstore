package sessionfacts

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
)

// SourceSymbols lists the symbols that have a 1Min OHLCV bucket, sorted.
func SourceSymbols(catDir *catalog.Directory) ([]string, error) {
	infos, err := catDir.GatherTimeBucketInfo()
	if err != nil {
		return nil, fmt.Errorf("gather buckets: %w", err)
	}
	seen := map[string]struct{}{}
	for _, tbi := range infos {
		// Path ends with SYMBOL/1Min/OHLCV/YEAR.bin.
		parts := strings.Split(tbi.Path, "/")
		if len(parts) < 4 || parts[len(parts)-3] != "1Min" || parts[len(parts)-2] != "OHLCV" {
			continue
		}
		seen[parts[len(parts)-4]] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out, nil
}

// TradingDays returns the trading days from..to (inclusive, by date in
// America/New_York), skipping weekends and holidays.
func TradingDays(from, to time.Time) []calendar.DaySessions {
	tz := calendar.Nasdaq.Tz()
	y, m, d := from.In(tz).Date()
	day := time.Date(y, m, d, 0, 0, 0, 0, tz)
	ty, tm, td := to.In(tz).Date()
	last := time.Date(ty, tm, td, 0, 0, 0, 0, tz)
	var out []calendar.DaySessions
	for !day.After(last) {
		if ds, err := calendar.Nasdaq.SessionBounds(day.Date()); err == nil {
			out = append(out, ds)
		}
		day = day.AddDate(0, 0, 1)
	}
	return out
}

// RebuildProgress is called after each batch of symbols with the number of
// symbols done and the total.
type RebuildProgress func(done, total int)

// Rebuild recomputes and writes the facts for symbols on every trading day
// from..to. An empty symbols list means every symbol with 1Min data. It is
// idempotent: rerunning over unchanged bars writes identical rows.
func (s *Service) Rebuild(symbols []string, from, to time.Time, progress RebuildProgress) error {
	if !s.Leader() {
		return ErrReplica
	}
	days := TradingDays(from, to)
	if len(days) == 0 {
		return fmt.Errorf("no trading days between %s and %s",
			from.Format("2006-01-02"), to.Format("2006-01-02"))
	}
	if len(symbols) == 0 {
		var err error
		if symbols, err = SourceSymbols(s.cfg.CatalogDir()); err != nil {
			return err
		}
	}
	const batch = 100
	for i := 0; i < len(symbols); i += batch {
		end := i + batch
		if end > len(symbols) {
			end = len(symbols)
		}
		entries := make([]Entry, 0, (end-i)*len(days))
		for _, sym := range symbols[i:end] {
			for _, d := range days {
				entries = append(entries, Entry{Symbol: sym, Date: d.Date})
			}
		}
		if err := s.Recompute(entries); err != nil {
			return err
		}
		if progress != nil {
			progress(end, len(symbols))
		}
	}
	return nil
}
