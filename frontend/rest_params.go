package frontend

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// defaultBarLimit is returned when no limit is supplied. At this value
	// a 1D request covers roughly six years, which also satisfies a
	// 1000-period indicator lookback with room to spare.
	defaultBarLimit = 1500
	// maxBarLimit bounds the response. Because /v1/bars is single-symbol,
	// this alone bounds the endpoint; no second cap is required.
	maxBarLimit = 10000

	defaultTimeframe = "1D"
	// attributeGroup is fixed. The REST surface intentionally exposes
	// neither the attribute group nor a column selector.
	attributeGroup = "OHLCV"
)

// parseLimit resolves the limit parameter. An empty value yields the
// default. Exceeding maxBarLimit is an error rather than a silent clamp, so
// a caller always knows whether their request was satisfied in full.
func parseLimit(raw string) (int, error) {
	if raw == "" {
		return defaultBarLimit, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("limit must be an integer, got %q", raw)
	}
	if n < 1 {
		return 0, fmt.Errorf("limit must be at least 1, got %d", n)
	}
	if n > maxBarLimit {
		return 0, fmt.Errorf("limit must not exceed %d, got %d", maxBarLimit, n)
	}
	return n, nil
}

// parseTimeBound accepts unix epoch seconds or RFC3339, disambiguated by
// shape: a value of digits only (with an optional leading minus) is epoch
// seconds, anything else is RFC3339. An empty value yields the zero time,
// meaning the bound is unset.
func parseTimeBound(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	if isAllDigits(raw) {
		secs, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid epoch seconds %q", raw)
		}
		return time.Unix(secs, 0).UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"time must be RFC3339 or unix epoch seconds, got %q", raw)
	}
	return t, nil
}

func isAllDigits(s string) bool {
	s = strings.TrimPrefix(s, "-")
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// validateSymbol rejects anything but a single plain symbol.
//
// queryColumnSeries expands a comma-separated list into a multi-symbol
// query and a bare "*" into every symbol in the catalog. Either would
// multiply the per-symbol record limit by an unbounded symbol count, so
// neither is permitted to reach it from this route.
func validateSymbol(sym string) error {
	if sym == "" {
		return errors.New("symbol is required")
	}
	if strings.ContainsAny(sym, ",*/") {
		return fmt.Errorf(
			"symbol must be a single symbol without ',', '*' or '/', got %q", sym)
	}
	return nil
}

// resolveTimeframe applies the default when none is supplied.
func resolveTimeframe(raw string) string {
	if raw == "" {
		return defaultTimeframe
	}
	return raw
}

// isNoDataErr reports whether err is the query layer's "this bucket has no
// records" signal rather than a malformed request. QueryService.ExecuteQuery
// returns a plain formatted error with this prefix when a symbol/timeframe is
// absent, so REST maps it to 404 (bars) or an empty list (quotes) instead of
// a 400. There is no sentinel error to match on, hence the string check.
func isNoDataErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no results returned from query")
}
