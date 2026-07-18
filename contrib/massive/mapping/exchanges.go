package mapping

import (
	"net/http"

	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// staticExchangeFallback maps a Massive/Polygon exchange id to the SIP
// participant-id ASCII char (enum.Exchange). It is the source of truth whenever
// the live /v3/reference/exchanges call fails, so it must be populated and kept
// current — it is NOT a placeholder.
//
// Transcribed from /v3/reference/exchanges (rows of type "exchange" or "TRF";
// SIP rows are skipped). The integer ids are stable Polygon identifiers; the
// char is the value of the row's "participant_id" field. Verified against the
// 2026-06-18 flat-file sample (ids 4, 8, 11, 12, 19, 21 and others present).
var staticExchangeFallback = map[int]enum.Exchange{
	1:  enum.NYSEAmerican,      // 'A' - NYSE American (AMEX)
	2:  enum.NasdaqOMXBX,       // 'B' - Nasdaq OMX BX
	3:  enum.NYSENational,      // 'C' - NYSE National
	4:  enum.FinraADF,          // 'D' - FINRA ADF
	5:  enum.MarketIndependent, // 'E' - Market Independent
	6:  enum.ISE,               // 'I' - International Securities Exchange
	7:  enum.CboeEDGA,          // 'J' - Cboe EDGA
	8:  enum.CboeEDGX,          // 'K' - Cboe EDGX
	9:  enum.NYSEChicago,       // 'M' - NYSE Chicago
	10: enum.NYSE,              // 'N' - New York Stock Exchange
	11: enum.NYSEArca,          // 'P' - NYSE Arca
	12: enum.Nasdaq,            // 'T'/'Q' - Nasdaq
	13: enum.CQS,               // 'S' - Consolidated Quote System
	14: enum.NasdaqOMX,         // 'Q' - Nasdaq OMX
	15: enum.IEX,               // 'V' - Investors Exchange
	16: enum.CBSX,              // 'W' - CBOE Stock Exchange
	17: enum.NasdaqOMXPSX,      // 'X' - Nasdaq PSX
	18: enum.CboeBYX,           // 'Y' - Cboe BYX
	19: enum.CboeBZX,           // 'Z' - Cboe BZX
	20: enum.MIAX,              // 'H' - MIAX
	21: enum.MEMX,              // 'U' - MEMX
	62: enum.LTSE,              // 'L' - Long-Term Stock Exchange
}

// ExchangeMap resolves Massive exchange ids to SIP enum.Exchange chars. It is
// built once per run (LoadExchangeMap) and read concurrently thereafter, so it
// is treated as immutable after construction.
type ExchangeMap struct {
	m map[int]enum.Exchange
}

// LoadExchangeMap fetches /v3/reference/exchanges and builds an id→participant_id
// (SIP char) map. On any error it logs a warning and falls back to the static
// table, so the returned *ExchangeMap is always usable and non-nil.
func LoadExchangeMap(client *http.Client) *ExchangeMap {
	exchanges, err := api.ListExchanges(client)
	if err != nil {
		log.Warn("[massive] failed to fetch exchanges, using static fallback table: %v", err)
		return &ExchangeMap{m: staticExchangeFallback}
	}

	m := make(map[int]enum.Exchange, len(exchanges))
	for _, ex := range exchanges {
		// SIP rows have no participant_id and are not real trading venues.
		if ex.ParticipantID == "" {
			continue
		}
		m[ex.ID] = enum.Exchange(ex.ParticipantID[0])
	}

	if len(m) == 0 {
		log.Warn("[massive] /v3/reference/exchanges returned no usable rows, using static fallback table")
		return &ExchangeMap{m: staticExchangeFallback}
	}

	// Backfill any ids missing from the API response from the static table so
	// we never regress below the known-good set.
	for id, c := range staticExchangeFallback {
		if _, ok := m[id]; !ok {
			m[id] = c
		}
	}

	log.Info("[massive] loaded %d exchanges from /v3/reference/exchanges", len(m))
	return &ExchangeMap{m: m}
}

// StaticExchangeMap returns an ExchangeMap backed solely by the static fallback
// table. Useful for tests and offline operation.
func StaticExchangeMap() *ExchangeMap {
	return &ExchangeMap{m: staticExchangeFallback}
}

// Get returns the SIP enum.Exchange char for a Massive exchange id, or
// enum.UndefinedExchange if the id is unknown.
func (e *ExchangeMap) Get(id int) enum.Exchange {
	if e == nil || e.m == nil {
		return enum.UndefinedExchange
	}
	if c, ok := e.m[id]; ok {
		return c
	}
	return enum.UndefinedExchange
}
