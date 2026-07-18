// Package mapping provides the single source of truth for translating Massive
// (Polygon-format) integer codes — trade conditions, tape ids, and exchange ids
// — into the SIP ASCII-char encoding used by MarketStore's models/enum package.
//
// It is shared by the flat-file parser, the REST backfiller, and (in future) the
// live ws feed, so that all ingestion paths encode conditions/exchanges/tape
// identically and consistently with what models.Bar.FromTrades consolidation
// expects.
package mapping

import "github.com/alpacahq/marketstore/v4/models/enum"

// tradeConditionToSIP maps a Massive/Polygon trade-condition modifier (the
// integer codes that appear in the flat-file "conditions" column and the REST
// /v3/trades "conditions" array) to the SIP ASCII character used by
// enum.TradeCondition.
//
// The integer→char mapping follows the Polygon conditions glossary
// (https://polygon.io/glossary/us/stocks/conditions-indicators). Only codes
// that have a corresponding SIP char in models/enum are listed. Codes with no
// SIP mapping (e.g. 41 "Trade Thru Exempt", 42 "NonEligible") are intentionally
// absent: they are functionally inert for bar consolidation
// (models.ConditionToUpdateInfo) and must be dropped rather than stored as a raw
// integer that would alias an unrelated SIP char.
var tradeConditionToSIP = map[int]enum.TradeCondition{
	0:  enum.RegularSale,                // '@'
	1:  enum.Acquisition,                // 'A'
	2:  enum.AveragePriceTrade,          // 'W'
	3:  enum.AutomaticExecution,         // 'E'
	4:  enum.BunchedTrade,               // 'B'
	5:  enum.BunchedSoldTrade,           // 'G'
	7:  enum.CashSale,                   // 'C'
	8:  enum.ClosingPrints,              // '6'
	9:  enum.CrossTrade,                 // 'X'
	10: enum.DerivativelyPriced,         // '4'
	11: enum.Distribution,               // 'D'
	12: enum.FormT,                      // 'T'
	13: enum.ExtendedHoursTrade,         // 'T' (Extended Trading Hours / Sold Out of Sequence)
	14: enum.IntermarketSweep,           // 'F'
	15: enum.MarketCenterOfficialClose,  // 'M'
	16: enum.MarketCenterOfficialOpen,   // 'Q'
	20: enum.NextDay,                    // 'N'
	21: enum.PriceVariationTrade,        // 'H'
	22: enum.PriorReferencePrice,        // 'P'
	23: enum.Rule155Trade,               // 'K'
	25: enum.OpeningPrints,              // 'O'
	27: enum.StoppedStock,               // '1'
	28: enum.ReopeningPrints,            // '5'
	29: enum.Seller,                     // 'R'
	30: enum.SoldLast,                   // 'L'
	33: enum.SoldOutOfSequence,          // 'Z'
	34: enum.SplitTrade,                 // 'S'
	36: enum.YellowFlagRegularTrade,     // 'Y'
	37: enum.OddLotTrade,                // 'I'
	38: enum.CorrectedConsolidatedClose, // '9'
	52: enum.ContingentTrade,            // 'V'
	53: enum.QualifiedContingentTrade,   // '7'
	59: enum.PlaceholderFor611Exempt,    // '8'
}

// TradeConditionToSIP maps a Massive trade-condition modifier (int) to the SIP
// ASCII char (enum.TradeCondition). ok is false for codes that have no SIP
// mapping; callers should drop such codes.
func TradeConditionToSIP(code int) (enum.TradeCondition, bool) {
	c, ok := tradeConditionToSIP[code]
	return c, ok
}

// Tape ASCII chars per CTA/UTP plans: A=NYSE, B=NYSE (regional), C=Nasdaq.
const (
	tapeNYSE     = 1
	tapeNYSERegl = 2
	tapeNasdaq   = 3
)

// TapeToChar maps a Massive integer tape (1/2/3) to the SIP enum.Tape char
// ('A'/'B'/'C'). Unknown tape values map to enum.UndefinedTape.
func TapeToChar(tape int) enum.Tape {
	switch tape {
	case tapeNYSE:
		return enum.TapeA
	case tapeNYSERegl:
		return enum.TapeB
	case tapeNasdaq:
		return enum.TapeC
	default:
		return enum.UndefinedTape
	}
}
