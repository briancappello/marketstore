// Package sessionfacts stores per-symbol, per-trading-date session facts
// (each session's volume, last close and bar count) as derived data in the
// <SYMBOL>/1D/SESSIONS bucket.
//
// The facts follow the derived-data rules in the watchlist session-facts
// spec (openspec/changes/watchlist-sessions/specs/watchlist/session-facts):
//
//   - Facts, not statistics: only per-date values are stored. Medians and
//     other window statistics are computed by readers.
//   - Rebuildable: 1Min bars are the source of truth, and Compute is the only
//     place a row is derived. The live job, the late-data recompute, the
//     rebuild command and the read fallback all call it, so they agree.
//   - Versioned: every row records Version, and readers treat any other
//     version as missing.
//
// The package is deliberately self-contained (definition, version, compute,
// read, write) so it can later move into a general derived-series framework.
package sessionfacts

import (
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
)

// Version is the current definition version. Bump it whenever Compute's
// output changes for the same bars; rows written with another version are
// then treated as missing and recomputed.
const Version int32 = 1

// Bucket naming: <SYMBOL>/1D/SESSIONS.
const (
	Timeframe      = "1D"
	AttributeGroup = "SESSIONS"
)

// Key returns the bucket key for symbol.
func Key(symbol string) string { return symbol + "/" + Timeframe + "/" + AttributeGroup }

// SourceKey returns the 1Min bucket the facts are derived from.
func SourceKey(symbol string) string { return symbol + "/1Min/OHLCV" }

// Row is one symbol's facts for one trading date.
type Row struct {
	// Date is midnight of the trading date in America/New_York.
	Date time.Time

	PreVolume, RegVolume, PostVolume int64
	// PreClose, RegClose and PostClose are the closes of each session's last
	// 1Min bar. They are meaningful only when the matching *Bars is > 0.
	PreClose, RegClose, PostClose float64
	// PreBars, RegBars and PostBars count each session's 1Min bars. Zero
	// means the session had no bars, which a reader must tell apart from a
	// row that does not exist (not computed yet).
	PreBars, RegBars, PostBars int32

	Version int32
}

// Current reports whether r was computed with the current definition.
func (r Row) Current() bool { return r.Version == Version }

// Volume returns session s's volume.
func (r Row) Volume(s calendar.Session) int64 {
	switch s {
	case calendar.Premarket:
		return r.PreVolume
	case calendar.Regular:
		return r.RegVolume
	default:
		return r.PostVolume
	}
}

// Close returns session s's last close, and false when the session had no
// bars.
func (r Row) Close(s calendar.Session) (float64, bool) {
	switch s {
	case calendar.Premarket:
		return r.PreClose, r.PreBars > 0
	case calendar.Regular:
		return r.RegClose, r.RegBars > 0
	default:
		return r.PostClose, r.PostBars > 0
	}
}

// Bars returns session s's bar count.
func (r Row) Bars(s calendar.Session) int32 {
	switch s {
	case calendar.Premarket:
		return r.PreBars
	case calendar.Regular:
		return r.RegBars
	default:
		return r.PostBars
	}
}

// Bar is the part of a 1Min bar the facts use.
type Bar struct {
	Epoch  int64
	Close  float64
	Volume int64
}

// Compute derives the row for the trading day ds from its 1Min bars. Bars
// outside the day's sessions are ignored; bars may come in any order. When
// two bars share an epoch, the later one in the slice wins, matching an
// overwrite on disk.
func Compute(ds calendar.DaySessions, bars []Bar) Row {
	type acc struct {
		volume    int64
		close     float64
		lastEpoch int64
		bars      int32
	}
	var accs [len(calendar.Sessions)]acc

	// Collapse duplicate epochs first so a repeated bar counts once.
	latest := make(map[int64]Bar, len(bars))
	for _, b := range bars {
		latest[b.Epoch] = b
	}
	for epoch, b := range latest {
		s, ok := ds.SessionAt(time.Unix(epoch, 0))
		if !ok {
			continue
		}
		a := &accs[s]
		a.volume += b.Volume
		a.bars++
		if a.bars == 1 || epoch > a.lastEpoch {
			a.close = b.Close
			a.lastEpoch = epoch
		}
	}

	pre, reg, post := accs[calendar.Premarket], accs[calendar.Regular], accs[calendar.Afterhours]
	return Row{
		Date:      ds.Date,
		PreVolume: pre.volume, RegVolume: reg.volume, PostVolume: post.volume,
		PreClose: pre.close, RegClose: reg.close, PostClose: post.close,
		PreBars: pre.bars, RegBars: reg.bars, PostBars: post.bars,
		Version: Version,
	}
}
