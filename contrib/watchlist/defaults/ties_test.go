package defaults_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/watchlist/defaults"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework"
)

// Tied entries come out in symbol order every time. The curated map's
// iteration order is random, so without a tie-break a restart or a rewind
// could order ties differently from the live ranking.
func TestTiesAreOrderedBySymbol(t *testing.T) {
	factories := map[string]func(map[string]interface{}) (framework.WatchlistStrategy, error){
		"PCT_CHANGE_UP": defaults.NewPctChangeUp, "PCT_CHANGE_DOWN": defaults.NewPctChangeDown,
		"VOLUME_UP": defaults.NewVolumeUp, "VOLUME_DOWN": defaults.NewVolumeDown,
	}
	for name, f := range factories {
		s, err := f(map[string]interface{}{})
		require.NoError(t, err)
		for run := 0; run < 50; run++ {
			curated := map[string]*framework.SymbolState{}
			for _, sym := range []string{"EEE", "CCC", "AAA", "DDD", "BBB"} {
				st := framework.NewSymbolState()
				st.PctChange, st.CumulativeVolume = 2.5, 1_000
				if name == "PCT_CHANGE_DOWN" || name == "VOLUME_DOWN" {
					st.PctChange = -2.5
				}
				curated[sym] = st
			}
			var got []string
			for _, r := range s.Rank(curated) {
				got = append(got, r.Symbol)
			}
			if !assert.Equal(t, []string{"AAA", "BBB", "CCC", "DDD", "EEE"}, got, "%s run %d", name, run) {
				break
			}
		}
	}
}
