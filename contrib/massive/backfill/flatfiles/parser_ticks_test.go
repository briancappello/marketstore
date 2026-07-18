package flatfiles

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/contrib/massive/mapping"
	"github.com/alpacahq/marketstore/v4/models/enum"
	utilsio "github.com/alpacahq/marketstore/v4/utils/io"
)

func TestParseConditionInts(t *testing.T) {
	t.Parallel()
	assert.Nil(t, parseConditionInts(""))
	assert.Equal(t, []int{14, 12, 37, 41}, parseConditionInts("14,12,37,41"))
	assert.Equal(t, []int{1}, parseConditionInts(" 1 "))
	assert.Equal(t, []int{2, 3}, parseConditionInts("2,,3"))
}

func TestParseTradesStream(t *testing.T) {
	t.Parallel()
	// Two tickers; quoted comma-separated conditions; fractional + empty
	// conditions; correction column present.
	csv := strings.Join([]string{
		"ticker,conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp",
		`AAA,"0,37",0,11,x,0,100.5,1,1700000000000000001,0.9,1,0,0`,
		`AAA,,0,11,x,0,100.6,2,1700000000000000002,10,1,0,0`,
		`BBB,"14",1,12,x,0,50.25,3,1700000000000000003,5,3,0,0`,
	}, "\n")

	em := mapping.StaticExchangeMap()
	emitted := map[string]utilsio.ColumnSeriesMap{}
	stats, err := ParseTradesStream(strings.NewReader(csv), nil, em,
		time.Date(2023, 11, 14, 0, 0, 0, 0, time.UTC),
		func(csm utilsio.ColumnSeriesMap) {
			for tbk := range csm {
				emitted[tbk.GetItemInCategory("Symbol")] = csm
			}
		})
	assert.Nil(t, err)
	assert.Equal(t, 3, stats.RowsRead)
	assert.Equal(t, 3, stats.RowsMatched)
	assert.Equal(t, 2, stats.SymbolCount)

	// AAA: two rows.
	aaa := emitted["AAA"]
	var aaaCs = csForSymbol(t, aaa, "AAA")
	sizes, ok := aaaCs.GetColumn("Size").([]float64)
	assert.True(t, ok)
	assert.Equal(t, []float64{0.9, 10}, sizes)

	// First AAA row condition 0 -> '@' RegularSale; 37 -> OddLotTrade.
	cond1, ok := aaaCs.GetColumn("Cond1").([]enum.TradeCondition)
	assert.True(t, ok, "Cond1 column should be []enum.TradeCondition")
	assert.Equal(t, enum.RegularSale, cond1[0])
	cond2, _ := aaaCs.GetColumn("Cond2").([]enum.TradeCondition)
	assert.Equal(t, enum.OddLotTrade, cond2[0])
	// Second AAA row: empty conditions -> NoTradeCondition.
	assert.Equal(t, enum.NoTradeCondition, cond1[1])

	// BBB: correction 1, tape 3 -> 'C'.
	bbbCs := csForSymbol(t, emitted["BBB"], "BBB")
	corr, _ := bbbCs.GetColumn("Correction").([]byte)
	assert.Equal(t, []byte{1}, corr)
	tape, _ := bbbCs.GetColumn("TapeID").([]enum.Tape)
	assert.Equal(t, enum.TapeC, tape[0])
}

func TestParseTradesStreamSymbolFilter(t *testing.T) {
	t.Parallel()
	csv := strings.Join([]string{
		"ticker,conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp",
		`AAA,,0,11,x,0,1.0,1,1700000000000000001,1,1,0,0`,
		`BBB,,0,11,x,0,2.0,2,1700000000000000002,2,1,0,0`,
	}, "\n")

	em := mapping.StaticExchangeMap()
	var symbols []string
	_, err := ParseTradesStream(strings.NewReader(csv), map[string]bool{"BBB": true}, em,
		time.Now(), func(csm utilsio.ColumnSeriesMap) {
			for tbk := range csm {
				symbols = append(symbols, tbk.GetItemInCategory("Symbol"))
			}
		})
	assert.Nil(t, err)
	assert.Equal(t, []string{"BBB"}, symbols)
}

func TestParseQuotesStreamSizeNormalization(t *testing.T) {
	t.Parallel()
	// Row dated before the cutoff (sip ts ~2025-10-01) uses round-lot units;
	// row on/after cutoff (sip ts ~2025-12-01) uses shares.
	beforeNs := time.Date(2025, 10, 1, 12, 0, 0, 5, time.UTC).UnixNano()
	afterNs := time.Date(2025, 12, 1, 12, 0, 0, 7, time.UTC).UnixNano()
	csv := strings.Join([]string{
		"ticker,ask_exchange,ask_price,ask_size,bid_exchange,bid_price,bid_size,conditions,indicators,participant_timestamp,sequence_number,sip_timestamp,tape,trf_timestamp",
		mkQuoteRow("AAA", 12, 10.5, 3, 11, 10.0, 2, "1,2", "5", beforeNs),
		mkQuoteRow("AAA", 12, 10.6, 4, 11, 10.1, 1, "", "", afterNs),
	}, "\n")

	em := mapping.StaticExchangeMap()
	roundLot := func(string) int { return 100 }
	var cs *utilsio.ColumnSeries
	stats, err := ParseQuotesStream(strings.NewReader(csv), nil, em, roundLot,
		time.Now(), func(csm utilsio.ColumnSeriesMap) {
			for tbk := range csm {
				c := csm[tbk]
				cs = c
			}
		})
	assert.Nil(t, err)
	assert.Equal(t, 2, stats.RowsMatched)

	bidSize, _ := cs.GetColumn("BidSize").([]uint64)
	// Before cutoff: 2 * 100 = 200 shares. After cutoff: 1 share.
	assert.Equal(t, []uint64{200, 1}, bidSize)
	askSize, _ := cs.GetColumn("AskSize").([]uint64)
	assert.Equal(t, []uint64{300, 4}, askSize)

	// Conditions/indicators stored raw; empty -> 0.
	cond, _ := cs.GetColumn("Cond").([]byte)
	assert.Equal(t, []byte{1, 0}, cond)
	cond2, _ := cs.GetColumn("Cond2").([]byte)
	assert.Equal(t, []byte{2, 0}, cond2)
	ind, _ := cs.GetColumn("Indicators").([]byte)
	assert.Equal(t, []byte{5, 0}, ind)
}

func TestIsTickKey(t *testing.T) {
	t.Parallel()
	assert.True(t, IsTickKey("trades"))
	assert.True(t, IsTickKey("quotes"))
	assert.False(t, IsTickKey("1D"))
	assert.False(t, IsTickKey("1Min"))
	assert.False(t, IsTickKey("nope"))
}

// --- helpers ---

func csForSymbol(t *testing.T, csm utilsio.ColumnSeriesMap, symbol string) *utilsio.ColumnSeries {
	t.Helper()
	for tbk := range csm {
		if tbk.GetItemInCategory("Symbol") == symbol {
			return csm[tbk]
		}
	}
	t.Fatalf("symbol %s not found in CSM", symbol)
	return nil
}

func mkQuoteRow(ticker string, askEx int, askPx float64, askSz int, bidEx int, bidPx float64, bidSz int,
	conditions, indicators string, sipNs int64,
) string {
	return strings.Join([]string{
		ticker,
		strconv.Itoa(askEx), strconv.FormatFloat(askPx, 'f', -1, 64), strconv.Itoa(askSz),
		strconv.Itoa(bidEx), strconv.FormatFloat(bidPx, 'f', -1, 64), strconv.Itoa(bidSz),
		quoteField(conditions), quoteField(indicators),
		"0", "0", strconv.FormatInt(sipNs, 10), "0", "0",
	}, ",")
}

// quoteField wraps a comma-containing condition/indicator list in double quotes
// so the std csv writer/reader treats it as a single field.
func quoteField(s string) string {
	if strings.Contains(s, ",") {
		return `"` + s + `"`
	}
	return s
}
