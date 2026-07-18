package mapping_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/contrib/massive/mapping"
	"github.com/alpacahq/marketstore/v4/models/enum"
)

func TestTradeConditionToSIP(t *testing.T) {
	t.Parallel()

	cases := []struct {
		code int
		want enum.TradeCondition
		ok   bool
	}{
		{0, enum.RegularSale, true},
		{1, enum.Acquisition, true},
		{2, enum.AveragePriceTrade, true},
		{8, enum.ClosingPrints, true},
		{9, enum.CrossTrade, true},
		{10, enum.DerivativelyPriced, true},
		{11, enum.Distribution, true},
		{12, enum.FormT, true},
		{14, enum.IntermarketSweep, true},
		{37, enum.OddLotTrade, true},
		// Unmapped codes must be dropped.
		{41, 0, false}, // Trade Thru Exempt
		{42, 0, false}, // NonEligible
		{9999, 0, false},
	}
	for _, c := range cases {
		got, ok := mapping.TradeConditionToSIP(c.code)
		assert.Equal(t, c.ok, ok, "code %d ok", c.code)
		if c.ok {
			assert.Equal(t, c.want, got, "code %d value", c.code)
		}
	}
}

func TestTapeToChar(t *testing.T) {
	t.Parallel()
	assert.Equal(t, enum.TapeA, mapping.TapeToChar(1))
	assert.Equal(t, enum.TapeB, mapping.TapeToChar(2))
	assert.Equal(t, enum.TapeC, mapping.TapeToChar(3))
	assert.Equal(t, enum.UndefinedTape, mapping.TapeToChar(0))
	assert.Equal(t, enum.UndefinedTape, mapping.TapeToChar(99))
}

func TestStaticExchangeMap(t *testing.T) {
	t.Parallel()
	em := mapping.StaticExchangeMap()

	// Ids observed in the 2026-06-18 sample must be covered.
	for _, id := range []int{4, 8, 11, 12, 19, 21} {
		assert.NotEqual(t, enum.UndefinedExchange, em.Get(id), "exchange id %d should be mapped", id)
	}

	// Spot-check specific SIP chars.
	assert.Equal(t, enum.NYSE, em.Get(10))
	assert.Equal(t, enum.NYSEArca, em.Get(11))
	assert.Equal(t, enum.CboeBZX, em.Get(19))

	// Unknown id -> undefined.
	assert.Equal(t, enum.UndefinedExchange, em.Get(99999))
}

func TestNilExchangeMapGet(t *testing.T) {
	t.Parallel()
	var em *mapping.ExchangeMap
	assert.Equal(t, enum.UndefinedExchange, em.Get(1))
}
