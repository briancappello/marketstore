package subscription_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/massive/subscription"
)

func drain(ch <-chan subscription.Change) []subscription.Change {
	var out []subscription.Change
	for {
		select {
		case c := <-ch:
			out = append(out, c)
		default:
			return out
		}
	}
}

func TestParseDataTypeRoundTrip(t *testing.T) {
	for _, dt := range subscription.AllDataTypes {
		parsed, ok := subscription.ParseDataType(dt.String())
		require.True(t, ok)
		assert.Equal(t, dt, parsed)
	}
	_, ok := subscription.ParseDataType("bogus")
	assert.False(t, ok)
}

func TestAcquireReleaseTransitions(t *testing.T) {
	m := subscription.New(0)
	ch := m.Changes()

	// 0->1 emits Active=true.
	n, err := m.Acquire("AAPL", subscription.Trades)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	// 1->2 emits nothing.
	n, err = m.Acquire("AAPL", subscription.Trades)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	changes := drain(ch)
	require.Len(t, changes, 1)
	assert.Equal(t, subscription.Change{Symbol: "AAPL", DataType: subscription.Trades, Active: true}, changes[0])

	// 2->1 emits nothing.
	assert.Equal(t, 1, m.Release("AAPL", subscription.Trades))
	assert.Empty(t, drain(ch))

	// 1->0 emits Active=false.
	assert.Equal(t, 0, m.Release("AAPL", subscription.Trades))
	changes = drain(ch)
	require.Len(t, changes, 1)
	assert.False(t, changes[0].Active)

	// Release on absent symbol is a no-op.
	assert.Equal(t, 0, m.Release("AAPL", subscription.Trades))
	assert.Empty(t, drain(ch))
}

func TestPerDataTypeCap(t *testing.T) {
	m := subscription.New(2)
	_ = m.Changes()

	_, err := m.Acquire("A", subscription.Trades)
	require.NoError(t, err)
	_, err = m.Acquire("B", subscription.Trades)
	require.NoError(t, err)

	// Third distinct trade symbol exceeds the cap.
	_, err = m.Acquire("C", subscription.Trades)
	assert.Error(t, err)

	// A bump on an existing symbol never trips the cap.
	_, err = m.Acquire("A", subscription.Trades)
	assert.NoError(t, err)

	// Quotes has its own independent cap.
	_, err = m.Acquire("A", subscription.Quotes)
	require.NoError(t, err)
	_, err = m.Acquire("B", subscription.Quotes)
	require.NoError(t, err)
}

func TestActiveSnapshot(t *testing.T) {
	m := subscription.New(0)
	_ = m.Changes()

	_, _ = m.Acquire("AAPL", subscription.Trades)
	_, _ = m.Acquire("MSFT", subscription.Trades)
	_, _ = m.Acquire("GOOG", subscription.Quotes)

	assert.ElementsMatch(t, []string{"AAPL", "MSFT"}, m.Active(subscription.Trades))
	assert.ElementsMatch(t, []string{"GOOG"}, m.Active(subscription.Quotes))
}

func TestChangesPanicsOnSecondConsumer(t *testing.T) {
	m := subscription.New(0)
	_ = m.Changes()
	assert.Panics(t, func() { _ = m.Changes() })
}
