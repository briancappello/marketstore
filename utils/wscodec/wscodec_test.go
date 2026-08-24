package wscodec

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

type sample struct {
	Key  string `msgpack:"key" json:"key"`
	Data any    `msgpack:"data" json:"data"`
}

func TestSubprotocolsOrder(t *testing.T) {
	// Upgrader.Subprotocols is server preference order. msgpack must come
	// first so a client offering both keeps the historical encoding.
	assert.Equal(t, []string{"msgpack", "json"}, Subprotocols)
}

func TestForResolvesCodec(t *testing.T) {
	assert.Equal(t, "msgpack", For("").Name())
	assert.Equal(t, "msgpack", For("msgpack").Name())
	assert.Equal(t, "json", For("json").Name())
	// Anything unrecognised must fall back to msgpack, never error.
	assert.Equal(t, "msgpack", For("cbor").Name())
	assert.Equal(t, "msgpack", For("JSON").Name())
}

func TestMessageTypes(t *testing.T) {
	assert.Equal(t, websocket.BinaryMessage, For("msgpack").MessageType())
	assert.Equal(t, websocket.TextMessage, For("json").MessageType())
}

func TestMsgpackRoundTrip(t *testing.T) {
	c := For("msgpack")
	buf, err := c.Marshal(sample{Key: "AAPL/1Min/OHLCV", Data: "x"})
	assert.Nil(t, err)

	var got sample
	assert.Nil(t, c.Unmarshal(buf, &got))
	assert.Equal(t, "AAPL/1Min/OHLCV", got.Key)
}

func TestJSONRoundTripUsesLowercaseTags(t *testing.T) {
	c := For("json")
	buf, err := c.Marshal(sample{Key: "AAPL/1Min/OHLCV", Data: "x"})
	assert.Nil(t, err)

	// The json tag, not the Go field name, must appear on the wire.
	assert.Contains(t, string(buf), `"key"`)
	assert.NotContains(t, string(buf), `"Key"`)

	var got sample
	assert.Nil(t, c.Unmarshal(buf, &got))
	assert.Equal(t, "AAPL/1Min/OHLCV", got.Key)
}

func TestJSONMarshalsNonFiniteFloatsAsNull(t *testing.T) {
	c := For("json")
	buf, err := c.Marshal(sample{
		Key: "AAPL/1Min/OHLCV",
		Data: map[string]any{
			"Open":   math.NaN(),
			"High":   math.Inf(1),
			"Low":    math.Inf(-1),
			"Close":  1.5,
			"Volume": float32(math.NaN()),
		},
	})
	// Without sanitizing, encoding/json returns
	// "json: unsupported value: NaN" and buf is nil.
	assert.Nil(t, err)

	var got map[string]any
	assert.Nil(t, json.Unmarshal(buf, &got))
	data := got["data"].(map[string]any)

	assert.Nil(t, data["Open"])
	assert.Nil(t, data["High"])
	assert.Nil(t, data["Low"])
	assert.Nil(t, data["Volume"])
	assert.Equal(t, 1.5, data["Close"])
}

func TestJSONSanitizesInsideSlices(t *testing.T) {
	// The live stream pushes column-oriented data: map values are slices.
	c := For("json")
	buf, err := c.Marshal(sample{
		Key:  "AAPL/5Min/OHLCV",
		Data: map[string]any{"Close": []float64{1.0, math.NaN(), 3.0}},
	})
	assert.Nil(t, err)

	var got map[string]any
	assert.Nil(t, json.Unmarshal(buf, &got))
	closes := got["data"].(map[string]any)["Close"].([]any)

	assert.Equal(t, 1.0, closes[0])
	assert.Nil(t, closes[1])
	assert.Equal(t, 3.0, closes[2])
}

func TestJSONSanitizeHonoursTags(t *testing.T) {
	// Sanitizing rebuilds structs, so json tags must survive the rebuild
	// and json:"-" fields must stay off the wire.
	type inner struct {
		Shown  float64 `json:"shown"`
		Hidden bool    `json:"-"`
	}
	c := For("json")
	buf, err := c.Marshal(sample{Key: "k", Data: inner{Shown: math.NaN(), Hidden: true}})
	assert.Nil(t, err)

	assert.Contains(t, string(buf), `"shown"`)
	assert.NotContains(t, string(buf), `"Hidden"`)
	assert.NotContains(t, string(buf), `"Shown"`)

	var got map[string]any
	assert.Nil(t, json.Unmarshal(buf, &got))
	assert.Nil(t, got["data"].(map[string]any)["shown"])
}

func TestMsgpackStillEncodesNonFiniteFloats(t *testing.T) {
	// The msgpack path must not be changed by this work.
	c := For("msgpack")
	buf, err := c.Marshal(sample{Key: "k", Data: map[string]any{"Open": math.NaN()}})
	assert.Nil(t, err)
	assert.NotEmpty(t, buf)
}
