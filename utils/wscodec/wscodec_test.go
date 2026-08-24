package wscodec

import (
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
