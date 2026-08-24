// Package wscodec provides pluggable encodings for marketstore's WebSocket
// endpoints.
//
// The encoding is chosen per connection through WebSocket subprotocol
// negotiation. msgpack is the default whenever no subprotocol is negotiated,
// so clients that predate JSON support are unaffected.
package wscodec

import (
	"encoding/json"

	"github.com/gorilla/websocket"
	msgpack "github.com/vmihailenco/msgpack"
)

// Subprotocols lists the supported WebSocket subprotocols in server
// preference order, suitable for assigning to websocket.Upgrader.Subprotocols.
//
// gorilla selects the first entry in this list that the client also offered.
// msgpack is listed first deliberately: a client offering both receives
// msgpack, so no existing client can change behaviour by accident.
var Subprotocols = []string{"msgpack", "json"}

// Codec encodes and decodes WebSocket frame payloads.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(b []byte, v any) error
	// MessageType is the gorilla/websocket frame type this codec writes.
	// RFC 6455 reserves text frames for UTF-8 payloads, so JSON writes
	// TextMessage and msgpack writes BinaryMessage.
	MessageType() int
	// Name is the negotiated subprotocol name this codec implements.
	Name() string
}

type msgpackCodec struct{}

func (msgpackCodec) Marshal(v any) ([]byte, error)   { return msgpack.Marshal(v) }
func (msgpackCodec) Unmarshal(b []byte, v any) error { return msgpack.Unmarshal(b, v) }
func (msgpackCodec) MessageType() int                { return websocket.BinaryMessage }
func (msgpackCodec) Name() string                    { return "msgpack" }

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (jsonCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
func (jsonCodec) MessageType() int                { return websocket.TextMessage }
func (jsonCodec) Name() string                    { return "json" }

// The codecs are stateless, so a single shared value of each is enough.
var (
	MsgpackCodec Codec = msgpackCodec{}
	JSONCodec    Codec = jsonCodec{}
)

// For maps a negotiated subprotocol to its codec.
//
// The empty string means gorilla negotiated no subprotocol. That case, and
// any unrecognised value, maps to msgpack so an odd handshake degrades to
// the historical behaviour instead of failing the connection.
func For(subprotocol string) Codec {
	if subprotocol == "json" {
		return JSONCodec
	}
	return MsgpackCodec
}
