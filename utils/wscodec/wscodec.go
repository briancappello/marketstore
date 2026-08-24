// Package wscodec provides pluggable encodings for marketstore's WebSocket
// endpoints.
//
// The encoding is chosen per connection through WebSocket subprotocol
// negotiation. msgpack is the default whenever no subprotocol is negotiated,
// so clients that predate JSON support are unaffected.
package wscodec

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"

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

func (jsonCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(sanitize(v)) }
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

// sanitize returns a copy of v with every non-finite float replaced by nil.
//
// encoding/json rejects NaN and ±Inf outright, while msgpack encodes them.
// OHLCV columns carry NaN where a bar has no value, so without this a single
// gapped bar would drop the entire frame for JSON subscribers while msgpack
// subscribers on the same payload received it. nil encodes as JSON null,
// which is the correct representation of a missing measurement.
//
// Structs are rebuilt as maps keyed by their json tag because the payload
// data this needs to reach sits behind an interface-typed struct field.
// The rebuild honours json:"-" and splits the tag on its first comma, which
// covers every wire struct in this repo. It does NOT implement omitempty:
// if you add omitempty to a streamed struct, the field is still emitted.
func sanitize(v any) any {
	return sanitizeValue(reflect.ValueOf(v))
}

func sanitizeValue(rv reflect.Value) any {
	if !rv.IsValid() {
		return nil
	}

	switch rv.Kind() {
	case reflect.Interface, reflect.Pointer:
		if rv.IsNil() {
			return nil
		}
		return sanitizeValue(rv.Elem())

	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return rv.Interface()

	case reflect.Slice, reflect.Array:
		// []byte is base64-encoded by encoding/json and holds no floats.
		if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
			return rv.Interface()
		}
		if rv.Kind() == reflect.Slice && rv.IsNil() {
			return nil
		}
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = sanitizeValue(rv.Index(i))
		}
		return out

	case reflect.Map:
		if rv.IsNil() {
			return nil
		}
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out[fmt.Sprint(iter.Key().Interface())] = sanitizeValue(iter.Value())
		}
		return out

	case reflect.Struct:
		// time.Time and anything else with custom marshalling must not be
		// torn apart into fields; hand it to encoding/json intact.
		if _, ok := rv.Interface().(json.Marshaler); ok {
			return rv.Interface()
		}
		rt := rv.Type()
		out := make(map[string]any, rt.NumField())
		for i := 0; i < rt.NumField(); i++ {
			field := rt.Field(i)
			if !field.IsExported() {
				continue
			}
			name, ok := jsonFieldName(field)
			if !ok {
				continue
			}
			out[name] = sanitizeValue(rv.Field(i))
		}
		return out
	}

	return rv.Interface()
}

// jsonFieldName resolves the wire name for a struct field, reporting false
// when the field is tagged json:"-" and must be omitted.
func jsonFieldName(f reflect.StructField) (string, bool) {
	tag, ok := f.Tag.Lookup("json")
	if !ok {
		return f.Name, true
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "-" {
		return "", false
	}
	if name == "" {
		return f.Name, true
	}
	return name, true
}
