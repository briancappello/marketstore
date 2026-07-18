package frontend

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// SubscribeRequest is the HTTP RPC request to subscribe or unsubscribe a symbol's
// live tick streams.
type SubscribeRequest struct {
	Symbol    string   `msgpack:"symbol"`
	DataTypes []string `msgpack:"data_types"` // ["trades","quotes"]
	Action    string   `msgpack:"action"`     // "subscribe" | "unsubscribe"
}

// SubscribeResponse is the HTTP RPC response for a subscribe/unsubscribe call.
//
// Active is the controller's current DESIRED subscription set (intent) — a map
// of symbol -> data types. A successful response means the request was accepted
// and the desired state recorded; it does NOT mean tick data is already
// flowing. Confirmation of live data is only observable by seeing rows land in
// the {sym}/1Sec/TRADE|QUOTE buckets.
type SubscribeResponse struct {
	Active map[string][]string `msgpack:"active"`
}

// Subscribe drives a live tick subscription (trades/quotes) for a symbol.
// Errors are returned synchronously only for request-level failures (nil
// controller, invalid action, unknown data type, cap exceeded). Upstream/wire
// failures surface asynchronously as logged status + absent data.
func (s *DataService) Subscribe(
	r *http.Request,
	req *SubscribeRequest,
	response *SubscribeResponse,
) error {
	if atomic.LoadUint32(&Queryable) == 0 {
		return errNotQueryable
	}

	controller := GetSubscriptionController()
	if controller == nil {
		return fmt.Errorf("dynamic subscriptions not available")
	}

	if req == nil {
		return fmt.Errorf("nil request")
	}
	if req.Symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	if len(req.DataTypes) == 0 {
		return fmt.Errorf("data_types is required (e.g. [\"trades\",\"quotes\"])")
	}

	switch req.Action {
	case "subscribe":
		if err := controller.Subscribe(req.Symbol, req.DataTypes); err != nil {
			return err
		}
	case "unsubscribe":
		if err := controller.Unsubscribe(req.Symbol, req.DataTypes); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid action %q: must be \"subscribe\" or \"unsubscribe\"", req.Action)
	}

	response.Active = controller.ActiveSubscriptions()
	return nil
}
