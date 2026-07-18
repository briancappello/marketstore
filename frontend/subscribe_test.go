package frontend_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/frontend"
)

// mockController implements frontend.SubscriptionController for testing.
type mockController struct {
	// active[symbol] = data types
	active map[string][]string
	// rejectDataType, when set, causes Subscribe/Unsubscribe to error if any
	// requested data type matches it (simulating unknown data type / cap).
	subErr error
}

func (m *mockController) Subscribe(symbol string, dataTypes []string) error {
	if m.subErr != nil {
		return m.subErr
	}
	if m.active == nil {
		m.active = map[string][]string{}
	}
	m.active[symbol] = append(m.active[symbol], dataTypes...)
	return nil
}

func (m *mockController) Unsubscribe(symbol string, dataTypes []string) error {
	if m.subErr != nil {
		return m.subErr
	}
	delete(m.active, symbol)
	return nil
}

func (m *mockController) ActiveSubscriptions() map[string][]string {
	return m.active
}

func setupSubscribeTest(t *testing.T, c frontend.SubscriptionController) {
	t.Helper()
	atomic.StoreUint32(&frontend.Queryable, 1)
	frontend.RegisterSubscriptionController(c)
	t.Cleanup(func() {
		frontend.RegisterSubscriptionController(nil)
	})
}

func TestSubscribe_Delegates(t *testing.T) {
	ctrl := &mockController{}
	setupSubscribeTest(t, ctrl)

	service := &frontend.DataService{}
	req := &frontend.SubscribeRequest{
		Symbol:    "AAPL",
		DataTypes: []string{"trades", "quotes"},
		Action:    "subscribe",
	}
	var resp frontend.SubscribeResponse
	err := service.Subscribe(nil, req, &resp)

	assert.Nil(t, err)
	assert.ElementsMatch(t, []string{"trades", "quotes"}, resp.Active["AAPL"])
}

func TestUnsubscribe_Delegates(t *testing.T) {
	ctrl := &mockController{active: map[string][]string{"AAPL": {"trades"}}}
	setupSubscribeTest(t, ctrl)

	service := &frontend.DataService{}
	req := &frontend.SubscribeRequest{
		Symbol:    "AAPL",
		DataTypes: []string{"trades"},
		Action:    "unsubscribe",
	}
	var resp frontend.SubscribeResponse
	err := service.Subscribe(nil, req, &resp)

	assert.Nil(t, err)
	assert.NotContains(t, resp.Active, "AAPL")
}

func TestSubscribe_NoController(t *testing.T) {
	atomic.StoreUint32(&frontend.Queryable, 1)
	frontend.RegisterSubscriptionController(nil)
	defer frontend.RegisterSubscriptionController(nil)

	service := &frontend.DataService{}
	req := &frontend.SubscribeRequest{Symbol: "AAPL", DataTypes: []string{"trades"}, Action: "subscribe"}
	var resp frontend.SubscribeResponse
	err := service.Subscribe(nil, req, &resp)

	assert.NotNil(t, err)
}

func TestSubscribe_InvalidAction(t *testing.T) {
	setupSubscribeTest(t, &mockController{})

	service := &frontend.DataService{}
	req := &frontend.SubscribeRequest{Symbol: "AAPL", DataTypes: []string{"trades"}, Action: "bogus"}
	var resp frontend.SubscribeResponse
	err := service.Subscribe(nil, req, &resp)

	assert.NotNil(t, err)
}

func TestSubscribe_ControllerError(t *testing.T) {
	setupSubscribeTest(t, &mockController{subErr: fmt.Errorf("unknown data type \"foo\"")})

	service := &frontend.DataService{}
	req := &frontend.SubscribeRequest{Symbol: "AAPL", DataTypes: []string{"foo"}, Action: "subscribe"}
	var resp frontend.SubscribeResponse
	err := service.Subscribe(nil, req, &resp)

	assert.NotNil(t, err)
}

func TestSubscribe_MissingFields(t *testing.T) {
	setupSubscribeTest(t, &mockController{})
	service := &frontend.DataService{}

	// Missing symbol.
	err := service.Subscribe(nil, &frontend.SubscribeRequest{DataTypes: []string{"trades"}, Action: "subscribe"}, &frontend.SubscribeResponse{})
	assert.NotNil(t, err)

	// Missing data types.
	err = service.Subscribe(nil, &frontend.SubscribeRequest{Symbol: "AAPL", Action: "subscribe"}, &frontend.SubscribeResponse{})
	assert.NotNil(t, err)
}

func TestSubscribe_NotQueryable(t *testing.T) {
	atomic.StoreUint32(&frontend.Queryable, 0)
	defer atomic.StoreUint32(&frontend.Queryable, 1)

	service := &frontend.DataService{}
	var resp frontend.SubscribeResponse
	err := service.Subscribe(nil, &frontend.SubscribeRequest{Symbol: "AAPL", DataTypes: []string{"trades"}, Action: "subscribe"}, &resp)

	assert.NotNil(t, err)
}
