package ws

// Test-only accessors. These live in an _test.go file so they are not part of
// the public API but are available to white-box tests in package ws.

// ErrConnClosedForTest exposes the unexported sentinel for assertions.
var ErrConnClosedForTest = errConnClosed

// Exported wire actions for test assertions.
const (
	ActionSubscribeForTest   = actionSubscribe
	ActionUnsubscribeForTest = actionUnsubscribe
)
