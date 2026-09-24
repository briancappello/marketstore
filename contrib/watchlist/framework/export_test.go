package framework

import (
	"testing"

	"github.com/alpacahq/marketstore/v4/plugins/trigger"
)

// Test-only exports for the framework_test package.

// RecordCapture exposes what the real WAL dispatcher hands to triggers.
type RecordCapture struct{ c *captureTrigger }

// SetupCapturingInstance starts an instance whose WAL dispatches every write
// to the returned capture.
func SetupCapturingInstance(t testing.TB) *RecordCapture {
	return &RecordCapture{c: setupCapturingInstance(t)}
}

// Next returns the n-th (0-based) dispatched batch for keyPath.
func (r *RecordCapture) Next(t testing.TB, keyPath string, n int) []trigger.Record {
	return r.c.next(t, keyPath, n)
}
