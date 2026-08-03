package executor

import (
	"bytes"
	"testing"
	"time"
)

// Regression tests for the result-buffer growth in readSecondStage.
//
// The compressed-data branch only *estimates* the required size
// (sum(compressed len) * 4). When snappy's real ratio combined with the
// (varRecLen+8)/varRecLen epoch expansion exceeds that estimate by more
// than 2x, the old code doubled the buffer exactly once, copy() silently
// truncated, and rbCursor still advanced by the full length -- so
// `rb = rb[:rbCursor]` panicked with "slice bounds out of range".
func TestGrowResultBuffer(t *testing.T) {
	tests := []struct {
		name   string
		bufLen int
		cursor int
		need   int
	}{
		{"no growth needed", 100, 10, 50},
		{"exact fit", 100, 0, 100},
		{"one doubling suffices", 100, 50, 150},
		// The actual production panic: estimate 1968, needed 3990 (2.03x).
		// A single doubling yields 3936 -- still short.
		{"needs more than one doubling", 1968, 1900, 3990},
		{"needs many doublings", 8, 4, 1000},
		{"zero-length buffer", 0, 0, 512},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, tt.bufLen)
			got := growResultBuffer(buf, tt.cursor, tt.need)
			if len(got) < tt.need {
				t.Fatalf("buffer still too small: len=%d, need=%d", len(got), tt.need)
			}
		})
	}
}

func TestGrowResultBufferPreservesContent(t *testing.T) {
	buf := make([]byte, 16)
	want := []byte("preserve-me")
	copy(buf, want)

	got := growResultBuffer(buf, len(want), 4096)
	if len(got) < 4096 {
		t.Fatalf("len(got)=%d, want >= 4096", len(got))
	}
	if !bytes.Equal(got[:len(want)], want) {
		t.Fatalf("content not preserved: got %q, want %q", got[:len(want)], want)
	}
}

// A zero-length buffer must not spin forever doubling zero.
func TestGrowResultBufferZeroDoesNotHang(t *testing.T) {
	done := make(chan []byte, 1)
	go func() { done <- growResultBuffer(nil, 0, 1024) }()
	select {
	case got := <-done:
		if len(got) < 1024 {
			t.Fatalf("len(got)=%d, want >= 1024", len(got))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("growResultBuffer hung on a zero-length buffer")
	}
}
