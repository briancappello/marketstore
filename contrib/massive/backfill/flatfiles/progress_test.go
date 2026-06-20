package flatfiles

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFmtDuration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   time.Duration
		want string
	}{
		{0, "0s"},
		{-5 * time.Second, "0s"},
		{12 * time.Second, "12s"},
		{90 * time.Second, "1m30s"},
		{3 * time.Minute, "3m00s"},
		{time.Hour + 2*time.Minute, "1h02m"},
		{2*time.Hour + 5*time.Minute + 30*time.Second, "2h05m"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, fmtDuration(c.in), "fmtDuration(%s)", c.in)
	}
}

func TestProgressFormat(t *testing.T) {
	t.Parallel()

	p := NewProgressReporter("1D", 100, func() int64 { return 0 }, 0, "never")
	// Pin the start time so rate/ETA are deterministic.
	p.startTime = time.Now().Add(-10 * time.Second)

	// 25 of 100 done in 10s => 2.5/s, 75 remaining => ETA 30s.
	line := p.format(25, false)
	assert.Contains(t, line, "1D")
	assert.Contains(t, line, "25/100")
	assert.Contains(t, line, "25%")
	assert.Contains(t, line, "2.5/s")
	assert.Contains(t, line, "ETA 30s")
	assert.Contains(t, line, "[")
	assert.Contains(t, line, "=")

	// Final clamps to total and reports elapsed.
	final := p.format(25, true)
	assert.Contains(t, final, "100/100")
	assert.Contains(t, final, "100%")
	assert.Contains(t, final, "done in")
	// Fully filled bar.
	assert.Equal(t, barWidth, strings.Count(final, "="))
}

func TestProgressFormatClampAndZeroRate(t *testing.T) {
	t.Parallel()

	p := NewProgressReporter("1Min", 10, func() int64 { return 0 }, 0, "never")
	p.startTime = time.Now() // ~0 elapsed => rate 0 => ETA --

	line := p.format(0, false)
	assert.Contains(t, line, "0/10")
	assert.Contains(t, line, "ETA --")

	// cur greater than total is clamped.
	over := p.format(999, false)
	assert.Contains(t, over, "10/10")
}

func TestProgressDisabledZeroTotal(t *testing.T) {
	t.Parallel()

	p := NewProgressReporter("1D", 0, func() int64 { return 0 }, 0, "always")
	assert.False(t, p.Active(), "zero-total reporter must be inactive")
	// Start/Finish must not block or panic with zero total.
	p.Start()
	p.Finish()
}

func TestProgressActiveRespectsMode(t *testing.T) {
	t.Parallel()

	on := NewProgressReporter("1D", 5, func() int64 { return 0 }, 0, "always")
	assert.True(t, on.Active())

	off := NewProgressReporter("1D", 5, func() int64 { return 0 }, 0, "never")
	assert.False(t, off.Active())
}
