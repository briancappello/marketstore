package flatfiles

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// ProgressReporter renders a single-line, self-updating progress bar to
// stderr showing completion percentage, an ASCII bar, processing rate
// (dates/sec), and an estimated time of arrival (ETA).
//
// Why stderr: the zap logger writes structured JSON to stdout (see
// utils/log), so rendering the bar to stderr keeps the two streams from
// garbling each other. When stderr is not a terminal (e.g., redirected to a
// file or piped), the bar is disabled and periodic text snapshots are emitted
// instead so logs remain readable.
//
// The reporter is goroutine-safe and driven by a time-based ticker, so the
// bar advances smoothly even when individual S3 downloads stall.
type ProgressReporter struct {
	label     string
	total     int64
	startTime time.Time

	// readCurrent returns the current number of completed work units. It is
	// read on each tick; the caller supplies it so the reporter does not own
	// the counters.
	readCurrent func() int64

	interval time.Duration
	isTTY    bool

	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}

	mu      sync.Mutex
	maxLine int // widest line drawn so far, for clearing on redraw
}

// barWidth is the number of cells in the rendered ASCII bar.
const barWidth = 30

// NewProgressReporter creates a reporter for a unit of work with the given
// label (e.g., "1D") and total number of work units (dates). readCurrent must
// return the number of completed units; it is polled on each tick. interval
// controls how often the bar is redrawn; if <= 0 it defaults to 500ms.
//
// forceMode controls TTY behaviour: "always" forces the bar on, "never"
// disables it, and any other value ("auto" or "") auto-detects whether stderr
// is a terminal.
func NewProgressReporter(label string, total int64, readCurrent func() int64, interval time.Duration, forceMode string) *ProgressReporter {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &ProgressReporter{
		label:       label,
		total:       total,
		startTime:   time.Now(),
		readCurrent: readCurrent,
		interval:    interval,
		isTTY:       resolveTTY(forceMode),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// resolveTTY decides whether to render an interactive bar based on the
// requested mode and whether stderr is a character device.
func resolveTTY(forceMode string) bool {
	switch forceMode {
	case "always":
		return true
	case "never":
		return false
	default:
		fi, err := os.Stderr.Stat()
		if err != nil {
			return false
		}
		return (fi.Mode() & os.ModeCharDevice) != 0
	}
}

// Active reports whether an interactive bar is being rendered. When true,
// callers should suppress redundant periodic text logging.
func (p *ProgressReporter) Active() bool {
	return p.total > 0 && p.isTTY
}

// Start launches the background ticker that periodically redraws the bar.
// Call Stop (or Finish) to terminate it. Start is a no-op if total <= 0.
func (p *ProgressReporter) Start() {
	if p.total <= 0 {
		close(p.done)
		return
	}
	go p.loop()
}

func (p *ProgressReporter) loop() {
	defer close(p.done)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Snapshot interval for non-TTY mode: emit a text line less often than
	// the redraw cadence to avoid flooding logs.
	const nonTTYEvery = 10 * time.Second
	lastSnapshot := time.Now()

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			cur := p.readCurrent()
			if p.isTTY {
				p.render(cur, false)
			} else if time.Since(lastSnapshot) >= nonTTYEvery {
				p.snapshot(cur)
				lastSnapshot = time.Now()
			}
		}
	}
}

// Finish stops the ticker, draws a final 100%/complete line, and waits for the
// background goroutine to exit. It is safe to call multiple times.
func (p *ProgressReporter) Finish() {
	p.Stop()
	if p.total <= 0 {
		return
	}
	cur := p.readCurrent()
	if p.isTTY {
		p.render(cur, true)
		// Terminate the bar's line so subsequent output starts cleanly.
		fmt.Fprintln(os.Stderr)
	} else {
		p.snapshot(cur)
	}
}

// Stop terminates the ticker goroutine without drawing a final line.
func (p *ProgressReporter) Stop() {
	p.stopOnce.Do(func() { close(p.stop) })
	<-p.done
}

// render draws the interactive single-line bar, overwriting the previous line.
// It writes a carriage return, the line, then trailing spaces to erase any
// remnant of a previously longer line, and a final carriage return so the
// cursor rests at column 0 ready for the next redraw.
func (p *ProgressReporter) render(cur int64, final bool) {
	line := p.format(cur, final)

	p.mu.Lock()
	defer p.mu.Unlock()

	pad := p.maxLine - len(line)
	if pad < 0 {
		pad = 0
	}
	if len(line) > p.maxLine {
		p.maxLine = len(line)
	}
	fmt.Fprintf(os.Stderr, "\r%s%s\r", line, strings.Repeat(" ", pad))
}

// snapshot emits a one-off text progress line (used when stderr is not a TTY).
func (p *ProgressReporter) snapshot(cur int64) {
	fmt.Fprintln(os.Stderr, p.format(cur, false))
}

// format builds the human-readable progress string.
func (p *ProgressReporter) format(cur int64, final bool) string {
	if final {
		cur = p.total
	}
	if cur > p.total {
		cur = p.total
	}

	frac := float64(cur) / float64(p.total)
	filled := int(frac * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	bar := strings.Repeat("=", filled) + strings.Repeat(" ", barWidth-filled)

	elapsed := time.Since(p.startTime)
	var rate float64
	if elapsed.Seconds() > 0 {
		rate = float64(cur) / elapsed.Seconds()
	}

	var eta string
	switch {
	case final || cur >= p.total:
		eta = "done in " + fmtDuration(elapsed)
	case rate > 0:
		remaining := time.Duration(float64(p.total-cur)/rate) * time.Second
		eta = "ETA " + fmtDuration(remaining)
	default:
		eta = "ETA --"
	}

	return fmt.Sprintf("[flatfiles] %-8s [%s] %3.0f%% %d/%d %5.1f/s %s",
		p.label, bar, frac*100, cur, p.total, rate, eta)
}

// fmtDuration renders a duration compactly (e.g., "1h02m", "3m05s", "12s").
func fmtDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	switch {
	case h > 0:
		return fmt.Sprintf("%dh%02dm", h, m)
	case m > 0:
		return fmt.Sprintf("%dm%02ds", m, s)
	default:
		return fmt.Sprintf("%ds", s)
	}
}
