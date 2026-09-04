package replication_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/replication"
)

// Retryer succeeds at a certain trial.
type retryer struct {
	Count     int
	SucceedAt int
}

func (r *retryer) try(_ context.Context) error {
	r.Count++
	if r.Count == r.SucceedAt {
		return nil
	}
	return replication.ErrRetryable
}

// Each retryable error means the replication stream dropped and we are about to
// redial. The replica needs that signal: while it was disconnected the master
// may have revised epochs at or below the backfill watermark, which only a
// lookback pass will pick up.
func TestRetryerCallsOnRetryBeforeEachReconnect(t *testing.T) {
	t.Parallel()

	// Fails twice, succeeds on the 3rd try => 2 reconnects.
	inner := retryer{SucceedAt: 3}
	reconnects := 0

	// backoffCoeff 1 keeps the retry interval flat so the test stays fast.
	r := replication.NewRetryer(inner.try, time.Millisecond, 1, func() { reconnects++ })

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if reconnects != 2 {
		t.Errorf("onRetry called %d times, want 2 (one per retryable failure)", reconnects)
	}
}

// A nil onRetry must be safe: masters and live-only replicas have no backfill
// driver to notify.
func TestRetryerNilOnRetryDoesNotPanic(t *testing.T) {
	t.Parallel()

	inner := retryer{SucceedAt: 2}
	r := replication.NewRetryer(inner.try, time.Millisecond, 1, nil)

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
}

func TestRetryer_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		retryFunc func(ctx context.Context) error
		interval  time.Duration
		context   context.Context
		wantErr   bool
	}{
		{
			name:      "success",
			retryFunc: func(ctx context.Context) error { return nil },
			context:   context.Background(),
			wantErr:   false,
		},
		{
			name:      "not retryable error",
			retryFunc: func(ctx context.Context) error { return errors.New("some error") },
			context:   context.Background(),
			wantErr:   true,
		},
		{
			// Run retries a retryable error until the context is canceled, so
			// this case needs a deadline -- with context.Background() it would
			// loop forever on an exponentially growing backoff.
			name:      "retryable error until the context expires",
			retryFunc: func(ctx context.Context) error { return replication.ErrRetryable },
			context: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				_ = cancel
				return ctx
			}(),
			wantErr: true,
		},
		{
			name: "succeed at the 3rd try",
			retryFunc: func() func(ctx context.Context) error {
				r := retryer{SucceedAt: 3}
				return r.try
			}(),
			context: context.Background(),
			wantErr: false,
		},
		{
			name: "don't retry if context is canceled",
			retryFunc: func(ctx context.Context) error {
				return replication.ErrRetryable
			},
			context: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx // already canceled context is passed
			}(),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// --- given ---
			r := replication.NewRetryer(tt.retryFunc, 10*time.Millisecond, 2, nil)

			// --- when ---
			err := r.Run(tt.context)

			// --- then ---
			if (err != nil) != tt.wantErr {
				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
