package replication

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// ErrRetryable is a custom error to retry the logic when returned.
var ErrRetryable = errors.New("retryable replication error")

// func (re ErrRetryable) Is(err error) bool {
// 	return errors.Is(err, ErrRetryable("")
// }

type Retryer struct {
	retryFunc    func(ctx context.Context) error
	interval     time.Duration
	backoffCoeff int
	// onRetry, when non-nil, is called once per retryable failure, just before
	// backing off to redial. May be nil (master, or live-only replica).
	onRetry func()
}

func NewRetryer(retryFunc func(ctx context.Context) error, interval time.Duration, backoffCoeff int,
	onRetry func(),
) *Retryer {
	return &Retryer{
		retryFunc:    retryFunc,
		interval:     interval,
		backoffCoeff: backoffCoeff,
		onRetry:      onRetry,
	}
}

// Run tries the Retryer until it succeeds, it returns unretriable error, or the context is canceled.
func (r *Retryer) Run(ctx context.Context) error {
	const decimal = 10
	cnt := -1
	for {
		cnt++
		select {
		case <-ctx.Done():
			return errors.New("context canceled")
		default:
			err := r.retryFunc(ctx)
			// success
			if err == nil {
				return nil
			}

			if errors.Is(err, ErrRetryable) {
				// retryable error. continue
				if r.onRetry != nil {
					r.onRetry()
				}
				interval := retryInterval(r.interval, r.backoffCoeff, cnt)
				log.Warn("caught a retryable error. It will be retried after an interval:" +
					strconv.FormatInt(interval.Milliseconds(), decimal) + "[ms], err=" + err.Error())
				// Sleep, but stay responsive to cancellation: a bare
				// time.Sleep would ignore ctx for the whole interval, which
				// grows exponentially and breaks this function's documented
				// "or the context is canceled" contract.
				timer := time.NewTimer(interval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return errors.New("context canceled")
				case <-timer.C:
				}
				continue
			} else {
				// not retryable error, give up.
				log.Warn("caught a non-retryable error:" + err.Error())
				return err
			}
		}
	}
}

func retryInterval(interval time.Duration, backoffCoeff, retryCount int) time.Duration {
	coeff := math.Pow(float64(backoffCoeff), float64(retryCount))
	intervalMilliSec := float64(interval.Milliseconds())
	return time.Duration(intervalMilliSec*coeff) * time.Millisecond
}
