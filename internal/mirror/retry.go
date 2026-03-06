// Package mirror implements the core replication engine for gomm2.
//
// This file provides generic retry logic with exponential backoff, jitter,
// and a circuit breaker pattern for sustained failures.
package mirror

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

// RetryConfig holds parameters for the retry helper.
type RetryConfig struct {
	// MaxRetries is the maximum number of retry attempts. Zero means no retries.
	MaxRetries int

	// BackoffBase is the initial backoff duration before the first retry.
	BackoffBase time.Duration

	// BackoffMax is the upper bound on backoff duration.
	BackoffMax time.Duration
}

// DefaultRetryConfig returns a sensible default retry configuration.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:  10,
		BackoffBase: 100 * time.Millisecond,
		BackoffMax:  30 * time.Second,
	}
}

// RetryableFunc is a function that may be retried. It returns an error
// to signal failure, or nil on success.
type RetryableFunc func(ctx context.Context) error

// Retry executes fn with exponential backoff and jitter. It returns nil on
// success, or the last error after exhausting all attempts.
func Retry(ctx context.Context, cfg RetryConfig, fn RetryableFunc) error {
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("context cancelled after %d attempts: %w (last: %v)", attempt, err, lastErr)
			}
			return fmt.Errorf("context cancelled: %w", err)
		}

		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}

		if attempt == cfg.MaxRetries {
			break
		}

		backoff := computeBackoff(attempt, cfg.BackoffBase, cfg.BackoffMax)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("context cancelled during backoff (attempt %d/%d): %w",
				attempt+1, cfg.MaxRetries, lastErr)
		case <-timer.C:
		}
	}
	return fmt.Errorf("exhausted %d retries: %w", cfg.MaxRetries, lastErr)
}

// computeBackoff calculates the backoff duration with exponential growth and
// full jitter, capped at maxBackoff.
func computeBackoff(attempt int, base, maxBackoff time.Duration) time.Duration {
	exp := math.Pow(2, float64(attempt))
	backoff := time.Duration(float64(base) * exp)
	if backoff > maxBackoff || backoff <= 0 {
		backoff = maxBackoff
	}
	// Full jitter: uniform random between 0 and backoff
	jittered := time.Duration(rand.Int63n(int64(backoff) + 1))
	return jittered
}

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	// CircuitClosed means the circuit is healthy — operations proceed normally.
	CircuitClosed CircuitState = iota
	// CircuitHalfOpen means the circuit is testing — a single probe is allowed.
	CircuitHalfOpen
	// CircuitOpen means the circuit is tripped — operations are rejected immediately.
	CircuitOpen
)

// String returns a human-readable name for the circuit state.
func (cs CircuitState) String() string {
	switch cs {
	case CircuitClosed:
		return "closed"
	case CircuitHalfOpen:
		return "half-open"
	case CircuitOpen:
		return "open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig holds configuration for the circuit breaker.
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures that trips the breaker.
	FailureThreshold int

	// ResetTimeout is how long the breaker stays open before moving to half-open.
	ResetTimeout time.Duration

	// HalfOpenMaxProbes is how many successful probes are needed to fully close the breaker.
	HalfOpenMaxProbes int
}

// DefaultCircuitBreakerConfig returns a sensible default circuit breaker configuration.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold:  5,
		ResetTimeout:      30 * time.Second,
		HalfOpenMaxProbes: 3,
	}
}

// CircuitBreaker implements the circuit breaker pattern for protecting
// against sustained downstream failures.
type CircuitBreaker struct {
	mu              sync.Mutex
	state           CircuitState
	failures        int
	successes       int // consecutive successes in half-open state
	lastFailureTime time.Time
	cfg             CircuitBreakerConfig
}

// NewCircuitBreaker creates a new CircuitBreaker with the given configuration.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		state: CircuitClosed,
		cfg:   cfg,
	}
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.checkStateTransition()
	return cb.state
}

// Allow checks whether an operation is allowed. Returns true if the circuit is
// closed or half-open (probe allowed). Returns false if the circuit is open.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.checkStateTransition()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitHalfOpen:
		return true
	case CircuitOpen:
		return false
	default:
		return false
	}
}

// RecordSuccess records a successful operation. In half-open state, sufficient
// consecutive successes close the breaker.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		cb.failures = 0
	case CircuitHalfOpen:
		cb.successes++
		if cb.successes >= cb.cfg.HalfOpenMaxProbes {
			cb.state = CircuitClosed
			cb.failures = 0
			cb.successes = 0
		}
	}
}

// RecordFailure records a failed operation. Sufficient consecutive failures
// trip the breaker to open state.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailureTime = time.Now()
	cb.failures++

	switch cb.state {
	case CircuitClosed:
		if cb.failures >= cb.cfg.FailureThreshold {
			cb.state = CircuitOpen
		}
	case CircuitHalfOpen:
		// Any failure in half-open trips back to open
		cb.state = CircuitOpen
		cb.successes = 0
	}
}

// Reset forcefully resets the breaker to the closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = CircuitClosed
	cb.failures = 0
	cb.successes = 0
}

// checkStateTransition transitions from open → half-open if the reset timeout has elapsed.
// Must be called with cb.mu held.
func (cb *CircuitBreaker) checkStateTransition() {
	if cb.state == CircuitOpen && time.Since(cb.lastFailureTime) >= cb.cfg.ResetTimeout {
		cb.state = CircuitHalfOpen
		cb.successes = 0
	}
}

// ErrCircuitOpen is returned when an operation is rejected because the circuit breaker is open.
var ErrCircuitOpen = fmt.Errorf("circuit breaker is open")

// RetryWithCircuitBreaker executes fn with retry logic and circuit breaker protection.
// If the circuit is open, it returns ErrCircuitOpen immediately. On success or failure,
// the circuit breaker is updated accordingly.
func RetryWithCircuitBreaker(ctx context.Context, retryCfg RetryConfig, cb *CircuitBreaker, fn RetryableFunc) error {
	if !cb.Allow() {
		return ErrCircuitOpen
	}

	err := Retry(ctx, retryCfg, func(ctx context.Context) error {
		if !cb.Allow() {
			return ErrCircuitOpen
		}
		return fn(ctx)
	})

	if err != nil {
		cb.RecordFailure()
	} else {
		cb.RecordSuccess()
	}
	return err
}
