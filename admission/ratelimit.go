package admission

import (
	"sync"
	"time"

	eventflow "github.com/rezarajan/eventflow"
)

// RateLimiter enforces a per-principal fixed-window admission limit.
// It is intentionally outside Evaluate so policy evaluation remains side-effect free.
type RateLimiter struct {
	mu      sync.Mutex
	windows map[string]rateWindow
}

type rateWindow struct {
	start time.Time
	count int
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{windows: map[string]rateWindow{}}
}

func (l *RateLimiter) Check(principal string, limit int, now time.Time, policy Policy) (eventflow.Decision, bool) {
	if limit <= 0 {
		return eventflow.Decision{}, false
	}
	key := principal
	if key == "" {
		key = "<anonymous>"
	}
	start := now.UTC().Truncate(time.Minute)
	l.mu.Lock()
	defer l.mu.Unlock()
	window := l.windows[key]
	if window.start.IsZero() || !window.start.Equal(start) {
		window = rateWindow{start: start}
	}
	window.count++
	l.windows[key] = window
	if window.count > limit {
		return eventflow.Rejected(eventflow.ReasonRateLimitExceeded, "principal exceeded the configured admission rate limit", "principal", principal, policy.Name, policy.Version), true
	}
	return eventflow.Decision{}, false
}
