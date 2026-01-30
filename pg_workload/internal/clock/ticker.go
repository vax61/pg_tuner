package clock

import "time"

// Ticker wraps time.Ticker to support both real and simulated clocks.
type Ticker struct {
	C          <-chan time.Time
	realTicker *time.Ticker
	done       <-chan struct{}
	stopCh     chan struct{}
}

// Stop stops the ticker.
func (t *Ticker) Stop() {
	if t.realTicker != nil {
		t.realTicker.Stop()
	}
	if t.stopCh != nil {
		select {
		case <-t.stopCh:
			// Already stopped
		default:
			close(t.stopCh)
		}
	}
}
