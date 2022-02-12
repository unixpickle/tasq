package tasq

import (
	"sync"
	"time"
)

// A RunningTask represents an in-progress task that is actively being
// performed by this process. Call Completed() on the object to mark it as
// complete.
//
// The object will automatically manage a background Goroutine that sends
// keepalives to the server until Completed() or Cancel() is called on it.
type RunningTask struct {
	Contents string
	ID       string

	client *Client

	cancelLock sync.Mutex
	cancelled  bool
	cancelChan chan struct{}
}

func newRunningTask(client *Client, contents, id string, interval time.Duration) *RunningTask {
	r := &RunningTask{
		Contents:   contents,
		ID:         id,
		client:     client,
		cancelChan: make(chan struct{}),
	}
	go r.keepaliveLoop(interval)
	return r
}

// Completed marks the task as complete and cancels the keepalive loop.
//
// Even if this returns an error, the keepalive loop will be stopped.
func (r *RunningTask) Completed() error {
	r.Cancel()
	return r.client.Completed(r.ID)
}

// Cancel the task's keepalive loop.
//
// This may be called any number of times, even if the task was completed,
// in which case it will have no effect after the first cancellation.
func (r *RunningTask) Cancel() {
	r.cancelLock.Lock()
	defer r.cancelLock.Unlock()
	if !r.cancelled {
		r.cancelled = true
		close(r.cancelChan)
	}
}

func (r *RunningTask) keepaliveLoop(interval time.Duration) {
	for {
		select {
		case <-time.After(interval):
		case <-r.cancelChan:
			return
		}
		r.client.Keepalive(r.ID)
	}
}
