package main

import "sync/atomic"

type SourceTask struct {
	Contents string
	ID       string
}

type TaskChan struct {
	Count int64
	Max   int64
	Chan  chan *SourceTask
}

func (t *TaskChan) KeepGoing() bool {
	if t.Max == -1 {
		return true
	}
	newValue := atomic.AddInt64(&t.Count, 1)
	return newValue <= t.Max
}
