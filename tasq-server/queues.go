package main

import (
	"strconv"
	"sync"
	"time"
)

// QueueState wraps the pending and running queues in a single object.
type QueueState struct {
	Pending *PendingQueue
	Running *RunningQueue

	completionLock    sync.RWMutex
	completionCounter int64
}

func (q *QueueState) Push(contents string) string {
	return q.Pending.AddTask(contents).ID
}

func (q *QueueState) Pop() (*Task, *time.Time) {
	nextPending := q.Pending.PopTask()
	if nextPending != nil {
		q.Running.StartedTask(nextPending)
		return nextPending, nil
	}

	nextExpired, nextTry := q.Running.PopExpired()
	if nextExpired != nil {
		q.Running.StartedTask(nextExpired)
		return nextExpired, nil
	}

	return nil, nextTry
}

func (q *QueueState) Completed(id string) bool {
	res := q.Running.Completed(id) != nil
	if res {
		q.completionLock.Lock()
		q.completionCounter += 1
		q.completionLock.Unlock()
	}
	return res
}

func (q *QueueState) NumCompleted() int64 {
	q.completionLock.RLock()
	defer q.completionLock.RUnlock()
	return q.completionCounter
}

// NewQueueState creates empty queues with the given task timeout.
func NewQueueState(timeout time.Duration) *QueueState {
	return &QueueState{
		Pending: NewPendingQueue(),
		Running: NewRunningQueue(timeout),
	}
}

type PendingQueue struct {
	lock  sync.Mutex
	deque *TaskDeque
	curID int64
}

func NewPendingQueue() *PendingQueue {
	return &PendingQueue{deque: &TaskDeque{}}
}

// AddTask creates a new task with the given contents and enqueues it.
func (p *PendingQueue) AddTask(contents string) *Task {
	p.lock.Lock()
	defer p.lock.Unlock()

	task := &Task{
		Contents: contents,
		ID:       strconv.FormatInt(p.curID, 16),
	}
	p.curID += 1
	p.deque.PushLast(task)
	return task
}

// PopTask gets the next task (in FIFO order).
func (p *PendingQueue) PopTask() *Task {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.deque.PopFirst()
}

// Len gets the number of queued tasks.
func (p *PendingQueue) Len() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.deque.Len()
}

type RunningQueue struct {
	lock     sync.Mutex
	idToTask map[string]*Task
	deque    *TaskDeque
	timeout  time.Duration
}

func NewRunningQueue(timeout time.Duration) *RunningQueue {
	return &RunningQueue{
		idToTask: map[string]*Task{},
		deque:    &TaskDeque{},
		timeout:  timeout,
	}
}

// StartedTask adds the task to the queue and sets its timeout accordingly.
func (r *RunningQueue) StartedTask(t *Task) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.idToTask[t.ID] = t
	r.deque.PushLast(t)
	t.expiration = time.Now().Add(r.timeout)
}

// PopExpired removes the first timed out task from the queue and returns it.
//
// If no tasks are timed out, the second return argument specifies the next
// time when a task is set to expire (if there is one).
func (r *RunningQueue) PopExpired() (*Task, *time.Time) {
	r.lock.Lock()
	defer r.lock.Unlock()
	task := r.deque.PeekFirst()
	if task == nil {
		return nil, nil
	}
	now := time.Now()
	if task.expiration.After(now) {
		exp := task.expiration
		return nil, &exp
	} else {
		r.deque.Remove(task)
		delete(r.idToTask, task.ID)
		return task, nil
	}
}

// Completed removes a task from the queue.
//
// If the task is no longer in the queue, for example if it was removed with
// PopExpired(), this returns nil.
func (r *RunningQueue) Completed(id string) *Task {
	r.lock.Lock()
	defer r.lock.Unlock()
	task, ok := r.idToTask[id]
	if !ok {
		return nil
	}
	r.deque.Remove(task)
	delete(r.idToTask, id)
	return task
}

// Len gets the number of tasks in the queue.
func (r *RunningQueue) Len() int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.deque.Len()
}
