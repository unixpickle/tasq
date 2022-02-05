package main

import (
	"strconv"
	"sync"
	"time"
)

// QueueState wraps the pending and running queues in a single object.
type QueueState struct {
	lock    sync.RWMutex
	pending *PendingQueue
	running *RunningQueue

	completionCounter int64
}

// NewQueueState creates empty queues with the given task timeout.
func NewQueueState(timeout time.Duration) *QueueState {
	return &QueueState{
		pending: NewPendingQueue(),
		running: NewRunningQueue(timeout),
	}
}

func (q *QueueState) Push(contents string) string {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.pending.AddTask(contents).ID
}

func (q *QueueState) Pop() (*Task, *time.Time) {
	q.lock.Lock()
	defer q.lock.Unlock()
	nextPending := q.pending.PopTask()
	if nextPending != nil {
		q.running.StartedTask(nextPending)
		return nextPending, nil
	}

	nextExpired, nextTry := q.running.PopExpired()
	if nextExpired != nil {
		q.running.StartedTask(nextExpired)
		return nextExpired, nil
	}

	return nil, nextTry
}

func (q *QueueState) Peek() (*Task, *Task, *time.Time) {
	q.lock.Lock()
	defer q.lock.Unlock()
	nextPending := q.pending.PeekTask()
	if nextPending != nil {
		return nextPending, nil, nil
	}
	return q.running.PeekExpired()
}

func (q *QueueState) Completed(id string) bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	res := q.running.Completed(id) != nil
	if res {
		q.completionCounter += 1
	}
	return res
}

func (q *QueueState) Counts() (pending, running, completed int64) {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return int64(q.pending.Len()), int64(q.running.Len()), q.completionCounter
}

func (q *QueueState) Clear() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.pending.Clear()
	q.running.Clear()
}

func (q *QueueState) ExpireAll() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.running.ExpireAll()
}

func (q *QueueState) QueueExpired() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	n := 0
	for {
		task, _ := q.running.PopExpired()
		if task == nil {
			break
		}
		n += 1
		q.pending.PushTask(task)
	}
	return n
}

type PendingQueue struct {
	deque *TaskDeque
	curID int64
}

func NewPendingQueue() *PendingQueue {
	return &PendingQueue{deque: &TaskDeque{}}
}

// AddTask creates a new task with the given contents and enqueues it.
func (p *PendingQueue) AddTask(contents string) *Task {
	task := &Task{
		Contents: contents,
		ID:       strconv.FormatInt(p.curID, 16),
	}
	p.curID += 1
	p.deque.PushLast(task)
	return task
}

// PushTask re-enqueues an existing task.
func (p *PendingQueue) PushTask(t *Task) {
	p.deque.PushLast(t)
}

// PopTask gets the next task (in FIFO order).
func (p *PendingQueue) PopTask() *Task {
	return p.deque.PopFirst()
}

// PeekTask gets a copy of the next task.
//
// The copy only includes visible metadata. It will have no connection to the
// queue or the original task.
func (p *PendingQueue) PeekTask() *Task {
	t := p.deque.PeekFirst()
	if t == nil {
		return nil
	}
	return t.DisconnectedCopy()
}

// Len gets the number of queued tasks.
func (p *PendingQueue) Len() int {
	return p.deque.Len()
}

// Clear deletes all of the pending tasks.
func (p *PendingQueue) Clear() {
	p.deque = &TaskDeque{}
}

type RunningQueue struct {
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
	r.idToTask[t.ID] = t
	r.deque.PushLast(t)
	t.expiration = time.Now().Add(r.timeout)
}

// PopExpired removes the first timed out task from the queue and returns it.
//
// If no tasks are timed out, the second return argument specifies the next
// time when a task is set to expire (if there is one).
func (r *RunningQueue) PopExpired() (*Task, *time.Time) {
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

// PeekExpired returns a copy of the first timed out task or the next task that
// will expire in the queue.
//
// If no tasks are timed out, the second return value is the next task to
// expire, and the third is the time when it will expire.
//
// If no tasks are enqueued (expired or not) all return values are nil.
//
// The returned tasks only include visible metadata. They will have no
// connection to the queue or the original task.
func (r *RunningQueue) PeekExpired() (*Task, *Task, *time.Time) {
	task := r.deque.PeekFirst()
	if task == nil {
		return nil, nil, nil
	}
	now := time.Now()
	if task.expiration.After(now) {
		exp := task.expiration
		return nil, task.DisconnectedCopy(), &exp
	} else {
		return task.DisconnectedCopy(), nil, nil
	}
}

// Completed removes a task from the queue.
//
// If the task is no longer in the queue, for example if it was removed with
// PopExpired(), this returns nil.
func (r *RunningQueue) Completed(id string) *Task {
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
	return r.deque.Len()
}

// ExpireAll changes the timeout for all tasks to be before now.
func (r *RunningQueue) ExpireAll() int {
	n := 0
	for _, task := range r.idToTask {
		n += 1
		task.expiration = time.Time{}
	}
	return n
}

// Clear deletes all of the running tasks.
func (r *RunningQueue) Clear() {
	r.idToTask = map[string]*Task{}
	r.deque = &TaskDeque{}
}
