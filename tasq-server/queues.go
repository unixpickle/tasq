package main

import (
	"archive/zip"
	"encoding/json"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/unixpickle/essentials"
)

// QueueStateMux manages multiple (named) QueueStates.
type QueueStateMux struct {
	saveLock sync.RWMutex
	lock     sync.Mutex
	queues   map[string]*QueueState
	users    map[string]int
	timeout  time.Duration
}

// NewQueueStateMux creates a QueueStateMux with the given task timeout.
func NewQueueStateMux(timeout time.Duration) *QueueStateMux {
	return &QueueStateMux{
		queues:  map[string]*QueueState{},
		users:   map[string]int{},
		timeout: timeout,
	}
}

// DeserializeQueueStateMux reads a file written by QueueStateMux.Serialize().
func DeserializeQueueStateMux(timeout time.Duration, r io.ReaderAt,
	size int64) (*QueueStateMux, error) {
	const context = "deserialize queue state"
	res := NewQueueStateMux(timeout)

	zf, err := zip.NewReader(r, size)
	if err != nil {
		return nil, errors.Wrap(err, context)
	}
	for _, file := range zf.File {
		subReader, err := file.Open()
		if err != nil {
			return nil, errors.Wrap(err, context)
		}
		var dictObj ContextState
		err = json.NewDecoder(subReader).Decode(&dictObj)
		subReader.Close()
		if err != nil {
			subReader.Close()
			return nil, errors.Wrap(err, context)
		}
		res.queues[dictObj.Name] = DecodeQueueState(dictObj.Encoded)
		res.users[dictObj.Name] = 0
	}
	return res, nil
}

// ReadQueueStateMux is like DeserializeQueueStateMux(), but reads from a local
// file instead of an arbitrary reader.
func ReadQueueStateMux(timeout time.Duration, path string) (*QueueStateMux, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return DeserializeQueueStateMux(timeout, r, stat.Size())
}

// Get calls f with a QueueState for the given name. One is created if
// necessary, and will be destroyed when the queue is cleared.
//
// The QueueState should not be accessed outside of f. In particular, f should
// not store a reference to the QueueState anywhere outside of its scope.
func (q *QueueStateMux) Get(name string, f func(*QueueState)) {
	q.saveLock.RLock()
	defer q.saveLock.RUnlock()

	q.lock.Lock()
	qs, ok := q.queues[name]
	if !ok {
		qs = NewQueueState(q.timeout)
		q.queues[name] = qs
	}
	q.users[name]++
	q.lock.Unlock()

	defer func() {
		q.lock.Lock()
		defer q.lock.Unlock()
		q.users[name]--
		if q.users[name] == 0 && qs.Cleared() {
			// Garbage collect unused queues.
			delete(q.users, name)
			delete(q.queues, name)
		}
	}()

	f(qs)
}

// Iterate calls f with every non-empty QueueState in q.
func (q *QueueStateMux) Iterate(f func(string, *QueueState)) {
	q.saveLock.RLock()
	defer q.saveLock.RUnlock()

	q.lock.Lock()
	names := make([]string, 0, len(q.queues))
	for name := range q.queues {
		names = append(names, name)
	}
	q.lock.Unlock()
	sort.Strings(names)
	for _, name := range names {
		q.Get(name, func(qs *QueueState) {
			f(name, qs)
		})
	}
}

// Serialize writes the contents of the queue to a file, blocking all
// operations on all queues to make sure cross-queue consistent state.
func (q *QueueStateMux) Serialize(w io.Writer) error {
	q.saveLock.Lock()
	var states []ContextState
	for name, q := range q.queues {
		states = append(states, ContextState{
			Name:    name,
			Encoded: q.Encode(),
		})
	}
	q.saveLock.Unlock()

	const context = "serialize queue state"

	resultWriter := zip.NewWriter(w)
	for i, state := range states {
		rw, err := resultWriter.Create(strconv.Itoa(i) + ".json")
		if err != nil {
			return errors.Wrap(err, context)
		}
		if err := json.NewEncoder(rw).Encode(state); err != nil {
			return errors.Wrap(err, context)
		}
	}

	if err := resultWriter.Close(); err != nil {
		return errors.Wrap(err, context)
	}

	return nil
}

// QueueState maintains two queues of tasks: a pending queue and a running
// queue.
//
// Tasks are added to the pending queue via Push(). When a task is returned
// from Pop(), it is moved to the running queue and given an expiration time.
// In general, Pop() first checks for tasks in the pending queue, and only
// attempts to re-use an expired task from the running queue if necessary.
// When Completed() is called for a task, it is removed from the running queue,
// preventing it from ever being returned by Pop() again.
// Tasks may be marked as completed at any time while they are in the running
// queue, even if they are expired.
type QueueState struct {
	lock    sync.RWMutex
	pending *PendingQueue
	running *RunningQueue

	completionCounter int64
	rateTracker       *RateTracker
}

// NewQueueState creates empty queues with the given task timeout.
func NewQueueState(timeout time.Duration) *QueueState {
	return &QueueState{
		pending:     NewPendingQueue(),
		running:     NewRunningQueue(timeout),
		rateTracker: NewRateTracker(0),
	}
}

// DecodeQueueState decodes an object from QueueState.Encode()
func DecodeQueueState(obj *EncodedQueueState) *QueueState {
	return &QueueState{
		pending:           DecodePendingQueue(obj.Pending),
		running:           DecodeRunningQueue(obj.Running),
		completionCounter: obj.Completed,
		rateTracker:       DecodeRateTracker(&obj.RateTracker),
	}
}

// Encode converts q into a JSON-serializable object.
func (q *QueueState) Encode() *EncodedQueueState {
	q.lock.Lock()
	defer q.lock.Unlock()
	return &EncodedQueueState{
		Pending:     q.pending.Encode(),
		Running:     q.running.Encode(),
		Completed:   q.completionCounter,
		RateTracker: *q.rateTracker.Encode(),
	}
}

// Push creates a task and returns the its new ID.
func (q *QueueState) Push(contents string) string {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.pending.AddTask(contents).ID
}

// Pop gets a task from the queue, preferring the pending queue and dipping
// into the expired tasks in the running queue only if necessary.
func (q *QueueState) Pop(timeout *time.Duration) (*Task, *time.Time) {
	q.lock.Lock()
	defer q.lock.Unlock()
	nextPending := q.pending.PopTask()
	if nextPending != nil {
		q.running.StartedTask(nextPending, timeout)
		return nextPending, nil
	}

	nextExpired, nextTry := q.running.PopExpired()
	if nextExpired != nil {
		q.running.StartedTask(nextExpired, timeout)
		return nextExpired, nil
	}

	return nil, nextTry
}

// PopBatch atomically pops at most n tasks from the queue.
//
// If fewer than n tasks are returned, the second return value is the time that
// the next running task will expire, or nil if no tasks were running before
// PopBatch was called.
func (q *QueueState) PopBatch(n int, timeout *time.Duration) ([]*Task, *time.Time) {
	q.lock.Lock()
	defer q.lock.Unlock()

	var tasks []*Task
	for len(tasks) < n {
		t := q.pending.PopTask()
		if t == nil {
			break
		}
		tasks = append(tasks, t)
	}
	var nextTry *time.Time
	for len(tasks) < n {
		var t *Task
		t, nextTry = q.running.PopExpired()
		if t == nil {
			break
		}
		tasks = append(tasks, t)
	}

	for _, t := range tasks {
		q.running.StartedTask(t, timeout)
	}

	return tasks, nextTry
}

// Peek gets the next available task to pop, if there is one.
//
// If no task is currently available, Peek returns the next task to expire and
// the time when it will expire, or nil if no tasks are running.
func (q *QueueState) Peek() (*Task, *Task, *time.Time) {
	q.lock.Lock()
	defer q.lock.Unlock()
	nextPending := q.pending.PeekTask()
	if nextPending != nil {
		return nextPending, nil, nil
	}
	return q.running.PeekExpired()
}

// Completed marks the identified task as complete, or returns false if no task
// with the given ID was in the running queue.
func (q *QueueState) Completed(id string) bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	res := q.running.Completed(id) != nil
	if res {
		q.completionCounter += 1
		q.rateTracker.Add(1)
	}
	return res
}

// Keepalive restarts the timeout period for the identified task, or returns
// false if no task with the given ID was in the running queue.
func (q *QueueState) Keepalive(id string, timeout *time.Duration) bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.running.Keepalive(id, timeout)
}

// Counts gets the current number of tasks in each state.
func (q *QueueState) Counts(rateSeconds int) *QueueCounts {
	q.lock.RLock()
	defer q.lock.RUnlock()
	runningTotal := q.running.Len()
	runningExpired := q.running.NumExpired()
	var rate *float64
	if rateSeconds > 0 {
		rateSeconds = essentials.MinInt(rateSeconds, q.rateTracker.HistorySize())
		r := float64(q.rateTracker.Count(rateSeconds)) / float64(rateSeconds)
		rate = &r
	}
	return &QueueCounts{
		Pending:   int64(q.pending.Len()),
		Running:   int64(runningTotal - runningExpired),
		Expired:   int64(runningExpired),
		Completed: q.completionCounter,
		Rate:      rate,
	}
}

// Clear empties the queues and resets the completion counter.
func (q *QueueState) Clear() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.pending.Clear()
	q.running.Clear()
	q.completionCounter = 0
	q.rateTracker.Reset()
}

// Cleared returns true if the queue is effectively a fresh object, containing
// no running tasks and zero completed tasks.
func (q *QueueState) Cleared() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return q.pending.Len() == 0 && q.running.Len() == 0 && q.completionCounter == 0
}

// ExpireAll marks all tasks as expired, allowing them to be immediately popped
// from the running queue.
//
// It does not move the tasks back to the pending queue. For this, call
// QueueExpired().
func (q *QueueState) ExpireAll() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.running.ExpireAll()
}

// QueueExpired puts expired tasks from the running queue back into the pending
// queue.
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

// DecodePendingQueue decodes an object from PendingQueue.Encode().
func DecodePendingQueue(obj *EncodedPendingQueue) *PendingQueue {
	return &PendingQueue{
		deque: DecodeTaskDeque(obj.Deque),
		curID: obj.CurID,
	}
}

// Encode converts p into a JSON-serializable object.
func (p *PendingQueue) Encode() *EncodedPendingQueue {
	return &EncodedPendingQueue{
		Deque: p.deque.Encode(),
		CurID: p.curID,
	}
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

// DecodeRunningQueue decodes an object from RunningQueue.Encode().
func DecodeRunningQueue(obj *EncodedRunningQueue) *RunningQueue {
	deque := DecodeTaskDeque(obj.Deque)
	idToTask := map[string]*Task{}
	deque.Iterate(func(t *Task) {
		idToTask[t.ID] = t
	})
	return &RunningQueue{
		idToTask: idToTask,
		deque:    deque,
		timeout:  obj.Timeout,
	}
}

// Encode converts the queue into a JSON-serializable object.
func (r *RunningQueue) Encode() *EncodedRunningQueue {
	return &EncodedRunningQueue{
		Deque:   r.deque.Encode(),
		Timeout: r.timeout,
	}
}

// StartedTask adds the task to the queue and sets its timeout accordingly.
func (r *RunningQueue) StartedTask(t *Task, timeout *time.Duration) {
	r.idToTask[t.ID] = t
	if timeout == nil {
		timeout = &r.timeout
	}
	t.expiration = time.Now().Add(*timeout)
	r.deque.PushByExpiration(t)
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

// Keepalive restarts the timeout period for the identified task.
//
// Returns true if the task was found, or false otherwise.
func (r *RunningQueue) Keepalive(id string, timeout *time.Duration) bool {
	task, ok := r.idToTask[id]
	if !ok {
		return false
	}
	r.deque.Remove(task)
	r.StartedTask(task, timeout)
	return true
}

// Len gets the number of tasks in the queue.
func (r *RunningQueue) Len() int {
	return r.deque.Len()
}

// NumExpired gets the number of expired tasks.
func (r *RunningQueue) NumExpired() int {
	now := time.Now()
	task := r.deque.first
	n := 0
	for task != nil && !task.expiration.After(now) {
		n++
		task = task.queueNext
	}
	return n
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

type QueueCounts struct {
	Pending   int64    `json:"pending"`
	Running   int64    `json:"running"`
	Expired   int64    `json:"expired"`
	Completed int64    `json:"completed"`
	Rate      *float64 `json:"rate,omitempty"`
}

type ContextState struct {
	Name    string
	Encoded *EncodedQueueState
}

type EncodedQueueState struct {
	Pending     *EncodedPendingQueue
	Running     *EncodedRunningQueue
	Completed   int64
	RateTracker EncodedRateTracker
}

type EncodedPendingQueue struct {
	Deque []EncodedTask
	CurID int64
}

type EncodedRunningQueue struct {
	Deque   []EncodedTask
	Timeout time.Duration
}
