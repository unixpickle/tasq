package main

import (
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestQueueStatePopAttempts(t *testing.T) {
	qs := NewQueueState(time.Minute)
	id, ok := qs.Push("task-contents", 0)
	if !ok {
		t.Fatal("task should have been pushed")
	}

	task, nextTry := qs.Pop(nil)
	if task == nil || nextTry != nil {
		t.Fatal("expected a task from first pop")
	}
	if task.ID != id {
		t.Fatalf("wrong task id: %q", task.ID)
	}
	if task.numAttempts != 1 {
		t.Fatalf("expected one attempt after first pop, got %d", task.numAttempts)
	}

	if !qs.Keepalive(id, nil) {
		t.Fatal("expected keepalive to succeed")
	}
	if task.numAttempts != 1 {
		t.Fatalf("keepalive should not increment attempts, got %d", task.numAttempts)
	}

	if !qs.Expire(id) {
		t.Fatal("expected expire to succeed")
	}
	task, nextTry = qs.Pop(nil)
	if task == nil || nextTry != nil {
		t.Fatal("expected expired task to be repopped")
	}
	if task.numAttempts != 2 {
		t.Fatalf("expected two attempts after repop, got %d", task.numAttempts)
	}
}

func TestQueueStatePopAttemptsAfterTimeout(t *testing.T) {
	qs := NewQueueState(time.Minute)
	id, ok := qs.Push("task-contents", 0)
	if !ok {
		t.Fatal("task should have been pushed")
	}

	popTimeout := 5 * time.Millisecond
	task, nextTry := qs.Pop(&popTimeout)
	if task == nil || nextTry != nil {
		t.Fatal("expected a task from first pop")
	}
	if task.ID != id {
		t.Fatalf("wrong task id: %q", task.ID)
	}
	if task.numAttempts != 1 {
		t.Fatalf("expected one attempt after first pop, got %d", task.numAttempts)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		task, nextTry = qs.Pop(nil)
		if task != nil {
			break
		}
		if nextTry != nil {
			time.Sleep(time.Until(*nextTry) + time.Millisecond)
		} else {
			time.Sleep(time.Millisecond)
		}
	}
	if task == nil {
		t.Fatal("timed out waiting for task to naturally expire and be repopped")
	}
	if task.ID != id {
		t.Fatalf("wrong task id after timeout: %q", task.ID)
	}
	if task.numAttempts != 2 {
		t.Fatalf("expected two attempts after timeout repop, got %d", task.numAttempts)
	}
}

func TestQueueStatePopBatchAttemptsAfterTimeout(t *testing.T) {
	qs := NewQueueState(time.Minute)
	id, ok := qs.Push("task-contents", 0)
	if !ok {
		t.Fatal("task should have been pushed")
	}

	popTimeout := 5 * time.Millisecond
	tasks, nextTry := qs.PopBatch(1, &popTimeout)
	if len(tasks) != 1 || nextTry != nil {
		t.Fatal("expected one task from first pop batch")
	}
	if tasks[0].ID != id {
		t.Fatalf("wrong task id: %q", tasks[0].ID)
	}
	if tasks[0].numAttempts != 1 {
		t.Fatalf("expected one attempt after first pop batch, got %d", tasks[0].numAttempts)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		tasks, nextTry = qs.PopBatch(1, nil)
		if len(tasks) > 0 {
			break
		}
		if nextTry != nil {
			time.Sleep(time.Until(*nextTry) + time.Millisecond)
		} else {
			time.Sleep(time.Millisecond)
		}
	}
	if len(tasks) != 1 {
		t.Fatal("timed out waiting for task to naturally expire and be repopped via pop batch")
	}
	if tasks[0].ID != id {
		t.Fatalf("wrong task id after timeout: %q", tasks[0].ID)
	}
	if tasks[0].numAttempts != 2 {
		t.Fatalf("expected two attempts after timeout pop batch repop, got %d", tasks[0].numAttempts)
	}
}

func TestQueueStateQueueExpiredDoesNotIncrementAttempts(t *testing.T) {
	qs := NewQueueState(time.Minute)
	id, ok := qs.Push("task-contents", 0)
	if !ok {
		t.Fatal("task should have been pushed")
	}

	task, nextTry := qs.Pop(nil)
	if task == nil || nextTry != nil {
		t.Fatal("expected a task from first pop")
	}
	if task.ID != id {
		t.Fatalf("wrong task id: %q", task.ID)
	}
	if task.numAttempts != 1 {
		t.Fatalf("expected one attempt after first pop, got %d", task.numAttempts)
	}

	if !qs.Expire(id) {
		t.Fatal("expected expire to succeed")
	}
	if n := qs.QueueExpired(); n != 1 {
		t.Fatalf("expected queue_expired to move one task, got %d", n)
	}

	task, nextTry = qs.Pop(nil)
	if task == nil || nextTry != nil {
		t.Fatal("expected queued expired task to be popped")
	}
	if task.ID != id {
		t.Fatalf("wrong task id after queue_expired: %q", task.ID)
	}
	if task.numAttempts != 2 {
		t.Fatalf("expected two attempts after queue_expired and repop, got %d", task.numAttempts)
	}
}

func TestServePopTaskIncludeAttempts(t *testing.T) {
	s := &Server{
		Queues: NewQueueStateMux(time.Minute),
	}
	if err := s.Queues.Get("", func(qs *QueueState) {
		qs.Push("task-1", 0)
		qs.Push("task-2", 0)
	}); err != nil {
		t.Fatalf("queue setup failed: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/task/pop", nil)
	s.ServePopTask(rec, req)
	var without struct {
		Data struct {
			ID                  string `json:"id"`
			Contents            string `json:"contents"`
			NumPreviousAttempts *int64 `json:"numPreviousAttempts,omitempty"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &without); err != nil {
		t.Fatalf("failed decoding response: %v", err)
	}
	if without.Data.ID == "" || without.Data.Contents == "" {
		t.Fatal("expected task fields in pop response")
	}
	if without.Data.NumPreviousAttempts != nil {
		t.Fatalf(
			"did not expect numPreviousAttempts without includePreviousAttempts flag: %#v",
			without.Data.NumPreviousAttempts,
		)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/task/pop?includePreviousAttempts=1", nil)
	s.ServePopTask(rec, req)
	var with struct {
		Data struct {
			ID                  string `json:"id"`
			Contents            string `json:"contents"`
			NumPreviousAttempts *int64 `json:"numPreviousAttempts,omitempty"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &with); err != nil {
		t.Fatalf("failed decoding response: %v", err)
	}
	if with.Data.NumPreviousAttempts == nil {
		t.Fatal("expected numPreviousAttempts with includePreviousAttempts flag")
	}
	if *with.Data.NumPreviousAttempts != 0 {
		t.Fatalf("expected numPreviousAttempts=0, got %d", *with.Data.NumPreviousAttempts)
	}
}

func TestServePopBatchIncludeAttempts(t *testing.T) {
	s := &Server{
		Queues: NewQueueStateMux(time.Minute),
	}
	if err := s.Queues.Get("", func(qs *QueueState) {
		qs.PushBatch([]string{"a", "b", "c"}, 0)
	}); err != nil {
		t.Fatalf("queue setup failed: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/task/pop_batch", strings.NewReader("count=2"))
	req.Header.Set("content-type", "application/x-www-form-urlencoded")
	s.ServePopBatch(rec, req)
	var without struct {
		Data struct {
			Done  bool `json:"done"`
			Tasks []struct {
				ID                  string `json:"id"`
				Contents            string `json:"contents"`
				NumPreviousAttempts *int64 `json:"numPreviousAttempts,omitempty"`
			} `json:"tasks"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &without); err != nil {
		t.Fatalf("failed decoding response: %v", err)
	}
	if without.Data.Done {
		t.Fatal("expected done=false when tasks are returned")
	}
	if len(without.Data.Tasks) != 2 {
		t.Fatalf("expected two tasks, got %d", len(without.Data.Tasks))
	}
	for i, task := range without.Data.Tasks {
		if task.NumPreviousAttempts != nil {
			t.Fatalf("did not expect numPreviousAttempts without includePreviousAttempts on task %d", i)
		}
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(
		"POST",
		"/task/pop_batch?includePreviousAttempts=1",
		strings.NewReader("count="+strconv.Itoa(1)),
	)
	req.Header.Set("content-type", "application/x-www-form-urlencoded")
	s.ServePopBatch(rec, req)
	var with struct {
		Data struct {
			Done  bool `json:"done"`
			Tasks []struct {
				ID                  string `json:"id"`
				Contents            string `json:"contents"`
				NumPreviousAttempts *int64 `json:"numPreviousAttempts,omitempty"`
			} `json:"tasks"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &with); err != nil {
		t.Fatalf("failed decoding response: %v", err)
	}
	if with.Data.Done {
		t.Fatal("expected done=false when tasks are returned")
	}
	if len(with.Data.Tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(with.Data.Tasks))
	}
	if with.Data.Tasks[0].NumPreviousAttempts == nil {
		t.Fatal("expected numPreviousAttempts with includePreviousAttempts flag")
	}
	if *with.Data.Tasks[0].NumPreviousAttempts != 0 {
		t.Fatalf("expected numPreviousAttempts=0, got %d", *with.Data.Tasks[0].NumPreviousAttempts)
	}
}
