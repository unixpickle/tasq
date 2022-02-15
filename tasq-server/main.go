package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/unixpickle/essentials"
)

func main() {
	var addr string
	var timeout time.Duration
	flag.StringVar(&addr, "addr", ":8080", "address to listen on")
	flag.DurationVar(&timeout, "timeout", time.Minute*15, "timeout of individual tasks")
	flag.Parse()

	s := &Server{
		Queues: NewQueueState(timeout),
	}
	http.HandleFunc("/", s.ServeIndex)
	http.HandleFunc("/counts", s.ServeCounts)
	http.HandleFunc("/task/push", s.ServePushTask)
	http.HandleFunc("/task/push_batch", s.ServePushBatch)
	http.HandleFunc("/task/pop", s.ServePopTask)
	http.HandleFunc("/task/pop_batch", s.ServePopBatch)
	http.HandleFunc("/task/peek", s.ServePeekTask)
	http.HandleFunc("/task/completed", s.ServeCompletedTask)
	http.HandleFunc("/task/completed_batch", s.ServeCompletedBatch)
	http.HandleFunc("/task/keepalive", s.ServeKeepalive)
	http.HandleFunc("/task/clear", s.ServeClearTasks)
	http.HandleFunc("/task/expire_all", s.ServeExpireTasks)
	http.HandleFunc("/task/queue_expired", s.ServeQueueExpired)
	essentials.Must(http.ListenAndServe(addr, nil))
}

type Server struct {
	Queues *QueueState
}

func (s *Server) ServeIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" || r.URL.Path == "" {
		w.Header().Set("content-type", "text/plain")
		pending, running, completed := s.Queues.Counts()
		fmt.Fprintf(w, "Pending tasks: %d\n", pending)
		fmt.Fprintf(w, "In-progress tasks: %d\n", running)
		fmt.Fprintf(w, "Completed tasks: %d\n", completed)
	} else {
		w.Header().Set("content-type", "text/html")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(w, "<html><body>Page not found</body></html>")
	}
}

func (s *Server) ServeCounts(w http.ResponseWriter, r *http.Request) {
	pending, running, completed := s.Queues.Counts()
	serveObject(w, map[string]int64{"pending": pending, "running": running, "completed": completed})
}

func (s *Server) ServePushTask(w http.ResponseWriter, r *http.Request) {
	contents := r.FormValue("contents")
	if contents == "" {
		serveError(w, "must specify non-empty `contents` parameter")
	} else {
		serveObject(w, s.Queues.Push(contents))
	}
}

func (s *Server) ServePushBatch(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	var contents []string
	if err := json.Unmarshal(data, &contents); err != nil {
		serveError(w, err.Error())
	} else {
		ids := []string{}
		for _, c := range contents {
			ids = append(ids, s.Queues.Push(c))
		}
		serveObject(w, ids)
	}
}

func (s *Server) ServePopTask(w http.ResponseWriter, r *http.Request) {
	task, nextTry := s.Queues.Pop()
	if task != nil {
		serveObject(w, task)
	} else {
		if nextTry != nil {
			timeout := (*nextTry).Sub(time.Now())
			serveObject(w, map[string]interface{}{
				"done":  false,
				"retry": math.Max(0, timeout.Seconds()),
			})
		} else {
			serveObject(w, map[string]interface{}{"done": true})
		}
	}
}

func (s *Server) ServePopBatch(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.Atoi(r.FormValue("count"))
	if err != nil {
		serveError(w, "invalid 'count' parameter: "+err.Error())
		return
	} else if n <= 0 {
		serveError(w, "invalid 'count' requested")
		return
	}

	tasks, nextTry := s.Queues.PopBatch(n)

	result := map[string]interface{}{
		"done": len(tasks) == 0 && nextTry == nil,
	}
	if nextTry != nil {
		timeout := (*nextTry).Sub(time.Now())
		result["retry"] = math.Max(0, timeout.Seconds())
	}
	if tasks == nil {
		// Prevent a null value in the JSON field.
		tasks = []*Task{}
	}
	result["tasks"] = tasks

	serveObject(w, result)
}

func (s *Server) ServePeekTask(w http.ResponseWriter, r *http.Request) {
	task, nextTask, nextTime := s.Queues.Peek()
	if task != nil {
		serveObject(w, map[string]interface{}{"contents": task.Contents, "id": task.ID})
	} else {
		if nextTask != nil {
			timeout := (*nextTime).Sub(time.Now())
			serveObject(w, map[string]interface{}{
				"done":  false,
				"retry": math.Max(0, timeout.Seconds()),
				"next": map[string]interface{}{
					"contents": nextTask.Contents,
					"id":       nextTask.ID,
				},
			})
		} else {
			serveObject(w, map[string]interface{}{"done": true})
		}
	}
}

func (s *Server) ServeCompletedTask(w http.ResponseWriter, r *http.Request) {
	if s.Queues.Completed(r.FormValue("id")) {
		serveObject(w, true)
	} else {
		serveError(w, "there was no in-progress task with the specified `id`")
	}
}

func (s *Server) ServeCompletedBatch(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		serveError(w, err.Error())
	} else {
		var failures []string
		for _, id := range ids {
			if !s.Queues.Completed(id) {
				failures = append(failures, id)
			}
		}
		if len(failures) > 0 {
			serveError(w, "there were no in-progress tasks with the specified ids: "+
				strings.Join(failures, ", "))
		} else {
			serveObject(w, true)
		}
	}
}

func (s *Server) ServeKeepalive(w http.ResponseWriter, r *http.Request) {
	if s.Queues.Keepalive(r.FormValue("id")) {
		serveObject(w, true)
	} else {
		serveError(w, "there was no in-progress task with the specified `id`")
	}
}

func (s *Server) ServeClearTasks(w http.ResponseWriter, r *http.Request) {
	s.Queues.Clear()
	serveObject(w, true)
}

func (s *Server) ServeExpireTasks(w http.ResponseWriter, r *http.Request) {
	n := s.Queues.ExpireAll()
	serveObject(w, n)
}

func (s *Server) ServeQueueExpired(w http.ResponseWriter, r *http.Request) {
	n := s.Queues.QueueExpired()
	serveObject(w, n)
}

func serveObject(w http.ResponseWriter, obj interface{}) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"data": obj})
}

func serveError(w http.ResponseWriter, err string) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"error": err})
}
