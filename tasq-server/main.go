package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"time"
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
	http.HandleFunc("/task/push", s.ServePushTask)
	http.HandleFunc("/task/push_batch", s.ServePushBatch)
	http.HandleFunc("/task/pop", s.ServePopTask)
	http.HandleFunc("/task/completed", s.ServeCompletedTask)
	http.HandleFunc("/task/clear", s.ServeClearTasks)
	http.HandleFunc("/task/expire_all", s.ServeExpireTasks)
	http.ListenAndServe(addr, nil)
}

type Server struct {
	Queues *QueueState
}

func (s *Server) ServeIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" || r.URL.Path == "" {
		w.Header().Set("content-type", "text/plain")
		fmt.Fprintf(w, "Pending tasks: %d\n", s.Queues.Pending.Len())
		fmt.Fprintf(w, "In-progress tasks: %d\n", s.Queues.Running.Len())
		fmt.Fprintf(w, "Completed tasks: %d\n", s.Queues.NumCompleted())
	} else {
		w.Header().Set("content-type", "text/html")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(w, "<html><body>Page not found</body></html>")
	}
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
		serveObject(w, map[string]interface{}{"contents": task.Contents, "id": task.ID})
	} else {
		if nextTry != nil {
			timeout := (*nextTry).Sub(time.Now())
			serveObject(w, map[string]interface{}{"done": false, "retry": math.Max(0, timeout.Seconds())})
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

func (s *Server) ServeClearTasks(w http.ResponseWriter, r *http.Request) {
	s.Queues.Pending.Clear()
	s.Queues.Running.Clear()
	serveObject(w, true)
}

func (s *Server) ServeExpireTasks(w http.ResponseWriter, r *http.Request) {
	s.Queues.Running.ExpireAll()
	serveObject(w, true)
}

func serveObject(w http.ResponseWriter, obj interface{}) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"data": obj})
}

func serveError(w http.ResponseWriter, err string) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"error": err})
}
