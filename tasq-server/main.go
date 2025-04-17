package main

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/unixpickle/essentials"
)

func main() {
	var addr string
	var pathPrefix string
	var authUsername string
	var authPassword string
	var savePath string
	var saveInterval time.Duration
	var timeout time.Duration
	flag.StringVar(&addr, "addr", ":8080", "address to listen on")
	flag.StringVar(&pathPrefix, "path-prefix", "/", "prefix for URL paths")
	flag.StringVar(&authUsername, "auth-username", "", "username for basic auth")
	flag.StringVar(&authPassword, "auth-password", "", "password for basic auth")
	flag.StringVar(&savePath, "save-path", "", "if specified, path to periodically save state to")
	flag.DurationVar(&timeout, "timeout", time.Minute*15, "timeout of individual tasks")
	flag.DurationVar(&saveInterval, "save-interval", time.Minute*5, "time between saves")
	flag.Parse()

	if !strings.HasSuffix(pathPrefix, "/") || !strings.HasPrefix(pathPrefix, "/") {
		essentials.Die("path prefix must start and end with a '/' character")
	}

	s := &Server{
		PathPrefix:   pathPrefix,
		AuthUsername: authUsername,
		AuthPassword: authPassword,
		SavePath:     savePath,
		SaveInterval: saveInterval,
		StartTime:    time.Now(),
		Queues:       NewQueueStateMux(timeout),
	}
	http.HandleFunc(pathPrefix, s.ServeIndex)
	http.HandleFunc(pathPrefix+"summary", s.ServeSummary)
	http.HandleFunc(pathPrefix+"counts", s.ServeCounts)
	http.HandleFunc(pathPrefix+"stats", s.ServeStats)
	http.HandleFunc(pathPrefix+"task/push", s.ServePushTask)
	http.HandleFunc(pathPrefix+"task/push_batch", s.ServePushBatch)
	http.HandleFunc(pathPrefix+"task/pop", s.ServePopTask)
	http.HandleFunc(pathPrefix+"task/pop_batch", s.ServePopBatch)
	http.HandleFunc(pathPrefix+"task/peek", s.ServePeekTask)
	http.HandleFunc(pathPrefix+"task/completed", s.ServeCompletedTask)
	http.HandleFunc(pathPrefix+"task/completed_batch", s.ServeCompletedBatch)
	http.HandleFunc(pathPrefix+"task/keepalive", s.ServeKeepalive)
	http.HandleFunc(pathPrefix+"task/clear", s.ServeClearTasks)
	http.HandleFunc(pathPrefix+"task/expire_all", s.ServeExpireTasks)
	http.HandleFunc(pathPrefix+"task/queue_expired", s.ServeQueueExpired)
	s.SetupSaveLoop(timeout)
	essentials.Must(http.ListenAndServe(addr, nil))
}

type Server struct {
	PathPrefix   string
	AuthUsername string
	AuthPassword string
	Queues       *QueueStateMux
	SavePath     string
	SaveInterval time.Duration

	StartTime time.Time

	SaveStatsLock    sync.RWMutex
	LastSave         time.Time
	LastSaveDuration time.Duration

	SignalChan <-chan os.Signal
}

func (s *Server) ServeIndex(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	if r.URL.Path == s.PathPrefix || r.URL.Path+"/" == s.PathPrefix {
		w.Header().Set("content-type", "text/html")
		w.Write([]byte(Homepage))
	} else {
		w.Header().Set("content-type", "text/html")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(w, "<html><body>Page not found</body></html>")
	}
}

func (s *Server) ServeSummary(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	w.Header().Set("content-type", "text/plain")
	found := false
	buf := bytes.NewBuffer(nil)
	err := s.Queues.Iterate(func(name string, qs *QueueState) {
		found = true
		if name == "" {
			fmt.Fprint(buf, "---- Default context ----\n")
		} else {
			fmt.Fprintf(buf, "---- Context: %s ----\n", name)
		}
		counts := qs.Counts(0, false, true)
		fmt.Fprintf(buf, "    Pending: %d\n", counts.Pending)
		fmt.Fprintf(buf, "In progress: %d\n", counts.Running)
		fmt.Fprintf(buf, "    Expired: %d\n", counts.Expired)
		fmt.Fprintf(buf, "  Completed: %d\n", counts.Completed)
		fmt.Fprintf(buf, "      Bytes: %d\n", counts.Bytes)
	})
	if err != nil {
		fmt.Fprint(buf, err.Error())
		w.WriteHeader(http.StatusServiceUnavailable)
	} else if !found {
		fmt.Fprint(buf, "No active queues.")
	}
	w.Write(buf.Bytes())
}

func (s *Server) ServeCounts(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}

	var rateWindow int
	if s := r.URL.Query().Get("window"); s != "" {
		var err error
		rateWindow, err = strconv.Atoi(s)
		if err != nil {
			serveError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	includeModtime := r.URL.Query().Get("includeModtime") == "1"
	includeBytes := r.URL.Query().Get("includeBytes") == "1"

	if r.URL.Query().Get("all") == "1" {
		allNames := []string{}
		allCounts := []*QueueCounts{}
		err := s.Queues.Iterate(func(name string, qs *QueueState) {
			allNames = append(allNames, name)
			allCounts = append(allCounts, qs.Counts(rateWindow, includeModtime, includeBytes))
		})
		if err != nil {
			serveError(w, err.Error(), http.StatusServiceUnavailable)
		} else {
			serveObject(w, map[string]interface{}{
				"names":  allNames,
				"counts": allCounts,
			})
		}
		return
	}

	var obj interface{}
	err := s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		obj = qs.Counts(rateWindow, includeModtime, includeBytes)
	})
	if err != nil {
		serveError(w, err.Error(), http.StatusServiceUnavailable)
	} else {
		serveObject(w, obj)
	}
}

func (s *Server) ServeStats(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	s.SaveStatsLock.RLock()
	saveStats := map[string]interface{}{
		"elapsed": (time.Since(s.LastSave).Seconds()),
		"latency": s.LastSaveDuration.Seconds(),
	}
	s.SaveStatsLock.RUnlock()

	serveObject(w, map[string]interface{}{
		"uptime": time.Since(s.StartTime).Seconds(),
		"memory": map[string]interface{}{
			"alloc":      m.Alloc,
			"totalAlloc": m.TotalAlloc,
			"sys":        m.Sys,
			"lastGC":     float64(time.Now().UnixNano()-int64(m.LastGC)) / 1000000000.0,
		},
		"save": saveStats,
	})
}

func (s *Server) ServePushTask(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	contents := r.FormValue("contents")
	limit, err := parseLimit(r.FormValue("limit"))
	if err != nil {
		serveError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if contents == "" {
		serveError(w, "must specify non-empty `contents` parameter", http.StatusBadRequest)
	} else {
		var obj interface{}
		err := s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
			if id, ok := qs.Push(contents, limit); ok {
				obj = id
			}
		})
		if err != nil {
			serveError(w, err.Error(), http.StatusServiceUnavailable)
		} else {
			serveObject(w, obj)
		}
	}
}

func (s *Server) ServePushBatch(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	var contents []string
	if err := json.Unmarshal(data, &contents); err != nil {
		serveError(w, err.Error(), http.StatusBadRequest)
	} else {
		limit, err := parseLimit(r.URL.Query().Get("limit"))
		if err != nil {
			serveError(w, err.Error(), http.StatusBadRequest)
			return
		}
		var ids []string
		err = s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
			ids, _ = qs.PushBatch(contents, limit)
		})
		if err != nil {
			serveError(w, err.Error(), http.StatusServiceUnavailable)
		} else {
			serveObject(w, ids)
		}
	}
}

func (s *Server) ServePopTask(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	timeout, timeoutOk := s.TimeoutParam(w, r)
	if !timeoutOk {
		return
	}

	var task *Task
	var nextTry *time.Time
	err := s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		task, nextTry = qs.Pop(timeout)
	})
	if err != nil {
		serveError(w, err.Error(), http.StatusServiceUnavailable)
	} else if task != nil {
		serveObject(w, task)
	} else {
		if nextTry != nil {
			timeout := time.Until(*nextTry)
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
	if !s.BasicAuth(w, r) {
		return
	}
	timeout, timeoutOk := s.TimeoutParam(w, r)
	if !timeoutOk {
		return
	}

	n, err := strconv.Atoi(r.FormValue("count"))
	if err != nil {
		serveError(w, "invalid 'count' parameter: "+err.Error(), http.StatusBadRequest)
		return
	} else if n <= 0 {
		serveError(w, "invalid 'count' requested", http.StatusBadRequest)
		return
	}

	var tasks []*Task
	var nextTry *time.Time
	err = s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		tasks, nextTry = qs.PopBatch(n, timeout)
	})
	if err != nil {
		serveError(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	result := map[string]interface{}{
		"done": len(tasks) == 0 && nextTry == nil,
	}
	if nextTry != nil {
		timeout := time.Until(*nextTry)
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
	if !s.BasicAuth(w, r) {
		return
	}
	var task, nextTask *Task
	var nextTime *time.Time
	err := s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		task, nextTask, nextTime = qs.Peek()
	})
	if err != nil {
		serveError(w, err.Error(), http.StatusServiceUnavailable)
	} else if task != nil {
		serveObject(w, map[string]interface{}{"contents": task.Contents, "id": task.ID})
	} else {
		if nextTask != nil {
			timeout := time.Until(*nextTime)
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
	if !s.BasicAuth(w, r) {
		return
	}
	id := r.FormValue("id")
	var status bool
	err := s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		status = qs.Completed(id)
	})
	if err != nil {
		serveError(w, err.Error(), http.StatusServiceUnavailable)
	} else if status {
		serveObject(w, true)
	} else {
		serveError(w, "there was no in-progress task with the specified `id`", http.StatusOK)
	}
}

func (s *Server) ServeCompletedBatch(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		serveError(w, err.Error(), http.StatusBadRequest)
	} else {
		var failures []string
		err := s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
			for _, id := range ids {
				if !qs.Completed(id) {
					failures = append(failures, id)
				}
			}
		})
		if err != nil {
			serveError(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if len(failures) > 0 {
			serveError(w, "there were no in-progress tasks with the specified ids: "+
				strings.Join(failures, ", "), http.StatusOK)
		} else {
			serveObject(w, true)
		}
	}
}

func (s *Server) ServeKeepalive(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	timeout, timeoutOk := s.TimeoutParam(w, r)
	if !timeoutOk {
		return
	}
	id := r.FormValue("id")

	var status bool
	err := s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		status = qs.Keepalive(id, timeout)
	})
	if err != nil {
		serveError(w, err.Error(), http.StatusServiceUnavailable)
	} else if status {
		serveObject(w, true)
	} else {
		serveError(w, "there was no in-progress task with the specified `id`", http.StatusOK)
	}
}

func (s *Server) ServeClearTasks(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	err := s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		qs.Clear()
	})
	if err != nil {
		serveError(w, err.Error(), http.StatusServiceUnavailable)
	} else {
		serveObject(w, true)
	}
}

func (s *Server) ServeExpireTasks(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	var n int
	err := s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		n = qs.ExpireAll()
	})
	if err != nil {
		serveError(w, err.Error(), http.StatusServiceUnavailable)
	} else {
		serveObject(w, n)
	}
}

func (s *Server) ServeQueueExpired(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	var n int
	err := s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		n = qs.QueueExpired()
	})
	if err != nil {
		serveError(w, err.Error(), http.StatusServiceUnavailable)
	} else {
		serveObject(w, n)
	}
}

func (s *Server) BasicAuth(w http.ResponseWriter, r *http.Request) bool {
	if s.AuthUsername == "" && s.AuthPassword == "" {
		return true
	}
	username, password, ok := r.BasicAuth()
	if !ok {
		w.Header().Set("www-authenticate", `Basic realm="restricted", charset="UTF-8"`)
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error": "basic auth must be provided"}`))
		return false
	}
	if subtle.ConstantTimeCompare([]byte(username), []byte(s.AuthUsername)) == 1 &&
		subtle.ConstantTimeCompare([]byte(password), []byte(s.AuthPassword)) == 1 {
		return true
	} else {
		w.Header().Set("www-authenticate", `Basic realm="restricted", charset="UTF-8"`)
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error": "incorrect credentials"}`))
		return false
	}
}

func (s *Server) TimeoutParam(w http.ResponseWriter, r *http.Request) (*time.Duration, bool) {
	timeoutStr := r.URL.Query().Get("timeout")
	if timeoutStr == "" {
		return nil, true
	}
	parsed, err := strconv.ParseFloat(timeoutStr, 64)
	duration := time.Millisecond * time.Duration(parsed*1000)
	if err == nil && duration <= 0.0 {
		err = errors.New("timeout must be at least one millisecond")
	}
	if err != nil {
		w.Header().Set("www-authenticate", `Basic realm="restricted", charset="UTF-8"`)
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		data, _ := json.Marshal(map[string]string{"error": err.Error()})
		w.Write(data)
		return nil, false
	}
	return &duration, true
}

func (s *Server) SetupSaveLoop(timeout time.Duration) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGUSR1)
	s.SignalChan = sigChan

	if s.SavePath == "" {
		return
	}
	if _, err := os.Stat(s.SavePath); err == nil {
		log.Printf("Loading state from: %s", s.SavePath)
		s.Queues, err = ReadQueueStateMux(timeout, s.SavePath)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Loaded state from: %s", s.SavePath)
		}
	}
	s.LastSave = time.Now()
	s.LastSaveDuration = 0
	go s.SaveLoop()
}

func (s *Server) SaveLoop() {
	var shutdown bool
	for !shutdown {
		select {
		case <-time.After(s.SaveInterval):
		case <-s.SignalChan:
			log.Println("caught SIGUSR1")
			shutdown = true
		}
		log.Printf("Saving state to: %s", s.SavePath)
		tmpPath := s.SavePath + ".tmp"
		w, err := os.Create(tmpPath)
		if err != nil {
			log.Fatal(err)
		}
		t1 := time.Now()
		err = s.Queues.Serialize(w, shutdown)
		w.Close()
		if err != nil {
			log.Fatal(err)
		}
		os.Rename(tmpPath, s.SavePath)

		s.SaveStatsLock.Lock()
		s.LastSave = time.Now()
		s.LastSaveDuration = s.LastSave.Sub(t1)
		s.SaveStatsLock.Unlock()

		log.Printf("Saved state to: %s", s.SavePath)
	}

	// We are shutting down post-save
	log.Println("exiting due to shutdown signal")
	os.Exit(0)
}

func parseLimit(limit string) (int, error) {
	if limit == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(limit)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func serveObject(w http.ResponseWriter, obj interface{}) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"data": obj})
}

func serveError(w http.ResponseWriter, err string, status int) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{"error": err})
}
