package main

import (
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
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
		Queues:       NewQueueStateMux(timeout),
	}
	http.HandleFunc(pathPrefix, s.ServeIndex)
	http.HandleFunc(pathPrefix+"summary", s.ServeSummary)
	http.HandleFunc(pathPrefix+"counts", s.ServeCounts)
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
	s.Queues.Iterate(func(name string, qs *QueueState) {
		found = true
		if name == "" {
			fmt.Fprint(w, "---- Default context ----\n")
		} else {
			fmt.Fprintf(w, "---- Context: %s ----\n", name)
		}
		counts := qs.Counts()
		fmt.Fprintf(w, "    Pending: %d\n", counts.Pending)
		fmt.Fprintf(w, "In progress: %d\n", counts.Running)
		fmt.Fprintf(w, "    Expired: %d\n", counts.Expired)
		fmt.Fprintf(w, "  Completed: %d\n", counts.Completed)
	})
	if !found {
		fmt.Fprint(w, "No active queues.")
	}
}

func (s *Server) ServeCounts(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	if r.URL.Query().Get("all") == "1" {
		allNames := []string{}
		allCounts := []*QueueCounts{}
		s.Queues.Iterate(func(name string, qs *QueueState) {
			allNames = append(allNames, name)
			allCounts = append(allCounts, qs.Counts())
		})
		serveObject(w, map[string]interface{}{
			"names":  allNames,
			"counts": allCounts,
		})
		return
	}
	s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		serveObject(w, qs.Counts())
	})
}

func (s *Server) ServePushTask(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	contents := r.FormValue("contents")
	if contents == "" {
		serveError(w, "must specify non-empty `contents` parameter")
	} else {
		s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
			serveObject(w, qs.Push(contents))
		})
	}
}

func (s *Server) ServePushBatch(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	var contents []string
	if err := json.Unmarshal(data, &contents); err != nil {
		serveError(w, err.Error())
	} else {
		ids := []string{}
		s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
			for _, c := range contents {
				ids = append(ids, qs.Push(c))
			}
		})
		serveObject(w, ids)
	}
}

func (s *Server) ServePopTask(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	var task *Task
	var nextTry *time.Time
	s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		task, nextTry = qs.Pop()
	})
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
	if !s.BasicAuth(w, r) {
		return
	}
	n, err := strconv.Atoi(r.FormValue("count"))
	if err != nil {
		serveError(w, "invalid 'count' parameter: "+err.Error())
		return
	} else if n <= 0 {
		serveError(w, "invalid 'count' requested")
		return
	}

	var tasks []*Task
	var nextTry *time.Time
	s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		tasks, nextTry = qs.PopBatch(n)
	})

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
	if !s.BasicAuth(w, r) {
		return
	}
	var task, nextTask *Task
	var nextTime *time.Time
	s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		task, nextTask, nextTime = qs.Peek()
	})
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
	if !s.BasicAuth(w, r) {
		return
	}
	var status bool
	s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		status = qs.Completed(r.FormValue("id"))
	})
	if status {
		serveObject(w, true)
	} else {
		serveError(w, "there was no in-progress task with the specified `id`")
	}
}

func (s *Server) ServeCompletedBatch(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		serveError(w, err.Error())
	} else {
		var failures []string
		s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
			for _, id := range ids {
				if !qs.Completed(id) {
					failures = append(failures, id)
				}
			}
		})
		if len(failures) > 0 {
			serveError(w, "there were no in-progress tasks with the specified ids: "+
				strings.Join(failures, ", "))
		} else {
			serveObject(w, true)
		}
	}
}

func (s *Server) ServeKeepalive(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	var status bool
	s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		status = qs.Keepalive(r.FormValue("id"))
	})
	if status {
		serveObject(w, true)
	} else {
		serveError(w, "there was no in-progress task with the specified `id`")
	}
}

func (s *Server) ServeClearTasks(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		qs.Clear()
	})
	serveObject(w, true)
}

func (s *Server) ServeExpireTasks(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	var n int
	s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		n = qs.ExpireAll()
	})
	serveObject(w, n)
}

func (s *Server) ServeQueueExpired(w http.ResponseWriter, r *http.Request) {
	if !s.BasicAuth(w, r) {
		return
	}
	var n int
	s.Queues.Get(r.URL.Query().Get("context"), func(qs *QueueState) {
		n = qs.QueueExpired()
	})
	serveObject(w, n)
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

func (s *Server) SetupSaveLoop(timeout time.Duration) {
	if s.SavePath == "" {
		return
	}
	if _, err := os.Stat(s.SavePath); err == nil {
		s.Queues, err = ReadQueueStateMux(timeout, s.SavePath)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Loaded state from: %s", s.SavePath)
		}
	}
	go s.SaveLoop()
}

func (s *Server) SaveLoop() {
	for {
		time.Sleep(s.SaveInterval)
		tmpPath := s.SavePath + ".tmp"
		w, err := os.Create(tmpPath)
		if err != nil {
			log.Fatal(err)
		}
		err = s.Queues.Serialize(w)
		w.Close()
		if err != nil {
			log.Fatal(err)
		}
		os.Rename(tmpPath, s.SavePath)
		log.Printf("Saved state to: %s", s.SavePath)
	}
}

func serveObject(w http.ResponseWriter, obj interface{}) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"data": obj})
}

func serveError(w http.ResponseWriter, err string) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"error": err})
}
