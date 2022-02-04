// Command tasq-transfer moves tasks from one tasq server to another.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"

	"github.com/unixpickle/essentials"
)

func main() {
	var sourceHost string
	var destHost string
	var numTasks int
	var bufferSize int
	var workers int
	flag.StringVar(&sourceHost, "source", "", "source host")
	flag.StringVar(&destHost, "dest", "", "source host")
	flag.IntVar(&numTasks, "num-tasks", -1, "rough number of tasks to transfer")
	flag.IntVar(&bufferSize, "buffer-size", 1024, "task buffer size")
	flag.IntVar(&workers, "workers", 32, "parallel Goroutines for popping tasks")
	flag.Parse()

	if sourceHost == "" || destHost == "" {
		fmt.Fprintln(os.Stderr, "Must provide -source and -dest. See -help.")
	}

	ch := &TaskChan{
		Max:  int64(numTasks),
		Chan: make(chan *SourceTask, bufferSize),
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ch.KeepGoing() {
				task, err := PopTask(sourceHost)
				if err != nil {
					log.Println("ERROR:", err)
					return
				} else if task != nil {
					return
				}
				ch.Chan <- task
			}
		}()
	}
	go func() {
		wg.Wait()
		close(ch.Chan)
	}()

	var buffer []*SourceTask
	for task := range ch.Chan {
		buffer = append(buffer, task)
		if len(buffer) == bufferSize {
			essentials.Must(PushTasks(destHost, buffer))
			essentials.Must(CancelTasks(sourceHost, buffer, workers))
			buffer = nil
		}
	}

	if len(buffer) > 0 {
		essentials.Must(PushTasks(destHost, buffer))
		essentials.Must(CancelTasks(sourceHost, buffer, workers))
	}
}

func PopTask(host string) (*SourceTask, error) {
	reqURL := host + "/task/pop"
	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	var response struct {
		Data *struct {
			ID       *string `json:"id"`
			Contents string  `json:"contents"`
			Done     bool    `json:"done"`
		} `json:"data"`
		Error *string `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, errors.New(*response.Error)
	}
	if response.Data == nil {
		return nil, errors.New("unexpected response data")
	}
	if response.Data.ID == nil {
		return nil, nil
	}
	return &SourceTask{Contents: response.Data.Contents, ID: *response.Data.ID}, nil
}

func PushTasks(host string, buffer []*SourceTask) error {
	var allContents []string
	for _, task := range buffer {
		allContents = append(allContents, task.Contents)
	}
	postData, _ := json.Marshal(allContents)
	req, err := http.NewRequest("POST", host+"/task/push_batch", bytes.NewReader(postData))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func CancelTasks(host string, buffer []*SourceTask, workers int) error {
	var firstErr error
	var errLock sync.Mutex
	essentials.ConcurrentMap(workers, len(buffer), func(i int) {
		url := host + "/task/completed?id=" + url.QueryEscape(buffer[i].ID)
		resp, err := http.Get(url)
		if err != nil {
			errLock.Lock()
			if firstErr == nil {
				firstErr = err
			}
			errLock.Unlock()
		} else {
			resp.Body.Close()
		}
	})
	return firstErr
}
