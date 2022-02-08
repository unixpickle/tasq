// Command tasq-transfer moves tasks from one tasq server to another.
//
// Regardless of program interruption or network errors, no tasks will be lost.
// In particular, a crash or network failure during the transfer may result in
// some tasks being duplicated between the source and destination servers, but
// no tasks will be removed from the source before being added to the
// destination.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/unixpickle/essentials"
)

func main() {
	var sourceHost string
	var destHost string
	var numTasks int
	var bufferSize int
	var waitRunning bool
	flag.StringVar(&sourceHost, "source", "", "source server URL")
	flag.StringVar(&destHost, "dest", "", "destination server URL")
	flag.IntVar(&numTasks, "count", -1, "number of tasks to transfer")
	flag.IntVar(&bufferSize, "buffer-size", 4096, "task buffer size")
	flag.BoolVar(&waitRunning, "wait-running", false,
		"attempt to transfer in-progress tasks once they expire")
	flag.Parse()

	if sourceHost == "" || destHost == "" {
		essentials.Die("Must provide -source and -dest. See -help.")
	}

	completed := 0
	for numTasks == -1 || completed < numTasks {
		bs := bufferSize
		if numTasks != -1 && bs > numTasks-completed {
			bs = numTasks - completed
		}
		response, err := PopBatch(sourceHost, bs)
		if err != nil {
			log.Fatalln("ERROR popping batch:", err)
		}
		if response.Done {
			log.Println("Source queue has been exhausted.")
			break
		} else if len(response.Tasks) == 0 {
			if waitRunning {
				log.Printf("Waiting %f seconds for next timeout...", response.Retry)
				time.Sleep(time.Duration(float64(time.Second) * response.Retry))
			} else {
				log.Printf("Done all immediately available tasks (wait time %f).", response.Retry)
				break
			}
		} else {
			if err := PushBatch(destHost, response.Tasks); err != nil {
				log.Fatalln("ERROR pushing batch:", err)
			}
			if err := CompletedBatch(sourceHost, response.Tasks); err != nil {
				log.Fatalln("ERROR marking batch as completed:", err)
			}
			completed += len(response.Tasks)
			log.Printf("Current status: transferred a total of %d tasks", completed)
		}
	}
}

type Task struct {
	ID       string `json:"id"`
	Contents string `json:"contents"`
}

type PopResponse struct {
	Tasks []*Task `json:"tasks"`
	Done  bool    `json:"done"`
	Retry float64 `json:"retry"`
}

func PopBatch(host string, n int) (*PopResponse, error) {
	reqURL, err := urlForAPI(host, "/task/pop_batch", map[string]string{"count": strconv.Itoa(n)})
	if err != nil {
		return nil, err
	}
	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var response struct {
		Data *PopResponse `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	return response.Data, nil
}

func PushBatch(host string, tasks []*Task) error {
	reqURL, err := urlForAPI(host, "/task/push_batch", nil)
	if err != nil {
		return err
	}
	var contents []string
	for _, t := range tasks {
		contents = append(contents, t.Contents)
	}
	return postStrings(reqURL, contents)
}

func CompletedBatch(host string, tasks []*Task) error {
	reqURL, err := urlForAPI(host, "/task/completed_batch", nil)
	if err != nil {
		return err
	}
	var ids []string
	for _, t := range tasks {
		ids = append(ids, t.ID)
	}
	return postStrings(reqURL, ids)
}

func postStrings(reqURL string, strs []string) error {
	data, _ := json.Marshal(strs)
	req, err := http.NewRequest("POST", reqURL, bytes.NewReader(data))
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

func urlForAPI(baseURL, path string, query map[string]string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	parsed.Path = path
	if query != nil {
		values := url.Values{}
		for k, v := range query {
			values.Set(k, v)
		}
		parsed.RawQuery = values.Encode()
	} else {
		parsed.RawQuery = ""
	}
	return parsed.String(), nil
}
