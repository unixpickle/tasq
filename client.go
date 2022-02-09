package tasq

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// A Task stores information about a popped task.
type Task struct {
	ID       string `json:"id"`
	Contents string `json:"contents"`
}

// A Client makes API calls to a tasq server.
//
// The server is identified as a URL. For example, you might provide a parsed
// URL "http://myserver.com:8080". The path in the URL is replaced with API
// endpoint paths, but the protocol, host, and port are retained.
type Client struct {
	URL *url.URL
}

// Push adds a task to the queue and returns its ID.
func (c *Client) Push(contents string) (string, error) {
	var response string
	err := c.postForm("/task/push", "contents", contents, &response)
	return response, err
}

// PushBatch adds a batch of tasks to the queue and return their IDs.
func (c *Client) PushBatch(contents []string) ([]string, error) {
	var response []string
	err := c.postJSON("/task/push_batch", contents, &response)
	return response, err
}

// Pop retrieves a pending task from the queue.
//
// If no task is returned, a retry time may be returned indicating the number
// of seconds until the next in-progress task will expire. If this retry time
// is also nil, then the queue has been exhausted.
func (c *Client) Pop() (*Task, *float64, error) {
	var response struct {
		ID       *string `json:"id"`
		Contents *string `json:"contents"`
		Done     bool    `json:"done"`
		Retry    float64 `json:"retry"`
	}
	if err := c.get("/task/pop", &response); err != nil {
		return nil, nil, err
	}
	if response.ID != nil && response.Contents != nil {
		return &Task{ID: *response.ID, Contents: *response.Contents}, nil, nil
	} else if response.Done {
		return nil, nil, nil
	} else {
		return nil, &response.Retry, nil
	}
}

// PopBatch retrieves at most n tasks from the queue.
//
// If fewer than n tasks are returned, then a retry time (in seconds) may be
// returned to indicate when the next pending task will expire.
//
// If no tasks are returned and the retry time is nil, then the queue has been
// exhausted.
func (c *Client) PopBatch(n int) ([]*Task, *float64, error) {
	var response struct {
		Done  bool    `json:"done"`
		Retry float64 `json:"retry"`
		Tasks []*Task `json:"tasks"`
	}
	if err := c.postForm("/task/pop_batch", "count", strconv.Itoa(n), &response); err != nil {
		return nil, nil, err
	}
	if response.Done {
		return nil, nil, nil
	} else {
		return response.Tasks, &response.Retry, nil
	}
}

// Completed tells the server that the identified task was completed.
func (c *Client) Completed(id string) error {
	return c.postForm("/task/completed", "id", id, nil)
}

// CompletedBatch tells the server that the identified tasks were completed.
func (c *Client) CompletedBatch(ids []string) error {
	return c.postJSON("/task/completed_batch", ids, nil)
}

func (c *Client) get(path string, output interface{}) error {
	reqURL := *c.URL
	reqURL.Path = path

	resp, err := http.Get(reqURL.String())
	if err := c.handleResponse(resp, err, output); err != nil {
		return errors.Wrap(err, "get "+path)
	}
	return nil
}

func (c *Client) postForm(path, key, value string, output interface{}) error {
	postBody := strings.NewReader(url.QueryEscape(key) + "=" + url.QueryEscape(value))
	return c.post(path, "application/x-www-form-urlencoded", postBody, output)
}

func (c *Client) postJSON(path string, input, output interface{}) error {
	data, err := json.Marshal(input)
	if err != nil {
		return errors.Wrap(err, "post "+path)
	}
	return c.post(path, "application/json", bytes.NewReader(data), output)
}

func (c *Client) post(path string, contentType string, input io.Reader, output interface{}) error {
	reqURL := *c.URL
	reqURL.Path = path

	resp, err := http.Post(reqURL.String(), contentType, input)
	if err := c.handleResponse(resp, err, output); err != nil {
		return errors.Wrap(err, "post "+path)
	}
	return nil
}

func (c *Client) handleResponse(resp *http.Response, err error, output interface{}) error {
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var response struct {
		Error *string     `json:"error"`
		Data  interface{} `json:"data"`
	}
	response.Data = output
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return err
	} else if response.Error != nil {
		return errors.New("remote error: " + *response.Error)
	} else {
		return nil
	}
}
