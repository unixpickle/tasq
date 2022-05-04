// Command tasq-transfer moves tasks from one tasq server to another.
//
// Regardless of program interruption or network errors, no tasks will be lost.
// In particular, a crash or network failure during the transfer may result in
// some tasks being duplicated between the source and destination servers, but
// no tasks will be removed from the source before being added to the
// destination.
package main

import (
	"flag"
	"log"
	"time"

	"github.com/unixpickle/essentials"
	"github.com/unixpickle/tasq"
)

func main() {
	var sourceHost string
	var sourceContext string
	var destHost string
	var destContext string
	var numTasks int
	var bufferSize int
	var waitRunning bool
	flag.StringVar(&sourceHost, "source", "", "source server URL")
	flag.StringVar(&sourceContext, "source-context", "", "source context")
	flag.StringVar(&destHost, "dest", "", "destination server URL")
	flag.StringVar(&destContext, "dest-context", "", "destination context")
	flag.IntVar(&numTasks, "count", -1, "number of tasks to transfer")
	flag.IntVar(&bufferSize, "buffer-size", 4096, "task buffer size")
	flag.BoolVar(&waitRunning, "wait-running", false,
		"attempt to transfer in-progress tasks once they expire")
	flag.Parse()

	if sourceHost == "" || destHost == "" {
		essentials.Die("Must provide -source and -dest. See -help.")
	}

	sourceClient, err := tasq.NewClient(sourceHost, sourceContext)
	essentials.Must(err)
	destClient, err := tasq.NewClient(destHost, destContext)
	essentials.Must(err)

	completed := 0
	for numTasks == -1 || completed < numTasks {
		bs := bufferSize
		if numTasks != -1 && bs > numTasks-completed {
			bs = numTasks - completed
		}
		tasks, retry, err := sourceClient.PopBatch(bs)
		if err != nil {
			log.Fatalln("ERROR popping batch:", err)
		}
		if len(tasks) == 0 && retry == nil {
			log.Println("Source queue has been exhausted.")
			break
		} else if len(tasks) == 0 {
			if waitRunning {
				log.Printf("Waiting %f seconds for next timeout...", *retry)
				time.Sleep(time.Duration(float64(time.Second) * *retry))
			} else {
				log.Printf("Done all immediately available tasks (wait time %f).", *retry)
				break
			}
		} else {
			var ids, contents []string
			for _, t := range tasks {
				ids = append(ids, t.ID)
				contents = append(contents, t.Contents)
			}
			if _, err := destClient.PushBatch(contents); err != nil {
				log.Fatalln("ERROR pushing batch:", err)
			}
			if err := sourceClient.CompletedBatch(ids); err != nil {
				log.Fatalln("ERROR marking batch as completed:", err)
			}
			completed += len(tasks)
			log.Printf("Current status: transferred a total of %d tasks", completed)
		}
	}
}
