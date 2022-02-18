package main

import (
	"flag"
	"log"
	"time"

	"github.com/unixpickle/essentials"
	"github.com/unixpickle/tasq"
)

func main() {
	var host string
	var interval time.Duration
	flag.StringVar(&host, "host", "", "server URL")
	flag.DurationVar(&interval, "interval", time.Second, "number of seconds between count calls")
	flag.Parse()

	if host == "" {
		essentials.Die("Must provide -host argument. See -help.")
	}

	client, err := tasq.NewClient(host)
	essentials.Must(err)

	t1 := time.Now()
	startCounts, err := client.QueueCounts()
	essentials.Must(err)

	for {
		time.Sleep(interval)
		counts, err := client.QueueCounts()
		essentials.Must(err)
		completed := float64(counts.Completed - startCounts.Completed)
		elapsed := time.Now().Sub(t1).Seconds()
		log.Printf("task rate: %.03f tasks/second (total time %.02f seconds)", completed/elapsed, elapsed)
	}
}
