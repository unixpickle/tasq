package main

import (
	"time"
)

// Default history time used for RateTracker.
const DefaultRateTrackerBins = 128

// A RateTracker keeps a sliding window of event counts over the last
// N seconds.
type RateTracker struct {
	firstBinTime int64
	bins         []int64
}

// NewRateTracker creates a RateTracker which keeps event counts up to
// historySize seconds in the past.
//
// If historySize is 0, then DefaultRateTrackerBins is used.
func NewRateTracker(historySize int) *RateTracker {
	if historySize == 0 {
		historySize = DefaultRateTrackerBins
	}
	return &RateTracker{
		bins: make([]int64, historySize),
	}
}

// DecodeRateTracker loads an encoded RateTracker.
// If the state is empty, a new rate tracker with DefaultRateTrackerBins is
// created.
func DecodeRateTracker(state *EncodedRateTracker) *RateTracker {
	if state == nil {
		return NewRateTracker(DefaultRateTrackerBins)
	}
	return &RateTracker{
		firstBinTime: state.FirstBinTime,
		bins:         state.Bins,
	}
}

// Reset zeros out the counters.
func (r *RateTracker) Reset() {
	for i := range r.bins {
		r.bins[i] = 0
	}
}

// HistorySize returns the number of time bins.
func (r *RateTracker) HistorySize() int {
	return len(r.bins)
}

// Add adds the count n to the current time bin.
func (r *RateTracker) Add(n int64) {
	r.AddAt(time.Now().Unix(), n)
}

// AddAt is like Add, but allows the caller to specify the current time.
func (r *RateTracker) AddAt(curTime, n int64) {
	r.truncateAndShift(curTime)
	r.bins[len(r.bins)-1] += n
}

// Count retrieves the count over the last t seconds.
// The t argument must be at most the history size passed to NewRateTracker.
func (r *RateTracker) Count(t int) int64 {
	return r.CountAt(time.Now().Unix(), t)
}

// CountAt is like Count, but allows the caller to specify the current time.
func (r *RateTracker) CountAt(curTime int64, t int) int64 {
	if t > len(r.bins) {
		panic("too many seconds requested")
	}
	r.truncateAndShift(curTime)
	var res int64
	for i := len(r.bins) - 1; i >= len(r.bins)-t; i-- {
		res += r.bins[i]
	}
	return res
}

func (r *RateTracker) Encode() *EncodedRateTracker {
	return &EncodedRateTracker{
		FirstBinTime: r.firstBinTime,
		Bins:         append([]int64{}, r.bins...),
	}
}

func (r *RateTracker) truncateAndShift(curTime int64) {
	lastBinTime := r.firstBinTime + int64(len(r.bins)) - 1

	if curTime < r.firstBinTime || curTime >= lastBinTime+int64(len(r.bins)) {
		r.firstBinTime = curTime - (int64(len(r.bins)) - 1)
		for i := range r.bins {
			r.bins[i] = 0
		}
	} else if curTime < lastBinTime {
		// The clock has likely changed a tiny bit, so the last bin is in
		// the future. This rarely happens, but we lose history when it does.
		backtrack := lastBinTime - curTime
		r.firstBinTime -= backtrack
		copy(r.bins[backtrack:], r.bins[:])
		for i := 0; i < int(backtrack); i++ {
			r.bins[i] = 0
		}
	} else if curTime > lastBinTime {
		forward := curTime - lastBinTime
		r.firstBinTime += forward
		copy(r.bins[:], r.bins[forward:])
		for i := len(r.bins) - int(forward); i < len(r.bins); i++ {
			r.bins[i] = 0
		}
	}
}

type EncodedRateTracker struct {
	FirstBinTime int64
	Bins         []int64
}
