package main

import (
	"testing"
)

func TestRateTracker(t *testing.T) {
	rt := NewRateTracker(5)
	rt.AddAt(30, 2)
	rt.AddAt(31, 3)
	rt.AddAt(32, 4)
	rt.AddAt(32, 1)
	rt.AddAt(33, 1)
	if count := rt.CountAt(33, 1); count != 1 {
		t.Fatalf("bad count: %d", count)
	}
	if count := rt.CountAt(33, 2); count != 6 {
		t.Fatalf("bad count: %d", count)
	}
	if count := rt.CountAt(33, 5); count != 11 {
		t.Fatalf("bad count: %d", count)
	}
	rt.AddAt(34, 3)
	if count := rt.CountAt(34, 2); count != 4 {
		t.Fatalf("bad count: %d", count)
	}
	rt.AddAt(36, 7)
	if count := rt.CountAt(36, 1); count != 7 {
		t.Fatalf("bad count: %d", count)
	}
	if count := rt.CountAt(36, 2); count != 7 {
		t.Fatalf("bad count: %d", count)
	}
	if count := rt.CountAt(36, 3); count != 10 {
		t.Fatalf("bad count: %d", count)
	}
	if count := rt.CountAt(36, 5); count != 16 {
		t.Fatalf("bad count: %d", count)
	}
	rt.AddAt(35, 5)
	if count := rt.CountAt(35, 1); count != 5 {
		t.Fatalf("bad count: %d", count)
	}
	if count := rt.CountAt(35, 2); count != 8 {
		t.Fatalf("bad count: %d", count)
	}
	if count := rt.CountAt(35, 5); count != 14 {
		t.Fatalf("bad count: %d", count)
	}
	rt.AddAt(40, 10)
	if count := rt.CountAt(40, 5); count != 10 {
		t.Fatalf("bad count: %d", count)
	}
}
