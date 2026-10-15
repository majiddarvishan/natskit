package natskit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWorkerStopIsConcurrentAndIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	completion := make(chan error, 1)
	finished := make(chan struct{})
	worker := &Worker{done: completion, cancel: cancel, stopped: make(chan struct{})}
	want := errors.New("worker stopped")

	var callers sync.WaitGroup
	results := make(chan error, 16)
	for i := 0; i < cap(results); i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			results <- worker.Stop()
		}()
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the worker context")
	}
	go func() {
		callers.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		t.Fatal("Stop returned before worker shutdown completed")
	case <-time.After(10 * time.Millisecond):
	}
	completion <- want
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("concurrent Stop calls did not finish")
	}
	close(results)
	for result := range results {
		if !errors.Is(result, want) {
			t.Fatalf("Stop returned %v, want %v", result, want)
		}
	}
	if result := worker.Stop(); !errors.Is(result, want) {
		t.Fatalf("sequential Stop returned %v, want %v", result, want)
	}
}
