package natskit

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestDisposableThreeReplicasShareOneDurable(t *testing.T) {
	url := startDisposableServer(t)
	cfg, _, _ := disposableConfig(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	const total = 30
	received := make(chan string, total*2)
	var clients []*Client
	var workers []*Worker
	defer func() {
		for _, worker := range workers {
			_ = worker.Stop()
		}
		for _, client := range clients {
			_ = client.Close()
		}
	}()
	for i := 0; i < 3; i++ {
		cfg.InstanceID = "replica-" + strconv.Itoa(i+1)
		client, err := Connect(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, client)
		worker, err := client.StartWorker(ctx, "RESULTS_WORKER", func(_ context.Context, msg *nats.Msg) error {
			received <- string(msg.Data)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		workers = append(workers, worker)
	}
	for i := 0; i < total; i++ {
		id := strconv.Itoa(i)
		if err := clients[0].Publish(ctx, "results", []byte(id), "three-replica-"+id); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]bool, total)
	for len(seen) < total {
		select {
		case payload := <-received:
			seen[payload] = true
		case <-ctx.Done():
			t.Fatalf("three replicas processed %d/%d messages: %v", len(seen), total, ctx.Err())
		}
	}
}

func TestDisposableRestartWithNewInstanceRedeliversUnackedMessage(t *testing.T) {
	url := startDisposableServer(t)
	cfg, _, _ := disposableConfig(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	first, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	began := make(chan struct{}, 1)
	worker, err := first.StartWorker(ctx, "RESULTS_WORKER", func(ctx context.Context, _ *nats.Msg) error {
		began <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Publish(ctx, "results", []byte("persisted"), "restart-message"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-began:
	case <-ctx.Done():
		t.Fatalf("first instance never received message: %v", ctx.Err())
	}
	if err := worker.Stop(); err != nil {
		t.Fatal(err)
	}
	cfg.InstanceID = "replacement-900"
	replacement, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	received := make(chan struct{}, 1)
	newWorker, err := replacement.StartWorker(ctx, "RESULTS_WORKER", func(_ context.Context, msg *nats.Msg) error {
		if string(msg.Data) != "persisted" {
			return errors.New("unexpected redelivery payload")
		}
		received <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer newWorker.Stop()
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatalf("replacement did not receive unacked message: %v", ctx.Err())
	}
}

func TestDisposableSlowHandlerInProgressExtendsAckDeadline(t *testing.T) {
	url := startDisposableServer(t)
	cfg, _, _ := disposableConfig(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var attempts atomic.Int32
	handled := make(chan struct{}, 1)
	worker, err := client.StartWorker(ctx, "RESULTS_WORKER", func(_ context.Context, _ *nats.Msg) error {
		attempts.Add(1)
		time.Sleep(650 * time.Millisecond) // exceeds 150ms ACK deadline
		handled <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Stop()
	if err := client.Publish(ctx, "results", []byte("slow"), "slow-handler"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-ctx.Done():
		t.Fatalf("slow Handler never finished: %v", ctx.Err())
	}
	// Allow more than two original AckWait intervals for any unwanted
	// redelivery to occur after Handler return and the library ACK.
	select {
	case <-time.After(400 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("slow Handler redelivered despite InProgress: %d attempts", got)
	}
}
