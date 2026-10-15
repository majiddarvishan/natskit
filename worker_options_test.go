package natskit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestWorkerOptionsRefuseNegativeRetryDelay(t *testing.T) {
	client := &Client{}
	_, err := client.StartWorkerWithOptions(context.Background(), "unused",
		func(context.Context, *nats.Msg) error { return nil },
		WorkerOptions{RetryDelay: -time.Second})
	if err == nil {
		t.Fatal("negative retry delay accepted")
	}
}

func TestDisposableQuarantineBuilderRejectsMutatedPayload(t *testing.T) {
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
	again := make(chan struct{}, 1)
	worker, err := client.StartWorkerWithOptions(ctx, "RESULTS_WORKER",
		func(context.Context, *nats.Msg) error {
			if attempts.Add(1) == 2 {
				again <- struct{}{}
			}
			return PermanentError(errors.New("unrecoverable"))
		}, WorkerOptions{
			RetryDelay: 120 * time.Millisecond,
			BuildQuarantine: func(msg *nats.Msg, _ *nats.MsgMetadata, _ string) (*nats.Msg, error) {
				return &nats.Msg{
					Subject: client.Topology.Quarantine.Subject,
					Data: []byte("re-encoded"),
					Header: nats.Header{nats.MsgIdHdr: []string{"invalid-copy"}},
				}, nil
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Stop()
	if err := client.Publish(ctx, "results", []byte("immutable-source"), "builder-original"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-again:
	case <-ctx.Done():
		t.Fatalf("invalid quarantine unexpectedly ACKed origin: %v", ctx.Err())
	}
	info, err := client.JS.StreamInfo(client.Topology.Quarantine.Stream, nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatal("invalid quarantine builder published modified payload")
	}
}

func TestDisposableObserverPanicDoesNotPreventACK(t *testing.T) {
	url := startDisposableServer(t)
	cfg, _, _ := disposableConfig(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	client, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	worker, err := client.StartWorkerWithOptions(ctx, "RESULTS_WORKER",
		func(context.Context, *nats.Msg) error { return nil },
		WorkerOptions{Observe: func(DeliveryEvent) { panic("broken logging backend") }})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Stop()
	if err := client.Publish(ctx, "results", []byte("ack-safe"), "observer-panic"); err != nil {
		t.Fatal(err)
	}
	for {
		info, err := client.JS.ConsumerInfo(client.Topology.Consumers["RESULTS_WORKER"].Stream,
			client.Topology.Consumers["RESULTS_WORKER"].Config.Durable, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		if info.AckFloor.Consumer >= 1 && info.NumAckPending == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("logging panic prevented durable ACK: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
