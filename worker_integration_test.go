package natskit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// These tests only launch a disposable localhost server in a temporary data
// directory. If nats-server is not installed they skip; CI installs it.
func startDisposableServer(t *testing.T) string {
	t.Helper()
	binary, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server is required for disposable JetStream tests")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	command := exec.CommandContext(ctx, binary, "-js", "-sd", t.TempDir(), "-a", "127.0.0.1", "-p", strconv.Itoa(port))
	if err := command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = command.Wait()
	})
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := nats.Connect(url, nats.NoReconnect(), nats.Timeout(150*time.Millisecond))
		if err == nil {
			conn.Close()
			return url
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("disposable nats-server did not become ready")
	return ""
}

func disposableConfig(t *testing.T, url string) (Config, nats.JetStreamContext, *nats.Conn) {
	t.Helper()
	cfg := sample(t)
	cfg.InstanceID = "worker-a"
	cfg.Connection.URLs = []string{url}
	cfg.JetStream.Defaults.Consumer.Backoff = []string{"150ms", "250ms"}
	cfg.JetStream.Defaults.Worker.InProgressInterval = "50ms"
	resolved, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := conn.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := ProvisionMissing(context.Background(), js, resolved); err != nil {
		t.Fatalf("disposable provision failed: %v", err)
	}
	return cfg, js, conn
}

func TestDisposableSharedDurableAcrossTwoWorkers(t *testing.T) {
	url := startDisposableServer(t)
	cfg, _, _ := disposableConfig(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	first, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	cfg.InstanceID = "worker-b"
	second, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	const total = 20
	received := make(chan string, total*2)
	handler := func(_ context.Context, msg *nats.Msg) error {
		received <- string(msg.Data)
		return nil
	}
	worker1, err := first.StartWorker(ctx, "RESULTS_WORKER", handler)
	if err != nil {
		t.Fatal(err)
	}
	defer worker1.Stop()
	worker2, err := second.StartWorker(ctx, "RESULTS_WORKER", handler)
	if err != nil {
		t.Fatal(err)
	}
	defer worker2.Stop()

	for i := 0; i < total; i++ {
		id := strconv.Itoa(i)
		if err := first.Publish(ctx, "results", []byte(id), "shared-"+id); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	seen := make(map[string]bool, total)
	for len(seen) < total {
		select {
		case msg := <-received:
			seen[msg] = true
		case <-ctx.Done():
			t.Fatalf("only %d/%d messages processed by shared durable: %v", len(seen), total, ctx.Err())
		}
	}
}

func TestDisposableHandlerFailureRedeliversBeforeACK(t *testing.T) {
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
	finished := make(chan struct{}, 1)
	worker, err := client.StartWorker(ctx, "RESULTS_WORKER", func(_ context.Context, msg *nats.Msg) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient database failure")
		}
		finished <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Stop()
	if err := client.Publish(ctx, "results", []byte("redeliver"), "retryable-1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
		if attempts.Load() < 2 {
			t.Fatal("message was ACKed before retry")
		}
	case <-ctx.Done():
		t.Fatalf("redelivery never completed: %v", ctx.Err())
	}
}

func TestDisposableQuarantinePubAckFailureDoesNotACKOrigin(t *testing.T) {
	url := startDisposableServer(t)
	cfg, js, _ := disposableConfig(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// Only the disposable server's quarantine Stream is removed. No external
	// broker or persisted application data is ever touched by this test.
	if err := js.DeleteStream(client.Topology.Quarantine.Stream); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	redelivered := make(chan struct{}, 1)
	worker, err := client.StartWorker(ctx, "RESULTS_WORKER", func(_ context.Context, _ *nats.Msg) error {
		if attempts.Add(1) == 2 {
			redelivered <- struct{}{}
		}
		return DecodeError(errors.New("invalid payload"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Stop()
	if err := client.Publish(ctx, "results", []byte("bad-payload"), "poison-1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-redelivered:
		if attempts.Load() < 2 {
			t.Fatal("source was ACKed despite missing quarantine PubAck")
		}
	case <-ctx.Done():
		t.Fatalf("origin was not redelivered after quarantine failure: %v", ctx.Err())
	}
}
