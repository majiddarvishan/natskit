package natskit

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Uses only a locally started disposable file-backed broker. The durable
// remains attached while the server process exits and starts at the same URL.
func TestDisposableWorkerRecoversAfterBrokerRestart(t *testing.T) {
	binary, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server executable required")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	dir := filepath.Join(t.TempDir(), "jetstream")
	start := func() (func(), error) {
		procCtx, cancel := context.WithCancel(context.Background())
		command := exec.CommandContext(procCtx, binary, "-js", "-sd", dir, "-a", "127.0.0.1", "-p", strconv.Itoa(port))
		if err := command.Start(); err != nil {
			cancel()
			return nil, err
		}
		return func() {
			cancel()
			_ = command.Wait()
		}, nil
	}
	stop, err := start()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { if stop != nil { stop() } }()
	deadline := time.Now().Add(8 * time.Second)
	for {
		conn, err := nats.Connect(url, nats.NoReconnect(), nats.Timeout(150*time.Millisecond))
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("disposable broker startup failed")
		}
		time.Sleep(30 * time.Millisecond)
	}
	cfg, _, _ := disposableConfig(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 23*time.Second)
	defer cancel()
	client, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	received := make(chan string, 1)
	worker, err := client.StartWorker(ctx, "RESULTS_WORKER", func(_ context.Context, msg *nats.Msg) error {
		select {
		case received <- string(msg.Data):
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Stop()
	stop()
	stop = nil
	for client.Conn.IsConnected() {
		if ctx.Err() != nil {
			t.Fatal("NATS Kit did not recognize broker shutdown")
		}
		time.Sleep(25 * time.Millisecond)
	}
	stop, err = start()
	if err != nil {
		t.Fatal(err)
	}
	for !client.Conn.IsConnected() {
		if ctx.Err() != nil {
			t.Fatal("NATS Kit did not reconnect after broker restart")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !worker.Ready() {
		t.Fatal("NATS Kit worker terminated during transient broker outage")
	}
	if err := client.Publish(ctx, "results", []byte("persisted-after-reconnect"), "broker-restart-test"); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-received:
		if payload != "persisted-after-reconnect" {
			t.Fatalf("post-reconnect payload changed: %q", payload)
		}
	case <-ctx.Done():
		t.Fatalf("NATS Kit worker did not recover: %v", ctx.Err())
	}
}
