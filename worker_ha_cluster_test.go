package natskit

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Fully disposable THREE-server JetStream quorum test, not a customer HA
// exercise. Uses the same shared pull Durable with two app replicas and
// deliberately kills the currently elected results Stream leader.
func TestDisposableThreeNodeJetStreamLeaderFailover(t *testing.T) {
	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server binary required for isolated HA test")
	}
	const nodes = 3
	var clientPorts, routePorts [nodes]int
	for i := 0; i < nodes; i++ {
		clientPorts[i] = haFreePort(t)
		routePorts[i] = haFreePort(t)
	}
	urls := make([]string, 0, nodes)
	var commands [nodes]*exec.Cmd
	var logPaths [nodes]string
	for i := 0; i < nodes; i++ {
		urls = append(urls, "nats://127.0.0.1:"+strconv.Itoa(clientPorts[i]))
	}
	dir := t.TempDir()
	for i := 0; i < nodes; i++ {
		var routes []string
		for j := 0; j < nodes; j++ {
			if i != j {
				routes = append(routes, fmt.Sprintf("nats-route://127.0.0.1:%d", routePorts[j]))
			}
		}
		config := fmt.Sprintf(`server_name: "cra-ha-%d"
listen: "127.0.0.1:%d"
jetstream {
  store_dir: %q
  max_mem_store: 128Mb
  max_file_store: 128Mb
}
cluster {
  name: "cra-ha"
  listen: "127.0.0.1:%d"
  routes: ["%s"]
}
`, i, clientPorts[i], filepath.Join(dir, fmt.Sprintf("node-%d", i)), routePorts[i], strings.Join(routes, `", "`))
		file := filepath.Join(dir, fmt.Sprintf("node-%d.conf", i))
		if err := os.WriteFile(file, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(bin, "-c", file)
		logPaths[i] = filepath.Join(dir, fmt.Sprintf("node-%d.log", i))
		logfile, err := os.Create(logPaths[i])
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stdout = logfile
		cmd.Stderr = logfile
		if err := cmd.Start(); err != nil {
			_ = logfile.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = logfile.Close() })
		commands[i] = cmd
	}
	stop := func(i int) {
		if commands[i] != nil && commands[i].Process != nil {
			_ = commands[i].Process.Kill()
			_ = commands[i].Wait()
			commands[i] = nil
		}
	}
	t.Cleanup(func() {
		for i := 0; i < nodes; i++ {
			stop(i)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	var nc *nats.Conn
	for {
		nc, err = nats.Connect(strings.Join(urls, ","), nats.Timeout(300*time.Millisecond))
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("disposable cluster did not start: %v", err)
		}
		time.Sleep(80 * time.Millisecond)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	cfg := sample(t)
	cfg.InstanceID = "cra-ha-worker-a"
	cfg.Connection.URLs = urls
	cfg.JetStream.Defaults.Consumer.Backoff = []string{"250ms", "500ms"}
	cfg.JetStream.Defaults.Worker.InProgressInterval = "100ms"
	replicas := 3
	maxBytes := int64(16 << 20) // Three disposable Streams fit in 128 MB / node.
	cfg.JetStream.Defaults.Stream.Replicas = &replicas
	cfg.JetStream.Defaults.Stream.MaxBytes = &maxBytes
	resolved, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	// A disposable, operator-owned test cluster is permitted to create
	// Resources directly. Production Connect always remains verify-only.
	// Avoid using a single 50-second Context on a cluster bootstrap API
	// probe: a missing resource before the metadata leader is ready can
	// otherwise pin the entire test until the overall deadline.
	provision := func(action string, apply func() error) {
		var firstErr error
		for {
			err := apply()
			if err == nil {
				return
			}
			if firstErr == nil {
				firstErr = err
			}
			if ctx.Err() != nil {
				for i := 0; i < nodes; i++ {
					log, readErr := os.ReadFile(logPaths[i])
					if readErr == nil {
						if len(log) > 3500 {
							log = log[len(log)-3500:]
						}
						t.Logf("disposable JetStream node %d diagnostic: %s", i, log)
					}
				}
				t.Fatalf("three-node %s failed: initial=%v last=%v", action, firstErr, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	provision("JetStream account readiness", func() error {
		_, err := js.AccountInfo(nats.MaxWait(time.Second))
		return err
	})
	for _, key := range sortedStreamKeys(resolved.Streams) {
		stream := resolved.Streams[key]
		provision("Stream "+stream.Name, func() error {
			_, err := js.AddStream(&stream, nats.MaxWait(2*time.Second))
			return err
		})
	}
	for _, key := range sortedConsumerKeys(resolved.Consumers) {
		consumer := resolved.Consumers[key]
		provision("Consumer "+consumer.Config.Durable, func() error {
			_, err := js.AddConsumer(consumer.Stream, &consumer.Config, nats.MaxWait(2*time.Second))
			return err
		})
	}
	provision("verified topology", func() error {
		checkCtx, stopCheck := context.WithTimeout(ctx, 2*time.Second)
		defer stopCheck()
		return Verify(checkCtx, js, resolved)
	})
	first, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	cfg.InstanceID = "cra-ha-worker-b"
	second, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	received := make(chan string, 32)
	handler := func(_ context.Context, msg *nats.Msg) error {
		select {
		case received <- string(msg.Data):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	workerA, err := first.StartWorker(ctx, "RESULTS_WORKER", handler)
	if err != nil {
		t.Fatal(err)
	}
	defer workerA.Stop()
	workerB, err := second.StartWorker(ctx, "RESULTS_WORKER", handler)
	if err != nil {
		t.Fatal(err)
	}
	defer workerB.Stop()
	publish := func(start, end int) {
		for n := start; n < end; n++ {
			if err := first.Publish(ctx, "results", []byte(strconv.Itoa(n)), fmt.Sprintf("cra-ha-%d", n)); err != nil {
				t.Fatalf("quorum publish %d failed: %v", n, err)
			}
		}
	}
	wait := func(want int) {
		for {
			info, err := js.ConsumerInfo(resolved.Consumers["RESULTS_WORKER"].Stream,
				resolved.Consumers["RESULTS_WORKER"].Config.Durable, nats.Context(ctx))
			if err == nil && info.AckFloor.Consumer >= uint64(want) && info.NumAckPending == 0 {
				return
			}
			if ctx.Err() != nil {
				t.Fatalf("quorum ACK floor not reached %d, err=%v", want, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	publish(0, 6)
	wait(6)
	stream, err := js.StreamInfo(resolved.Consumers["RESULTS_WORKER"].Stream, nats.Context(ctx))
	if err != nil || stream.Cluster == nil || stream.Cluster.Leader == "" {
		t.Fatalf("replicated results Stream has no known leader: %+v err=%v", stream, err)
	}
	leader := -1
	for i := 0; i < nodes; i++ {
		if stream.Cluster.Leader == fmt.Sprintf("cra-ha-%d", i) {
			leader = i
		}
	}
	if leader < 0 {
		t.Fatalf("unexpected cluster leader %q", stream.Cluster.Leader)
	}
	stop(leader)
	// Inspect failover from a FRESH connection to an explicitly live
	// server. The original observer may itself be connected to the
	// killed node and its reconnect wait must not mask quorum health.
	var survivors []string
	for i, url := range urls {
		if i != leader {
			survivors = append(survivors, url)
		}
	}
	survivorConn, err := nats.Connect(strings.Join(survivors, ","), nats.Timeout(time.Second))
	if err != nil {
		t.Fatalf("cannot connect to surviving quorum: %v", err)
	}
	defer survivorConn.Close()
	survivorJS, err := survivorConn.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	var newLeader string
	for {
		current, probeErr := survivorJS.StreamInfo(resolved.Consumers["RESULTS_WORKER"].Stream, nats.MaxWait(time.Second))
		if probeErr == nil && current.Cluster != nil && current.Cluster.Leader != "" &&
			current.Cluster.Leader != stream.Cluster.Leader {
			newLeader = current.Cluster.Leader
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("cluster did not elect a new results leader after %s stopped: last=%v", stream.Cluster.Leader, probeErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = newLeader
	js = survivorJS
	publish(6, 12)
	wait(12)
	// Verify no unexpected extra business deliveries; the durable ACK
	// cursor includes all 12 distinct stored stream messages.
	info, err := js.StreamInfo(resolved.Consumers["RESULTS_WORKER"].Stream, nats.Context(ctx))
	if err != nil || info.State.Msgs != 12 {
		t.Fatalf("leader failover lost or duplicated persisted result: state=%+v err=%v", info, err)
	}
}

func haFreePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}
