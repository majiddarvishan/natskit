package natskit

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func publishOnlyConfig(url, instanceID, outgoing string) Config {
	return Config{
		InstanceID: instanceID,
		Connection: Connection{URLs: []string{url}},
		Subjects:   map[string]string{"outbound": outgoing},
		JetStream: JetStreamConfig{
			Enabled: true, Namespace: instanceID, Provisioning: "verify",
		},
	}
}

func TestPublishOnlyTopologyDoesNotRequireManagedStreams(t *testing.T) {
	cfg := publishOnlyConfig("nats://127.0.0.1:4222", "A", "subject.one")
	resolved, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved.Streams) != 0 || len(resolved.Consumers) != 0 || resolved.Subjects["outbound"] != "subject.one" {
		t.Fatalf("outbound address accidentally enrolled in managed topology: %+v", resolved)
	}
	cfg.Subjects["outbound"] = "subject.>"
	client := &Client{Topology: Resolved{Subjects: cfg.Subjects}}
	if _, err := client.Subject("outbound"); err == nil {
		t.Fatal("wildcard must not be accepted for publish")
	}
}

func TestDisposableIndependentBidirectionalPublishAndRemap(t *testing.T) {
	url := startDisposableServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	operator, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer operator.Close()
	js, err := operator.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	type receiver struct {
		stream, durable, subject string
	}
	receivers := []receiver{
		{"RX_ONE", "DURABLE_ONE", "subject.one"},
		{"RX_TWO", "DURABLE_TWO", "subject.two"},
		{"RX_X", "DURABLE_X", "subject.x"},
		{"RX_Y", "DURABLE_Y", "subject.y"},
	}
	streamBefore := make(map[string]nats.StreamConfig)
	consumerBefore := make(map[string]nats.ConsumerConfig)
	for _, receiver := range receivers {
		_, err := js.AddStream(&nats.StreamConfig{
			Name: receiver.stream, Subjects: []string{receiver.subject},
			Storage: nats.MemoryStorage, Retention: nats.LimitsPolicy, MaxAge: time.Hour,
		}, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		_, err = js.AddConsumer(receiver.stream, &nats.ConsumerConfig{
			Durable: receiver.durable, FilterSubject: receiver.subject,
			AckPolicy: nats.AckExplicitPolicy,
		}, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		info, err := js.StreamInfo(receiver.stream, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		consumer, err := js.ConsumerInfo(receiver.stream, receiver.durable, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		streamBefore[receiver.stream] = info.Config
		consumerBefore[receiver.durable] = consumer.Config
	}
	publishAndReceive := func(sender *Client, target receiver, id string) PublishReceipt {
		t.Helper()
		options := PublishOptions{ExpectedStream: target.stream}
		receipt, err := sender.PublishConfirmedWithOptions(ctx, "outbound", []byte(id), id, options)
		if err != nil {
			t.Fatal(err)
		}
		if !receipt.JetStream || receipt.Stream != target.stream || receipt.Sequence != 1 || receipt.Duplicate {
			t.Fatalf("expected genuine first PubAck from %s: %+v", target.stream, receipt)
		}
		duplicate, err := sender.PublishConfirmedWithOptions(ctx, "outbound", []byte(id), id, options)
		if err != nil || !duplicate.Duplicate || duplicate.Sequence != receipt.Sequence || duplicate.Stream != receipt.Stream {
			t.Fatalf("expected broker duplicate PubAck: %+v err=%v", duplicate, err)
		}
		subscription, err := js.PullSubscribe(target.subject, target.durable,
			nats.Bind(target.stream, target.durable))
		if err != nil {
			t.Fatal(err)
		}
		defer subscription.Unsubscribe()
		msgs, err := subscription.Fetch(1, nats.MaxWait(2*time.Second))
		if err != nil || len(msgs) != 1 || string(msgs[0].Data) != id {
			t.Fatalf("receiver %s did not get independent message %q: msgs=%v err=%v", target.durable, id, msgs, err)
		}
		if err := msgs[0].AckSync(nats.Context(ctx)); err != nil {
			t.Fatal(err)
		}
		return receipt
	}
	runPair := func(subjectA, subjectB string, first, second receiver) {
		t.Helper()
		a, err := Connect(ctx, publishOnlyConfig(url, "A", subjectA))
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		b, err := Connect(ctx, publishOnlyConfig(url, "B", subjectB))
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		publishAndReceive(a, first, "A-"+subjectA)
		publishAndReceive(b, second, "B-"+subjectB)
	}
	// Receiver/operator owns all four Streams and Durables. Publishers own none.
	runPair("subject.one", "subject.two", receivers[0], receivers[1])
	// Remapping only changes address configuration, never existing messages/cursors.
	runPair("subject.x", "subject.y", receivers[2], receivers[3])

	for _, receiver := range receivers {
		info, err := js.StreamInfo(receiver.stream, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		consumer, err := js.ConsumerInfo(receiver.stream, receiver.durable, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(info.Config, streamBefore[receiver.stream]) ||
			!reflect.DeepEqual(consumer.Config, consumerBefore[receiver.durable]) ||
			info.State.Msgs != 1 {
			t.Fatalf("publisher changed receiver-owned resources or message history: %s", receiver.stream)
		}
		msg, err := js.GetMsg(receiver.stream, 1, nats.Context(ctx))
		if err != nil || !strings.Contains(string(msg.Data), receiver.subject) {
			t.Fatalf("retained independent message missing from %s: %v", receiver.stream, err)
		}
	}
}

func TestDisposableExternalPublishFailuresDoNotMutateStreams(t *testing.T) {
	url := startDisposableServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name: "RECEIVER", Subjects: []string{"captured.subject"},
		Storage: nats.MemoryStorage, MaxAge: time.Hour,
	}, nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	cfg := publishOnlyConfig(url, "PUBLISHER", "captured.subject")
	client, err := ConnectReconciled(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.Reconciliation != (ReconcileReport{}) {
		t.Fatalf("no owned resources should be reconciled: %+v", client.Reconciliation)
	}
	if _, err := client.PublishConfirmedWithOptions(ctx, "outbound", []byte("wrong"), "wrong",
		PublishOptions{ExpectedStream: "NOT_RECEIVER"}); err == nil {
		t.Fatal("broker must reject mismatched expected Stream")
	}
	if _, err := client.PublishConfirmedWithOptions(ctx, "outbound", []byte("wrong"), "wrong",
		PublishOptions{ExpectedStream: "invalid.stream"}); err == nil {
		t.Fatal("invalid Stream assertion accepted")
	}
	cfg.Subjects["outbound"] = "missing.capture"
	orphan, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer orphan.Close()
	if _, err := orphan.PublishConfirmed(ctx, "outbound", []byte("not-stored"), "missing"); err == nil {
		t.Fatal("missing JetStream capture must fail, not fall back to Core NATS")
	}
	info, err := js.StreamInfo("RECEIVER", nats.Context(ctx))
	if err != nil || info.State.Msgs != 0 || len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != "captured.subject" {
		t.Fatalf("failed publications unexpectedly mutated receiver Stream: info=%v err=%v", info, err)
	}
}

func TestDisposableCorePublishHasNoJetStreamReceipt(t *testing.T) {
	url := startDisposableServer(t)
	cfg := publishOnlyConfig(url, "CORE", "core.outbound")
	cfg.JetStream = JetStreamConfig{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	sub, err := client.Conn.SubscribeSync("core.outbound")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Conn.FlushWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishConfirmedWithOptions(ctx, "outbound", []byte("no"),
		"", PublishOptions{ExpectedStream: "ANY"}); err == nil {
		t.Fatal("Core publishing cannot fulfill a JetStream assertion")
	}
	receipt, err := client.PublishConfirmed(ctx, "outbound", []byte("core"), "no-core-dedup")
	if err != nil || receipt != (PublishReceipt{}) {
		t.Fatalf("Core mode must not fabricate a PubAck: %+v err=%v", receipt, err)
	}
	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil || string(msg.Data) != "core" {
		t.Fatalf("Core receiver did not receive message: %v", err)
	}
}
