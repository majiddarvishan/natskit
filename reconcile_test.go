package natskit

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestReconcileSubjectContainmentRequiresExactAddition(t *testing.T) {
	for _, tc := range []struct {
		have, want []string
		ok         bool
	}{
		{[]string{"a", "b"}, []string{"b", "a", "c"}, true},
		{[]string{"a"}, []string{"b"}, false},
		{[]string{"a.>"}, []string{"a.b"}, false},
		{nil, []string{"a"}, false},
	} {
		if got := subjectsContained(tc.have, tc.want); got != tc.ok {
			t.Fatalf("subjectsContained(%q,%q)=%v, want %v", tc.have, tc.want, got, tc.ok)
		}
	}
}

func TestDisposableReconcileCreatesAndPreservesMessages(t *testing.T) {
	url := startDisposableServer(t)
	cfg := sample(t)
	cfg.InstanceID = "managed-test"
	cfg.Connection.URLs = []string{url}
	resolved, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	js, err := conn.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	report, err := Reconcile(ctx, js, resolved)
	if err != nil || report.CreatedStreams != len(resolved.Streams) || report.CreatedConsumers != len(resolved.Consumers) {
		t.Fatalf("managed bootstrap report=%+v err=%v", report, err)
	}
	original := resolved.Streams["COMMANDS"]
	subject := original.Subjects[0]
	if _, err := js.Publish(subject, []byte("retained-command"), nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	next := resolved
	next.Streams = make(map[string]nats.StreamConfig, len(resolved.Streams))
	for key, value := range resolved.Streams {
		next.Streams[key] = value
	}
	expanded := original
	expanded.Subjects = append(slices.Clone(original.Subjects), "additional.command.subject")
	next.Streams["COMMANDS"] = expanded
	report, err = Reconcile(ctx, js, next)
	if err != nil || report.ExpandedStreams != 1 {
		t.Fatalf("safe addition report=%+v err=%v", report, err)
	}
	info, err := js.StreamInfo(original.Name, nats.Context(ctx))
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("old message lost during additive update: info=%v err=%v", info, err)
	}
	if _, err := js.Publish("additional.command.subject", []byte("new"), nats.Context(ctx)); err != nil {
		t.Fatalf("new subject not captured: %v", err)
	}
	if err := Verify(ctx, js, next); err != nil {
		t.Fatalf("verify additive result: %v", err)
	}
}

func TestDisposableReconcileBlocksFilterAndSubjectReplacementBeforeAnyMutation(t *testing.T) {
	url := startDisposableServer(t)
	cfg := sample(t)
	cfg.InstanceID = "blocked-test"
	cfg.Connection.URLs = []string{url}
	resolved, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	js, err := conn.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if _, err := Reconcile(ctx, js, resolved); err != nil {
		t.Fatal(err)
	}
	original := resolved.Streams["COMMANDS"]
	changed := resolved
	changed.Streams = make(map[string]nats.StreamConfig, len(resolved.Streams))
	for k, stream := range resolved.Streams {
		changed.Streams[k] = stream
	}
	stream := original
	stream.Subjects = []string{"replacement.subject"}
	changed.Streams["COMMANDS"] = stream
	_, err = Reconcile(ctx, js, changed)
	if ReconcileErrorCode(err) != "JS_STREAM_SUBJECT_DRIFT" || !IsUnsafeDrift(err) {
		t.Fatalf("subject replacement should be blocked with code: %v", err)
	}
	for _, text := range []string{"reason=", "risk=", "suggested_action=", "refusing update"} {
		if !strings.Contains(err.Error(), text) {
			t.Fatalf("missing %s: %v", text, err)
		}
	}
	after, err := js.StreamInfo(original.Name, nats.Context(ctx))
	if err != nil || !sameStream(after.Config, original) {
		t.Fatalf("blocked attempt changed Stream: %v", err)
	}
	changed = resolved
	changed.Consumers = make(map[string]ResolvedConsumer, len(resolved.Consumers))
	for k, c := range resolved.Consumers {
		changed.Consumers[k] = c
	}
	for key, c := range changed.Consumers {
		cc := c.Config
		cc.FilterSubject = "replacement.result.subject"
		c.Config = cc
		changed.Consumers[key] = c
		break
	}
	_, err = Reconcile(ctx, js, changed)
	if ReconcileErrorCode(err) != "JS_CONSUMER_FILTER_DRIFT" {
		t.Fatalf("filter change must block: %v", err)
	}
	var blocked *ReconcileBlocked
	if !errors.As(err, &blocked) || blocked.SuggestedAction == "" {
		t.Fatalf("missing typed remedy: %v", err)
	}
}

func TestDisposableReconcileConcurrentManagedReplicas(t *testing.T) {
	url := startDisposableServer(t)
	cfg := sample(t)
	cfg.InstanceID = "replica-1"
	cfg.Connection.URLs = []string{url}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errorsCh := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := ConnectReconciled(ctx, cfg)
			if err == nil {
				err = c.Close()
			}
			errorsCh <- err
		}()
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent managed startup: %v", err)
		}
	}
}

func TestPolicySummariesExplainActualSettings(t *testing.T) {
	stream := streamPolicySummary(nats.StreamConfig{MaxBytes: 123, Replicas: 1})
	if !strings.Contains(stream, "max_bytes=123") || !strings.Contains(stream, "replicas=1") {
		t.Fatalf("stream policy summary omitted actual values: %s", stream)
	}
	consumer := consumerPolicySummary(nats.ConsumerConfig{MaxAckPending: 128})
	if !strings.Contains(consumer, "max_ack_pending=128") {
		t.Fatalf("consumer policy summary omitted actual values: %s", consumer)
	}
}

func TestDisposableReconcileBlocksUnmanagedRetentionCap(t *testing.T) {
	url := startDisposableServer(t)
	cfg := sample(t)
	cfg.Connection.URLs = []string{url}
	topology, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if _, err := Reconcile(ctx, js, topology); err != nil {
		t.Fatal(err)
	}
	stream := topology.Streams["COMMANDS"]
	info, err := js.StreamInfo(stream.Name, nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	changed := info.Config
	changed.MaxMsgs = 2
	if _, err := js.UpdateStream(&changed, nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	report, err := Reconcile(ctx, js, topology)
	if report != (ReconcileReport{}) || ReconcileErrorCode(err) != "JS_STREAM_POLICY_DRIFT" || !IsUnsafeDrift(err) {
		t.Fatalf("hidden retention cap must stop startup before changes: report=%+v err=%v", report, err)
	}
	after, err := js.StreamInfo(stream.Name, nats.Context(ctx))
	if err != nil || after.Config.MaxMsgs != 2 {
		t.Fatalf("refused reconciliation unexpectedly changed existing policy: %v", err)
	}
}

func TestDisposableSubjectReplacementRequiresExplicitPolicy(t *testing.T) {
	url := startDisposableServer(t)
	cfg := sample(t)
	cfg.Connection.URLs = []string{url}
	resolved, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if _, err := Reconcile(ctx, js, resolved); err != nil {
		t.Fatal(err)
	}
	original := resolved.Streams["COMMANDS"]
	if original.MaxAge <= 0 {
		t.Fatalf("replacement fixture requires positive MaxAge: %v", original.MaxAge)
	}
	pub, err := js.Publish(original.Subjects[0], []byte("old-retained"), nats.Context(ctx))
	if err != nil {
		t.Fatal(err)
	}
	changed := resolved
	changed.Streams = make(map[string]nats.StreamConfig, len(resolved.Streams))
	for k, v := range resolved.Streams {
		changed.Streams[k] = v
	}
	replacement := original
	replacement.Subjects = []string{"changed.commands.subject"}
	changed.Streams["COMMANDS"] = replacement
	if _, err := Reconcile(ctx, js, changed); ReconcileErrorCode(err) != "JS_STREAM_SUBJECT_DRIFT" {
		t.Fatalf("default mode must reject replacement: %v", err)
	}
	changed.SubjectUpdatePolicy = "replace"
	report, err := Reconcile(ctx, js, changed)
	if err != nil || report.ReplacedStreams != 1 || report.ExpandedStreams != 0 {
		t.Fatalf("explicit replacement report=%+v err=%v", report, err)
	}
	info, err := js.StreamInfo(original.Name, nats.Context(ctx))
	if err != nil || !sortedEqual(info.Config.Subjects, replacement.Subjects) {
		t.Fatalf("destination capture mismatch: info=%v err=%v", info, err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("replacement must not purge old stored messages, got %d", info.State.Msgs)
	}
	retained, err := js.GetMsg(original.Name, pub.Sequence, nats.Context(ctx))
	if err != nil || string(retained.Data) != "old-retained" {
		t.Fatalf("old retained message disappeared: err=%v", err)
	}
	if _, err := js.Publish(replacement.Subjects[0], []byte("new-captured"), nats.Context(ctx)); err != nil {
		t.Fatal(err)
	}
	report, err = Reconcile(ctx, js, changed)
	if err != nil || report != (ReconcileReport{}) {
		t.Fatalf("idempotent replace should be no-op report=%+v err=%v", report, err)
	}
}

func TestDisposableSubjectReplacementPreflightStillBlocksConsumerDrift(t *testing.T) {
	url := startDisposableServer(t)
	cfg := sample(t)
	cfg.Connection.URLs = []string{url}
	resolved, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if _, err := Reconcile(ctx, js, resolved); err != nil {
		t.Fatal(err)
	}
	changed := resolved
	changed.SubjectUpdatePolicy = "replace"
	changed.Streams = make(map[string]nats.StreamConfig, len(resolved.Streams))
	for k, v := range resolved.Streams {
		changed.Streams[k] = v
	}
	before := changed.Streams["COMMANDS"]
	want := before
	want.Subjects = []string{"different.command"}
	changed.Streams["COMMANDS"] = want
	changed.Consumers = make(map[string]ResolvedConsumer, len(resolved.Consumers))
	for k, v := range resolved.Consumers {
		changed.Consumers[k] = v
	}
	for k, c := range changed.Consumers {
		c.Config.FilterSubject = "different.result"
		changed.Consumers[k] = c
		break
	}
	if _, err := Reconcile(ctx, js, changed); ReconcileErrorCode(err) != "JS_CONSUMER_FILTER_DRIFT" {
		t.Fatalf("replacement must never bypass consumer preflight: %v", err)
	}
	info, err := js.StreamInfo(before.Name, nats.Context(ctx))
	if err != nil || !sameStream(info.Config, before) {
		t.Fatalf("preflight must not change Stream on Consumer drift: info=%v err=%v", info, err)
	}
}
