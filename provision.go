package natskit

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"

	"github.com/nats-io/nats.go"
)

// Verify is strictly read-only. Application startup must fail closed if an
// expected resource is absent or has drifted from its declarative contract.
func Verify(ctx context.Context, js nats.JetStreamContext, topology Resolved) error {
	for _, key := range sortedStreamKeys(topology.Streams) {
		want := topology.Streams[key]
		info, err := js.StreamInfo(want.Name, nats.Context(ctx))
		if err != nil {
			return fmt.Errorf("inspect stream %s: %w", want.Name, err)
		}
		if !sameStream(info.Config, want) {
			return fmt.Errorf("stream %s configuration drift; refusing implicit update", want.Name)
		}
	}
	for _, key := range sortedConsumerKeys(topology.Consumers) {
		want := topology.Consumers[key]
		info, err := js.ConsumerInfo(want.Stream, want.Config.Durable, nats.Context(ctx))
		if err != nil {
			return fmt.Errorf("inspect durable %s/%s: %w", want.Stream, want.Config.Durable, err)
		}
		if !sameConsumer(info.Config, want.Config) {
			return fmt.Errorf("durable %s/%s configuration drift; refusing implicit update", want.Stream, want.Config.Durable)
		}
	}
	return nil
}

// ProvisionMissing is ONLY for an explicitly invoked administrative command.
// It never updates or deletes existing resources. It preflights all resources
// for drift before creating anything, then verifies the complete topology.
func ProvisionMissing(ctx context.Context, js nats.JetStreamContext, topology Resolved) error {
	var newStreams []nats.StreamConfig
	missingStreams := make(map[string]bool)
	for _, key := range sortedStreamKeys(topology.Streams) {
		want := topology.Streams[key]
		info, err := js.StreamInfo(want.Name, nats.Context(ctx))
		if errors.Is(err, nats.ErrStreamNotFound) {
			newStreams = append(newStreams, want)
			missingStreams[want.Name] = true
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect stream %s: %w", want.Name, err)
		}
		if !sameStream(info.Config, want) {
			return fmt.Errorf("stream %s drift; refusing update", want.Name)
		}
	}
	var newConsumers []ResolvedConsumer
	for _, key := range sortedConsumerKeys(topology.Consumers) {
		want := topology.Consumers[key]
		if missingStreams[want.Stream] {
			newConsumers = append(newConsumers, want)
			continue
		}
		info, err := js.ConsumerInfo(want.Stream, want.Config.Durable, nats.Context(ctx))
		if errors.Is(err, nats.ErrConsumerNotFound) {
			newConsumers = append(newConsumers, want)
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect durable %s: %w", want.Config.Durable, err)
		}
		if !sameConsumer(info.Config, want.Config) {
			return fmt.Errorf("durable %s drift; refusing update", want.Config.Durable)
		}
	}
	for _, want := range newStreams {
		if _, err := js.AddStream(&want, nats.Context(ctx)); err != nil {
			return fmt.Errorf("create stream %s: %w", want.Name, err)
		}
	}
	for _, want := range newConsumers {
		if _, err := js.AddConsumer(want.Stream, &want.Config, nats.Context(ctx)); err != nil {
			return fmt.Errorf("create durable %s: %w", want.Config.Durable, err)
		}
	}
	return Verify(ctx, js, topology)
}

func sortedStreamKeys(m map[string]nats.StreamConfig) []string {
	s := make([]string, 0, len(m))
	for k := range m {
		s = append(s, k)
	}
	sort.Strings(s)
	return s
}
func sortedConsumerKeys(m map[string]ResolvedConsumer) []string {
	s := make([]string, 0, len(m))
	for k := range m {
		s = append(s, k)
	}
	sort.Strings(s)
	return s
}
func sortedEqual(a, b []string) bool { return slices.Equal(sortedCopy(a), sortedCopy(b)) }
func sortedCopy(a []string) []string { b := slices.Clone(a); sort.Strings(b); return b }

func sameStream(a, b nats.StreamConfig) bool {
	return a.Name == b.Name && sortedEqual(a.Subjects, b.Subjects) && a.Storage == b.Storage &&
		a.Retention == b.Retention && a.Discard == b.Discard && a.MaxAge == b.MaxAge &&
		a.Duplicates == b.Duplicates && a.MaxBytes == b.MaxBytes && a.MaxMsgSize == b.MaxMsgSize && a.Replicas == b.Replicas &&
		a.NoAck == b.NoAck && a.DiscardNewPerSubject == b.DiscardNewPerSubject &&
		unlimitedLimit(a.MaxMsgs) == unlimitedLimit(b.MaxMsgs) &&
		unlimitedLimit(a.MaxMsgsPerSubject) == unlimitedLimit(b.MaxMsgsPerSubject) &&
		a.Sealed == b.Sealed && a.AllowRollup == b.AllowRollup &&
		a.AllowMsgTTL == b.AllowMsgTTL && a.SubjectDeleteMarkerTTL == b.SubjectDeleteMarkerTTL &&
		reflect.DeepEqual(a.SubjectTransform, b.SubjectTransform) &&
		reflect.DeepEqual(a.RePublish, b.RePublish) &&
		reflect.DeepEqual(a.Mirror, b.Mirror) &&
		reflect.DeepEqual(a.Sources, b.Sources)
}

func sameConsumer(a, b nats.ConsumerConfig) bool {
	return a.Durable == b.Durable && a.FilterSubject == b.FilterSubject && sortedEqual(a.FilterSubjects, b.FilterSubjects) &&
		a.AckPolicy == b.AckPolicy && a.DeliverPolicy == b.DeliverPolicy && a.AckWait == b.AckWait &&
		a.MaxDeliver == b.MaxDeliver && a.MaxAckPending == b.MaxAckPending && slices.Equal(a.BackOff, b.BackOff) &&
		a.ReplayPolicy == b.ReplayPolicy && a.DeliverSubject == b.DeliverSubject &&
		a.DeliverGroup == b.DeliverGroup && a.HeadersOnly == b.HeadersOnly &&
		a.InactiveThreshold == b.InactiveThreshold && a.MemoryStorage == b.MemoryStorage &&
		a.RateLimit == b.RateLimit
}

// unlimitedLimit normalizes the Go client's unset limit (0) to the broker's
// unlimited default (-1). Any positive retention cap is material drift.
func unlimitedLimit(value int64) int64 {
	if value == 0 {
		return -1
	}
	return value
}
