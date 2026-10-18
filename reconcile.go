package natskit

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/nats-io/nats.go"
)

// ReconcileReport counts broker changes made by an explicitly managed startup.
// It never exposes payloads, credentials, or business identifiers.
type ReconcileReport struct {
	CreatedStreams   int
	ExpandedStreams  int
	CreatedConsumers int
}

// ReconcileBlocked identifies unsupported topology drift with actionable
// diagnostics. A refusal does not imply that any broker data was deleted.
type ReconcileBlocked struct {
	Code            string
	Resource        string
	Current         string
	Desired         string
	Reason          string
	Risk            string
	SuggestedAction string
	AckPending      int
	NumPending      uint64
}

func (e *ReconcileBlocked) Error() string {
	if strings.HasPrefix(e.Code, "JS_CONSUMER_") {
		return fmt.Sprintf("code=%s resource=%s current=%s desired=%s ack_pending=%d num_pending=%d reason=%q risk=%q suggested_action=%q; refusing update",
			e.Code, e.Resource, e.Current, e.Desired, e.AckPending, e.NumPending, e.Reason, e.Risk, e.SuggestedAction)
	}
	return fmt.Sprintf("code=%s resource=%s current=%s desired=%s reason=%q risk=%q suggested_action=%q; refusing update",
		e.Code, e.Resource, e.Current, e.Desired, e.Reason, e.Risk, e.SuggestedAction)
}

type plannedStream struct {
	want   nats.StreamConfig
	create bool
	expand bool
}
type plannedConsumer struct {
	want   ResolvedConsumer
	create bool
}

// Reconcile preflights the COMPLETE topology before any broker mutation.
// Missing resources and additive Stream subject updates are supported. These
// operations preserve existing messages, consumer filters and ACK cursors.
//
// Replacing/removing subjects or changing a durable filter requires a
// coordinated producer/consumer cutover: an instantaneous broker snapshot
// cannot establish that all external writers and other CRA replicas stopped.
// Those migrations fail closed with an actionable reason instead of silently
// losing message capture or delivery. No Delete/Purge calls are made.
//
// The broker cannot apply multiple API mutations atomically. On a transient
// failure after preflight, only the above non-destructive changes may have
// succeeded. A subsequent invocation safely converges or fails on drift.
func Reconcile(ctx context.Context, js nats.JetStreamContext, topology Resolved) (ReconcileReport, error) {
	var result ReconcileReport
	streams := make([]plannedStream, 0, len(topology.Streams))
	missing := make(map[string]bool)
	for _, key := range sortedStreamKeys(topology.Streams) {
		want := topology.Streams[key]
		info, err := js.StreamInfo(want.Name, nats.Context(ctx))
		if errors.Is(err, nats.ErrStreamNotFound) {
			missing[want.Name] = true
			streams = append(streams, plannedStream{want: want, create: true})
			continue
		}
		if err != nil {
			return result, fmt.Errorf("inspect Stream %s: %w", want.Name, err)
		}
		if sameStream(info.Config, want) {
			continue
		}
		// Inspect policy separately: a subject-only difference is eligible
		// solely when all previously captured subjects remain captured.
		normalized := info.Config
		normalized.Subjects = want.Subjects
		if !sameStream(normalized, want) {
			return result, &ReconcileBlocked{
				Code: "JS_STREAM_POLICY_DRIFT", Resource: "stream/" + want.Name,
				Current: streamPolicySummary(info.Config), Desired: streamPolicySummary(want),
				Reason:          "persistent Stream policies differ from the approved topology",
				Risk:            "changing quotas, retention or storage may evict messages or affect availability",
				SuggestedAction: "restore compatible settings or schedule a backup-backed operator-reviewed change; never delete the JetStream data directory",
			}
		}
		if !subjectsContained(info.Config.Subjects, want.Subjects) {
			return result, &ReconcileBlocked{
				Code: "JS_STREAM_SUBJECT_DRIFT", Resource: "stream/" + want.Name,
				Current:         fmt.Sprintf("%q", info.Config.Subjects),
				Desired:         fmt.Sprintf("%q", want.Subjects),
				Reason:          "configured subjects remove or replace an existing Stream capture",
				Risk:            "external producers may still publish on the removed subjects, and previously retained data or existing durable filters may become stranded",
				SuggestedAction: "restore the old subject mapping to recover, or coordinate a verified writer/consumer drain and lossless cutover; never delete the Stream or data directory",
			}
		}
		streams = append(streams, plannedStream{want: want, expand: true})
	}
	consumers := make([]plannedConsumer, 0, len(topology.Consumers))
	for _, key := range sortedConsumerKeys(topology.Consumers) {
		want := topology.Consumers[key]
		if missing[want.Stream] {
			consumers = append(consumers, plannedConsumer{want: want, create: true})
			continue
		}
		info, err := js.ConsumerInfo(want.Stream, want.Config.Durable, nats.Context(ctx))
		if errors.Is(err, nats.ErrConsumerNotFound) {
			consumers = append(consumers, plannedConsumer{want: want, create: true})
			continue
		}
		if err != nil {
			return result, fmt.Errorf("inspect Consumer %s/%s: %w", want.Stream, want.Config.Durable, err)
		}
		if sameConsumer(info.Config, want.Config) {
			continue
		}
		if info.Config.FilterSubject != want.Config.FilterSubject ||
			!sortedEqual(info.Config.FilterSubjects, want.Config.FilterSubjects) {
			return result, &ReconcileBlocked{
				Code:       "JS_CONSUMER_FILTER_DRIFT",
				Resource:   "consumer/" + want.Stream + "/" + want.Config.Durable,
				Current:    fmt.Sprintf("filter=%q filters=%q", info.Config.FilterSubject, info.Config.FilterSubjects),
				Desired:    fmt.Sprintf("filter=%q filters=%q", want.Config.FilterSubject, want.Config.FilterSubjects),
				AckPending: info.NumAckPending, NumPending: info.NumPending,
				Reason:          "durable filter differs from the configured result subject",
				Risk:            "an uncoordinated filter update may skip old or in-flight results and lose cursor continuity",
				SuggestedAction: "restore the previous result subject mapping, or verify a coordinated drain and cursor-preserving migration; never delete and recreate the durable automatically",
			}
		}
		return result, &ReconcileBlocked{
			Code:       "JS_CONSUMER_POLICY_DRIFT",
			Resource:   "consumer/" + want.Stream + "/" + want.Config.Durable,
			Current:    consumerPolicySummary(info.Config),
			Desired:    consumerPolicySummary(want),
			AckPending: info.NumAckPending, NumPending: info.NumPending,
			Reason:          "durable ACK/delivery configuration differs from the approved topology",
			Risk:            "changing delivery settings without a review may affect redelivery or acknowledgement safety",
			SuggestedAction: "restore the previous policy or plan a drained and verified deployment; preserve the durable and its ACK cursor",
		}
	}
	// The full preflight passed. Concurrent instances may independently reach
	// this point; re-read after write errors and accept only the exact winner.
	for _, change := range streams {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		want := change.want
		switch {
		case change.create:
			if _, err := js.AddStream(&want, nats.Context(ctx)); err != nil {
				info, check := js.StreamInfo(want.Name, nats.Context(ctx))
				if check != nil || !sameStream(info.Config, want) {
					return result, fmt.Errorf("create Stream %s: %w", want.Name, err)
				}
			}
			result.CreatedStreams++
		case change.expand:
			info, err := js.StreamInfo(want.Name, nats.Context(ctx))
			if err != nil {
				return result, fmt.Errorf("reinspect Stream %s: %w", want.Name, err)
			}
			if sameStream(info.Config, want) {
				continue
			}
			before := info.Config
			before.Subjects = want.Subjects
			if !sameStream(before, want) || !subjectsContained(info.Config.Subjects, want.Subjects) {
				return result, fmt.Errorf("Stream %s changed concurrently; refusing update", want.Name)
			}
			updated := info.Config
			updated.Subjects = slices.Clone(want.Subjects)
			if _, err := js.UpdateStream(&updated, nats.Context(ctx)); err != nil {
				latest, check := js.StreamInfo(want.Name, nats.Context(ctx))
				if check != nil || !sameStream(latest.Config, want) {
					return result, fmt.Errorf("expand Stream %s subjects: %w", want.Name, err)
				}
			}
			result.ExpandedStreams++
		}
	}
	for _, change := range consumers {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if !change.create {
			continue
		}
		want := change.want
		if _, err := js.AddConsumer(want.Stream, &want.Config, nats.Context(ctx)); err != nil {
			info, check := js.ConsumerInfo(want.Stream, want.Config.Durable, nats.Context(ctx))
			if check != nil || !sameConsumer(info.Config, want.Config) {
				return result, fmt.Errorf("create Consumer %s/%s: %w", want.Stream, want.Config.Durable, err)
			}
		}
		result.CreatedConsumers++
	}
	if err := Verify(ctx, js, topology); err != nil {
		return result, fmt.Errorf("verify reconciled topology: %w", err)
	}
	return result, nil
}

func subjectsContained(current, desired []string) bool {
	// Exact pattern inclusion only. A broadened wildcard is not automatically
	// treated as equivalent because it may change capture across other Streams.
	if len(current) == 0 {
		return false
	}
	have := make(map[string]struct{}, len(desired))
	for _, value := range desired {
		have[value] = struct{}{}
	}
	for _, value := range current {
		if _, ok := have[value]; !ok {
			return false
		}
	}
	return len(current) <= len(desired)
}

func streamPolicySummary(c nats.StreamConfig) string {
	return fmt.Sprintf("storage=%v retention=%v discard=%v replicas=%d max_bytes=%d max_age=%s max_msg_size=%d duplicate_window=%s no_ack=%t discard_new_per_subject=%t",
		c.Storage, c.Retention, c.Discard, c.Replicas, c.MaxBytes, c.MaxAge, c.MaxMsgSize, c.Duplicates, c.NoAck, c.DiscardNewPerSubject)
}

func consumerPolicySummary(c nats.ConsumerConfig) string {
	return fmt.Sprintf("ack_policy=%v deliver_policy=%v ack_wait=%s max_deliver=%d max_ack_pending=%d backoff=%v replay_policy=%v deliver_subject=%q deliver_group=%q headers_only=%t",
		c.AckPolicy, c.DeliverPolicy, c.AckWait, c.MaxDeliver, c.MaxAckPending, c.BackOff, c.ReplayPolicy, c.DeliverSubject, c.DeliverGroup, c.HeadersOnly)
}

// ReconcileErrorCode extracts the stable failure classification for application
// logging. Do not rely on matching natural-language error messages.
func ReconcileErrorCode(err error) string {
	var blocked *ReconcileBlocked
	if errors.As(err, &blocked) {
		return blocked.Code
	}
	if err == nil {
		return ""
	}
	return "JS_RECONCILE_ERROR"
}

// IsUnsafeDrift lets applications distinguish changes that require a
// coordinated cutover from retryable transport errors.
func IsUnsafeDrift(err error) bool {
	var blocked *ReconcileBlocked
	return errors.As(err, &blocked) && strings.HasSuffix(blocked.Code, "_DRIFT")
}
