package natskit

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

var resourceName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type Resolved struct {
	InstanceID          string
	Namespace           string
	SubjectUpdatePolicy string // additive by default; explicit replace allows subject-only Stream updates
	Subjects            map[string]string
	Streams             map[string]nats.StreamConfig
	Consumers           map[string]ResolvedConsumer
	Quarantine          *ResolvedQuarantine
}

type ResolvedConsumer struct {
	Stream string // resolved physical stream name
	Config nats.ConsumerConfig
	Worker ResolvedWorker
}

type ResolvedWorker struct {
	FetchBatch         int
	MaxConcurrency     int
	AckSyncTimeout     time.Duration
	InProgressInterval time.Duration
}

type ResolvedQuarantine struct {
	Stream, Subject   string
	On                map[string]bool
	ReconcileInterval time.Duration
}

func validPattern(p string) bool {
	if p == "" || strings.ContainsAny(p, " \t\r\n") {
		return false
	}
	tokens := strings.Split(p, ".")
	for i, t := range tokens {
		if t == "" || (strings.ContainsAny(t, "*>") && t != "*" && t != ">") {
			return false
		}
		if t == ">" && (i == 0 || i != len(tokens)-1) {
			return false
		}
	}
	return true
}

func patternsOverlap(left, right string) bool {
	a, b := strings.Split(left, "."), strings.Split(right, ".")
	aw, bw := a[len(a)-1] == ">", b[len(b)-1] == ">"
	ap, bp := len(a), len(b)
	if aw {
		ap--
	}
	if bw {
		bp--
	}
	if (!aw && !bw && ap != bp) || (!aw && bw && ap <= bp) || (aw && !bw && bp <= ap) {
		return false
	}
	n := ap
	if bp < n {
		n = bp
	}
	for i := 0; i < n; i++ {
		if a[i] != "*" && b[i] != "*" && a[i] != b[i] {
			return false
		}
	}
	return true
}

// patternsCover conservatively establishes that a consumer filter is wholly
// contained in one stream capture pattern. It never admits mere intersections.
func patternCovers(outer, inner string) bool {
	a, b := strings.Split(outer, "."), strings.Split(inner, ".")
	aw, bw := a[len(a)-1] == ">", b[len(b)-1] == ">"
	prefix := len(a)
	if aw {
		prefix--
	}
	if !aw && (bw || len(a) != len(b)) {
		return false
	}
	if aw && len(b) <= prefix {
		return false
	}
	for i := 0; i < prefix; i++ {
		if i >= len(b) || b[i] == ">" {
			return false
		}
		if a[i] != "*" && a[i] != b[i] {
			return false
		}
	}
	return true
}

func duration(value string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	d, e := time.ParseDuration(value)
	if e != nil || d <= 0 {
		return 0, fmt.Errorf("invalid positive duration %q", value)
	}
	return d, nil
}

func intValue(v *int, fallback int) int {
	if v == nil {
		return fallback
	}
	return *v
}
func int64Value(v *int64, fallback int64) int64 {
	if v == nil {
		return fallback
	}
	return *v
}
func int32Value(v *int32, fallback int32) int32 {
	if v == nil {
		return fallback
	}
	return *v
}

func (c Config) Resolve() (Resolved, error) {
	var r Resolved
	id, err := expandEnv(c.InstanceID)
	if err != nil {
		return r, err
	}
	if id == "" {
		return r, errors.New("nats.instance_id must be set; unique per running instance")
	}
	if len(c.Connection.URLs) == 0 {
		return r, errors.New("nats.connection.urls must not be empty")
	}
	for _, raw := range c.Connection.URLs {
		u, e := url.Parse(raw)
		if e != nil || (u.Scheme != "nats" && u.Scheme != "tls") || u.Host == "" || u.User != nil || u.Fragment != "" {
			return r, fmt.Errorf("invalid NATS connection URL %q", raw)
		}
	}
	if _, err = duration(c.Connection.DrainTimeout, 30*time.Second); err != nil {
		return r, err
	}
	if s := c.Connection.Security; s != nil && s.TLS != nil {
		if (s.TLS.CertFile == "") != (s.TLS.KeyFile == "") {
			return r, errors.New("TLS certificate and key must be provided together")
		}
	}
	r.InstanceID = id
	r.Subjects = make(map[string]string, len(c.Subjects))
	for key, value := range c.Subjects {
		if !resourceName.MatchString(key) || !validPattern(value) {
			return r, fmt.Errorf("invalid subject registry entry %q", key)
		}
		r.Subjects[key] = value
	}
	if !c.JetStream.Enabled {
		if c.JetStream.SubjectUpdatePolicy != "" {
			return r, errors.New("jetstream.subject_update_policy requires jetstream.enabled")
		}
		return r, nil
	}
	j := c.JetStream
	r.SubjectUpdatePolicy = j.SubjectUpdatePolicy
	if r.SubjectUpdatePolicy == "" {
		r.SubjectUpdatePolicy = "additive"
	}
	if r.SubjectUpdatePolicy != "additive" && r.SubjectUpdatePolicy != "replace" {
		return r, errors.New("jetstream.subject_update_policy must be additive or replace")
	}
	if !resourceName.MatchString(j.Namespace) {
		return r, errors.New("jetstream.namespace is required and must contain only letters, digits, - or _")
	}
	r.Namespace = strings.ToUpper(j.Namespace)
	if j.Provisioning != "verify" && j.Provisioning != "operator_only" {
		return r, errors.New("jetstream.provisioning must be verify or operator_only; automatic creation at runtime is not supported")
	}
	if len(j.Streams) == 0 {
		return r, errors.New("jetstream.streams must not be empty")
	}
	r.Streams = make(map[string]nats.StreamConfig, len(j.Streams))
	r.Consumers = make(map[string]ResolvedConsumer, len(j.Consumers))
	allPatterns := map[string]string{}
	physicalStreams := map[string]string{}
	for key, spec := range j.Streams {
		if !resourceName.MatchString(key) {
			return r, fmt.Errorf("invalid stream name %q", key)
		}
		if len(spec.Subjects) == 0 {
			return r, fmt.Errorf("stream %s has no subjects", key)
		}
		settings := StreamSettings{Storage: "file", Retention: "limits", Discard: "old"}
		settings = mergeStream(settings, j.Defaults.Stream)
		if spec.Profile != "" {
			p, ok := j.Profiles[spec.Profile]
			if !ok {
				return r, fmt.Errorf("stream %s references missing profile %q", key, spec.Profile)
			}
			settings = mergeStream(settings, p)
		}
		settings = mergeStream(settings, spec.StreamSettings)
		name := r.Namespace + "_" + key
		if spec.Name != "" {
			name = spec.Name
		}
		if previous, exists := physicalStreams[name]; exists {
			return r, fmt.Errorf("streams %s and %s share physical name %q", previous, key, name)
		}
		physicalStreams[name] = key
		if !resourceName.MatchString(name) {
			return r, fmt.Errorf("invalid physical stream name %q", name)
		}
		subjects := make([]string, 0, len(spec.Subjects))
		for _, ref := range spec.Subjects {
			subject, ok := r.Subjects[ref]
			if !ok {
				return r, fmt.Errorf("stream %s unknown subject alias %q", key, ref)
			}
			for previous, from := range allPatterns {
				if patternsOverlap(previous, subject) {
					return r, fmt.Errorf("subject overlap: %s (%s) and %s (%s)", previous, from, subject, key)
				}
			}
			allPatterns[subject] = key
			subjects = append(subjects, subject)
		}
		storage := nats.FileStorage
		if settings.Storage == "memory" {
			storage = nats.MemoryStorage
		} else if settings.Storage != "file" {
			return r, fmt.Errorf("stream %s unsupported storage %q", key, settings.Storage)
		}
		retention := nats.LimitsPolicy
		switch settings.Retention {
		case "limits":
		case "workqueue":
			retention = nats.WorkQueuePolicy
		case "interest":
			retention = nats.InterestPolicy
		default:
			return r, fmt.Errorf("stream %s invalid retention %q", key, settings.Retention)
		}
		discard := nats.DiscardOld
		switch settings.Discard {
		case "old":
		case "new":
			discard = nats.DiscardNew
		default:
			return r, fmt.Errorf("stream %s invalid discard %q", key, settings.Discard)
		}
		age, e := duration(settings.MaxAge, 0)
		if e != nil {
			return r, fmt.Errorf("stream %s max_age: %w", key, e)
		}
		dup, e := duration(settings.DuplicateWindow, 2*time.Minute)
		if e != nil {
			return r, fmt.Errorf("stream %s duplicate_window: %w", key, e)
		}
		replicas := intValue(settings.Replicas, 1)
		if replicas != 1 && replicas != 3 && replicas != 5 {
			return r, fmt.Errorf("stream %s replicas must be 1, 3 or 5", key)
		}
		maxBytes := int64Value(settings.MaxBytes, -1)
		maxMsgSize := int32Value(settings.MaxMsgSize, -1)
		if maxBytes == 0 || maxBytes < -1 || maxMsgSize == 0 || maxMsgSize < -1 {
			return r, fmt.Errorf("stream %s invalid storage limits", key)
		}
		r.Streams[key] = nats.StreamConfig{Name: name, Subjects: subjects, Storage: storage, Retention: retention, Discard: discard, MaxAge: age, Duplicates: dup, Replicas: replicas, MaxBytes: maxBytes, MaxMsgSize: maxMsgSize}
	}
	physicalConsumers := map[string]string{}
	for key, spec := range j.Consumers {
		if !resourceName.MatchString(key) {
			return r, fmt.Errorf("invalid consumer name %q", key)
		}
		stream, ok := r.Streams[spec.Stream]
		if !ok {
			return r, fmt.Errorf("consumer %s references missing stream %q", key, spec.Stream)
		}
		if len(spec.Filter) == 0 {
			return r, fmt.Errorf("consumer %s requires at least one filter", key)
		}
		filters := make([]string, 0, len(spec.Filter))
		for _, ref := range spec.Filter {
			value, ok := r.Subjects[ref]
			if !ok {
				return r, fmt.Errorf("consumer %s unknown filter alias %q", key, ref)
			}
			covered := false
			for _, p := range stream.Subjects {
				if patternCovers(p, value) {
					covered = true
					break
				}
			}
			if !covered {
				return r, fmt.Errorf("consumer %s filter %q not fully covered by stream %s", key, value, spec.Stream)
			}
			for _, prev := range filters {
				if patternsOverlap(prev, value) {
					return r, fmt.Errorf("consumer %s contains overlapping filters", key)
				}
			}
			filters = append(filters, value)
		}
		settings := mergeConsumer(ConsumerSettings{Mode: "pull", AckPolicy: "explicit", DeliverPolicy: "all"}, j.Defaults.Consumer)
		settings = mergeConsumer(settings, spec.ConsumerSettings)
		if settings.Mode != "pull" || settings.AckPolicy != "explicit" {
			return r, fmt.Errorf("consumer %s requires pull + explicit ack for shared workers", key)
		}
		if settings.DeliverPolicy != "all" && settings.DeliverPolicy != "new" {
			return r, fmt.Errorf("consumer %s unsupported deliver_policy", key)
		}
		delivery := nats.DeliverAllPolicy
		if settings.DeliverPolicy == "new" {
			delivery = nats.DeliverNewPolicy
		}
		ackWait, e := duration(settings.AckWait, 30*time.Second)
		if e != nil {
			return r, e
		}
		if len(settings.Backoff) > 0 && settings.AckWait != "" {
			return r, fmt.Errorf("consumer %s: backoff overrides ack_wait; configure only one", key)
		}
		maxDeliver := intValue(settings.MaxDeliver, 12)
		maxPending := intValue(settings.MaxAckPending, 128)
		if maxDeliver < 1 || maxPending < 1 || len(settings.Backoff) > maxDeliver {
			return r, fmt.Errorf("consumer %s invalid delivery limits", key)
		}
		backoff := make([]time.Duration, 0, len(settings.Backoff))
		for _, d := range settings.Backoff {
			v, e := duration(d, 0)
			if e != nil {
				return r, fmt.Errorf("consumer %s backoff: %w", key, e)
			}
			backoff = append(backoff, v)
		}
		if len(backoff) > 0 {
			ackWait = backoff[0]
		}
		worker := mergeWorker(WorkerSettings{}, j.Defaults.Worker)
		worker = mergeWorker(worker, spec.Worker)
		ackSync, e := duration(worker.AckSyncTimeout, 3*time.Second)
		if e != nil {
			return r, e
		}
		inProgress, e := duration(worker.InProgressInterval, 0)
		if e != nil {
			return r, e
		}
		concurrency := intValue(worker.MaxConcurrency, 8)
		batch := intValue(worker.FetchBatch, 8)
		if concurrency <= 0 || batch <= 0 || batch > concurrency || concurrency > maxPending {
			return r, fmt.Errorf("consumer %s requires 0 < fetch_batch <= max_concurrency <= max_ack_pending", key)
		}
		if inProgress > 0 && inProgress >= ackWait {
			return r, fmt.Errorf("consumer %s in_progress_interval must be shorter than ack deadline", key)
		}
		physical := r.Namespace + "_" + key
		if spec.Durable != "" {
			physical = spec.Durable
		}
		if !resourceName.MatchString(physical) {
			return r, fmt.Errorf("consumer %s has invalid durable name %q", key, physical)
		}
		identity := stream.Name + "/" + physical
		if previous, exists := physicalConsumers[identity]; exists {
			return r, fmt.Errorf("consumers %s and %s share durable %q", previous, key, identity)
		}
		physicalConsumers[identity] = key
		cc := nats.ConsumerConfig{Durable: physical, AckPolicy: nats.AckExplicitPolicy, DeliverPolicy: delivery, AckWait: ackWait, MaxDeliver: maxDeliver, MaxAckPending: maxPending, BackOff: backoff, ReplayPolicy: nats.ReplayInstantPolicy}
		if len(filters) == 1 {
			cc.FilterSubject = filters[0]
		} else {
			cc.FilterSubjects = filters
		}
		r.Consumers[key] = ResolvedConsumer{Stream: stream.Name, Config: cc, Worker: ResolvedWorker{FetchBatch: batch, MaxConcurrency: concurrency, AckSyncTimeout: ackSync, InProgressInterval: inProgress}}
	}
	// Workqueue only allows one non-overlapping consumer per message.
	for first, a := range r.Consumers {
		for second, b := range r.Consumers {
			if first >= second || a.Stream != b.Stream {
				continue
			}
			var parent *nats.StreamConfig
			for _, s := range r.Streams {
				if s.Name == a.Stream {
					sCopy := s
					parent = &sCopy
					break
				}
			}
			if parent == nil || parent.Retention != nats.WorkQueuePolicy {
				continue
			}
			for _, f := range consumerFilters(a.Config) {
				for _, g := range consumerFilters(b.Config) {
					if patternsOverlap(f, g) {
						return r, fmt.Errorf("workqueue %s has overlapping consumers %s and %s", a.Stream, first, second)
					}
				}
			}
		}
	}
	if q := j.Quarantine; q != nil {
		stream, ok := r.Streams[q.Stream]
		if !ok {
			return r, errors.New("quarantine stream unknown")
		}
		subject, ok := r.Subjects[q.Subject]
		if !ok || strings.ContainsAny(subject, "*>") {
			return r, errors.New("quarantine requires an exact subject alias")
		}
		if !slices.Contains(stream.Subjects, subject) || len(stream.Subjects) != 1 || stream.Retention != nats.LimitsPolicy {
			return r, errors.New("quarantine requires a dedicated LimitsPolicy stream")
		}
		enabled := make(map[string]bool, len(q.On))
		for _, kind := range q.On {
			switch kind {
			case "decode_error", "permanent_error", "max_deliver":
			default:
				return r, fmt.Errorf("unknown quarantine event %q", kind)
			}
			enabled[kind] = true
		}
		var interval time.Duration
		if q.Reconciliation != nil && q.Reconciliation.Enabled {
			return r, errors.New("automatic max-deliver reconciliation requires durable advisory capture and is not yet implemented; leave reconciliation disabled and use operator audit")
		}
		r.Quarantine = &ResolvedQuarantine{Stream: stream.Name, Subject: subject, On: enabled, ReconcileInterval: interval}
	}
	return r, nil
}

func consumerFilters(c nats.ConsumerConfig) []string {
	if len(c.FilterSubjects) > 0 {
		return c.FilterSubjects
	}
	return []string{c.FilterSubject}
}
