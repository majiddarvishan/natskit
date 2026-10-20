// Package natskit provides application-agnostic NATS transport and JetStream
// topology, provisioning, and shared durable workers. It has no dependency on
// application databases, protobuf contracts, or logging implementations.
package natskit

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// Config is the contents of a JSON "nats" object, not the full application file.
type Config struct {
	Connection Connection        `json:"connection"`
	InstanceID string            `json:"instance_id"`
	Subjects   map[string]string `json:"subjects"`
	JetStream  JetStreamConfig   `json:"jetstream"`
}

type Connection struct {
	URLs         []string  `json:"urls"`
	Security     *Security `json:"security,omitempty"`
	DrainTimeout string    `json:"drain_timeout,omitempty"`
}

// Optional; a missing Security value means no explicit credentials or TLS.
type Security struct {
	CredsFile string `json:"creds_file,omitempty"`
	TLS       *TLS   `json:"tls,omitempty"`
}

type TLS struct {
	CAFile   string `json:"ca_file,omitempty"`
	CertFile string `json:"cert_file,omitempty"`
	KeyFile  string `json:"key_file,omitempty"`
}

type JetStreamConfig struct {
	Enabled             bool                      `json:"enabled"`
	Namespace           string                    `json:"namespace,omitempty"`
	Provisioning        string                    `json:"provisioning,omitempty"`          // verify (runtime) or operator_only
	SubjectUpdatePolicy string                    `json:"subject_update_policy,omitempty"` // additive (default) or replace
	Profiles            map[string]StreamSettings `json:"profiles,omitempty"`
	Streams             map[string]StreamSpec     `json:"streams,omitempty"`
	Defaults            Defaults                  `json:"defaults,omitempty"`
	Consumers           map[string]ConsumerSpec   `json:"consumers,omitempty"`
	Quarantine          *QuarantineSpec           `json:"quarantine,omitempty"`
}

type Defaults struct {
	Stream   StreamSettings   `json:"stream,omitempty"`
	Consumer ConsumerSettings `json:"consumer,omitempty"`
	Worker   WorkerSettings   `json:"worker,omitempty"`
}

type StreamSettings struct {
	Storage         string `json:"storage,omitempty"`
	Retention       string `json:"retention,omitempty"`
	Discard         string `json:"discard,omitempty"`
	MaxAge          string `json:"max_age,omitempty"`
	DuplicateWindow string `json:"duplicate_window,omitempty"`
	MaxBytes        *int64 `json:"max_bytes,omitempty"`
	MaxMsgSize      *int32 `json:"max_msg_size,omitempty"`
	Replicas        *int   `json:"replicas,omitempty"`
}

type StreamSpec struct {
	Name     string   `json:"name,omitempty"` // optional exact physical name of an existing Stream
	Profile  string   `json:"profile,omitempty"`
	Subjects []string `json:"subjects"`
	StreamSettings
}

type ConsumerSettings struct {
	Mode          string   `json:"mode,omitempty"`
	AckPolicy     string   `json:"ack_policy,omitempty"`
	DeliverPolicy string   `json:"deliver_policy,omitempty"`
	AckWait       string   `json:"ack_wait,omitempty"`
	MaxDeliver    *int     `json:"max_deliver,omitempty"`
	MaxAckPending *int     `json:"max_ack_pending,omitempty"`
	Backoff       []string `json:"backoff,omitempty"`
}

type ConsumerSpec struct {
	Durable        string   `json:"durable,omitempty"` // optional exact existing Durable name
	Stream         string   `json:"stream"`
	ExternalStream bool     `json:"external_stream,omitempty"` // attach to an existing stream without managing it
	Filter         []string `json:"filter"`
	ConsumerSettings
	Worker WorkerSettings `json:"worker,omitempty"`
}

type WorkerSettings struct {
	FetchBatch         *int   `json:"fetch_batch,omitempty"`
	MaxConcurrency     *int   `json:"max_concurrency,omitempty"`
	AckSyncTimeout     string `json:"ack_sync_timeout,omitempty"`
	InProgressInterval string `json:"in_progress_interval,omitempty"`
}

type QuarantineSpec struct {
	Subject        string          `json:"subject"`
	Stream         string          `json:"stream"`
	On             []string        `json:"on,omitempty"`
	Reconciliation *Reconciliation `json:"reconciliation,omitempty"`
}

type Reconciliation struct {
	Enabled  bool   `json:"enabled"`
	Interval string `json:"interval"`
}

func Decode(data []byte) (Config, error) {
	var cfg Config
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode nats configuration: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Config{}, errors.New("trailing content after NATS configuration")
	}
	return cfg, nil
}

// expandEnv resolves ${VARIABLE} references without silently accepting unset or
// empty instance IDs. Use environment-backed secrets/files, not inlined secrets.
func expandEnv(value string) (string, error) {
	var missing string
	out := os.Expand(value, func(key string) string {
		v := os.Getenv(key)
		if v == "" && missing == "" {
			missing = key
		}
		return v
	})
	if missing != "" {
		return "", fmt.Errorf("missing environment variable %q", missing)
	}
	return out, nil
}

func mergeStream(a, b StreamSettings) StreamSettings {
	if b.Storage != "" {
		a.Storage = b.Storage
	}
	if b.Retention != "" {
		a.Retention = b.Retention
	}
	if b.Discard != "" {
		a.Discard = b.Discard
	}
	if b.MaxAge != "" {
		a.MaxAge = b.MaxAge
	}
	if b.DuplicateWindow != "" {
		a.DuplicateWindow = b.DuplicateWindow
	}
	if b.MaxBytes != nil {
		a.MaxBytes = b.MaxBytes
	}
	if b.MaxMsgSize != nil {
		a.MaxMsgSize = b.MaxMsgSize
	}
	if b.Replicas != nil {
		a.Replicas = b.Replicas
	}
	return a
}

func mergeConsumer(a, b ConsumerSettings) ConsumerSettings {
	if b.Mode != "" {
		a.Mode = b.Mode
	}
	if b.AckPolicy != "" {
		a.AckPolicy = b.AckPolicy
	}
	if b.DeliverPolicy != "" {
		a.DeliverPolicy = b.DeliverPolicy
	}
	if b.AckWait != "" {
		a.AckWait = b.AckWait
	}
	if b.MaxDeliver != nil {
		a.MaxDeliver = b.MaxDeliver
	}
	if b.MaxAckPending != nil {
		a.MaxAckPending = b.MaxAckPending
	}
	if b.Backoff != nil {
		a.Backoff = b.Backoff
	}
	return a
}

func mergeWorker(a, b WorkerSettings) WorkerSettings {
	if b.FetchBatch != nil {
		a.FetchBatch = b.FetchBatch
	}
	if b.MaxConcurrency != nil {
		a.MaxConcurrency = b.MaxConcurrency
	}
	if b.AckSyncTimeout != "" {
		a.AckSyncTimeout = b.AckSyncTimeout
	}
	if b.InProgressInterval != "" {
		a.InProgressInterval = b.InProgressInterval
	}
	return a
}
