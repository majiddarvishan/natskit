package natskit

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func sample(t *testing.T) Config {
	t.Helper()
	t.Setenv("NATS_INSTANCE_ID", "replica-01")
	raw, err := os.ReadFile("config.example.json")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestExampleResolvesWithIndependentResourceNames(t *testing.T) {
	c := sample(t)
	r, err := c.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if r.InstanceID != "replica-01" || r.Namespace != "SERVICE_A" {
		t.Fatalf("identity mismatch: %+v", r)
	}
	if len(r.Streams) != 3 || len(r.Consumers) != 2 {
		t.Fatalf("resource count: streams=%d consumers=%d", len(r.Streams), len(r.Consumers))
	}
	stream := r.Streams["RESULTS"]
	if stream.Name != "SERVICE_A_RESULTS" || len(stream.Subjects) != 2 || stream.Subjects[0] != "v1.results" {
		t.Fatalf("results: %+v", stream)
	}
	worker := r.Consumers["CANCEL_RESULTS_WORKER"]
	if worker.Stream != stream.Name || worker.Config.Durable != "SERVICE_A_CANCEL_RESULTS_WORKER" || worker.Config.FilterSubject != "v1.cancel.results" {
		t.Fatalf("consumer: %+v", worker)
	}
	if worker.Worker.MaxConcurrency != 2 || worker.Worker.FetchBatch != 2 {
		t.Fatalf("worker override not merged: %+v", worker.Worker)
	}
	normal := r.Consumers["RESULTS_WORKER"]
	if normal.Worker.MaxConcurrency != 8 || normal.Worker.FetchBatch != 8 {
		t.Fatalf("worker defaults: %+v", normal.Worker)
	}
	if normal.Config.AckWait != 30*time.Second || len(normal.Config.BackOff) != 5 {
		t.Fatalf("backoff: %+v", normal.Config)
	}
	if r.Streams["QUARANTINE"].MaxMsgSize != 16384 || r.Streams["RESULTS"].MaxMsgSize != 4096 {
		t.Fatal("profile/stream overrides failed")
	}
	if r.Streams["COMMANDS"].Retention != nats.WorkQueuePolicy || r.Streams["RESULTS"].Retention != nats.LimitsPolicy {
		t.Fatal("retention profiles invalid")
	}
}

func TestReplicasShareDurablesButNotInstanceNames(t *testing.T) {
	c := sample(t)
	first, e := c.Resolve()
	if e != nil {
		t.Fatal(e)
	}
	t.Setenv("NATS_INSTANCE_ID", "replica-02")
	second, e := c.Resolve()
	if e != nil {
		t.Fatal(e)
	}
	if first.InstanceID == second.InstanceID {
		t.Fatal("instances should differ")
	}
	for key, a := range first.Streams {
		if second.Streams[key].Name != a.Name {
			t.Fatalf("stream %s changed per instance", key)
		}
	}
	for key, a := range first.Consumers {
		if second.Consumers[key].Config.Durable != a.Config.Durable {
			t.Fatalf("durable %s changed per instance", key)
		}
	}
}

func TestOptionalSecurityAndStrictTLS(t *testing.T) {
	c := sample(t)
	if _, err := c.Resolve(); err != nil {
		t.Fatal(err)
	}
	c.Connection.Security = &Security{TLS: &TLS{CertFile: "client.pem"}}
	if _, err := c.Resolve(); err == nil {
		t.Fatal("accepted certificate without key")
	}
	c.Connection.Security = &Security{CredsFile: "service.creds"}
	if _, err := c.Resolve(); err != nil {
		t.Fatal(err)
	}
}

func TestStrictTopologyValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"missing alias", func(c *Config) {
			s := c.JetStream.Streams["RESULTS"]
			s.Subjects = []string{"reslts"}
			c.JetStream.Streams["RESULTS"] = s
		}},
		{"overlapping streams", func(c *Config) { c.Subjects["commands"] = "v1.>" }},
		{"missing profile", func(c *Config) {
			s := c.JetStream.Streams["COMMANDS"]
			s.Profile = "missing"
			c.JetStream.Streams["COMMANDS"] = s
		}},
		{"wrong filter stream", func(c *Config) {
			s := c.JetStream.Consumers["RESULTS_WORKER"]
			s.Stream = "COMMANDS"
			c.JetStream.Consumers["RESULTS_WORKER"] = s
		}},
		{"overlapping work queue consumers", func(c *Config) {
			c.JetStream.Consumers["A"] = ConsumerSpec{Stream: "COMMANDS", Filter: []string{"commands"}}
			c.JetStream.Consumers["B"] = ConsumerSpec{Stream: "COMMANDS", Filter: []string{"commands"}}
		}},
		{"backoff and ackwait", func(c *Config) {
			s := c.JetStream.Consumers["RESULTS_WORKER"]
			s.AckWait = "1m"
			c.JetStream.Consumers["RESULTS_WORKER"] = s
		}},
		{"unsupported push", func(c *Config) {
			s := c.JetStream.Consumers["RESULTS_WORKER"]
			s.Mode = "push"
			c.JetStream.Consumers["RESULTS_WORKER"] = s
		}},
		{"unsafe reconciliation", func(c *Config) { c.JetStream.Quarantine.Reconciliation.Enabled = true }},
		{"unknown quarantine kind", func(c *Config) { c.JetStream.Quarantine.On = []string{"oops"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := sample(t)
			tt.mutate(&c)
			if _, e := c.Resolve(); e == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	c := sample(t)
	raw, e := json.Marshal(c)
	if e != nil {
		t.Fatal(e)
	}
	invalid := strings.Replace(string(raw), `"instance_id":`, `"bogus":4,"instance_id":`, 1)
	if _, err := Decode([]byte(invalid)); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestPatternsAndProfiles(t *testing.T) {
	for _, tt := range []struct {
		outer, inner string
		want         bool
	}{
		{"v1.>", "v1.results", true}, {"v1.*", "v1.cancel.results", false},
		{"v1.*", "v1.results", true}, {"v1.foo", "v1.*", false},
		{"v1.>", "v1.>", true}, {"v1.foo.>", "v1.>", false},
	} {
		if got := patternCovers(tt.outer, tt.inner); got != tt.want {
			t.Errorf("cover(%s,%s)=%v", tt.outer, tt.inner, got)
		}
	}
}

func TestExplicitPhysicalNamesPreserveExistingBrokerResources(t *testing.T) {
	cfg := sample(t)
	stream := cfg.JetStream.Streams["RESULTS"]
	stream.Name = "LEGACY_RESULTS"
	cfg.JetStream.Streams["RESULTS"] = stream
	consumer := cfg.JetStream.Consumers["RESULTS_WORKER"]
	consumer.Durable = "EXISTING_RESULTS_DURABLE"
	cfg.JetStream.Consumers["RESULTS_WORKER"] = consumer
	first, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if first.Streams["RESULTS"].Name != stream.Name ||
		first.Consumers["RESULTS_WORKER"].Stream != stream.Name ||
		first.Consumers["RESULTS_WORKER"].Config.Durable != consumer.Durable {
		t.Fatalf("explicit broker identities were renamed: %+v", first)
	}
	t.Setenv("NATS_INSTANCE_ID", "replica-99")
	second, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if second.Consumers["RESULTS_WORKER"].Config.Durable != consumer.Durable ||
		second.Streams["RESULTS"].Name != stream.Name {
		t.Fatal("broker identities changed across replica IDs")
	}
}

func TestExplicitPhysicalNamesRejectCollisionsAndInvalidNames(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"stream collision", func(c *Config) {
			a := c.JetStream.Streams["RESULTS"]
			b := c.JetStream.Streams["COMMANDS"]
			a.Name, b.Name = "DUPLICATE", "DUPLICATE"
			c.JetStream.Streams["RESULTS"], c.JetStream.Streams["COMMANDS"] = a, b
		}},
		{"invalid stream name", func(c *Config) {
			a := c.JetStream.Streams["RESULTS"]
			a.Name = "contains.dot"
			c.JetStream.Streams["RESULTS"] = a
		}},
		{"invalid durable name", func(c *Config) {
			a := c.JetStream.Consumers["RESULTS_WORKER"]
			a.Durable = "illegal.durable"
			c.JetStream.Consumers["RESULTS_WORKER"] = a
		}},
		{"durable collision", func(c *Config) {
			a := c.JetStream.Consumers["RESULTS_WORKER"]
			b := c.JetStream.Consumers["CANCEL_RESULTS_WORKER"]
			a.Durable, b.Durable = "SAME", "SAME"
			b.Stream = a.Stream
			c.JetStream.Consumers["RESULTS_WORKER"] = a
			c.JetStream.Consumers["CANCEL_RESULTS_WORKER"] = b
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := sample(t)
			tc.change(&c)
			if _, err := c.Resolve(); err == nil {
				t.Fatal("unsafe physical resource naming accepted")
			}
		})
	}
}

func TestVerifyDetectsUnsafeUntrackedStreamAndConsumerDrift(t *testing.T) {
	cfg := sample(t)
	resolved, err := cfg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	stream := resolved.Streams["RESULTS"]
	changedStream := stream
	changedStream.NoAck = true
	if sameStream(changedStream, stream) {
		t.Fatal("stream NoAck drift must be rejected")
	}
	changedStream = stream
	changedStream.MaxMsgs = 1
	if sameStream(changedStream, stream) {
		t.Fatal("stream MaxMsgs drift must be rejected")
	}
	changedStream = stream
	changedStream.MaxMsgsPerSubject = 1
	if sameStream(changedStream, stream) {
		t.Fatal("stream MaxMsgsPerSubject drift must be rejected")
	}
	changedStream = stream
	changedStream.SubjectTransform = &nats.SubjectTransformConfig{Source: "v1.*", Destination: "other.*"}
	if sameStream(changedStream, stream) {
		t.Fatal("stream subject transform drift must be rejected")
	}
	changedStream = stream
	changedStream.DiscardNewPerSubject = true
	if sameStream(changedStream, stream) {
		t.Fatal("per-subject discard drift must be rejected")
	}
	consumer := resolved.Consumers["RESULTS_WORKER"].Config
	changedConsumer := consumer
	changedConsumer.ReplayPolicy = nats.ReplayOriginalPolicy
	if sameConsumer(changedConsumer, consumer) {
		t.Fatal("consumer replay policy drift must be rejected")
	}
	changedConsumer = consumer
	changedConsumer.DeliverSubject = "unexpected.push.subject"
	if sameConsumer(changedConsumer, consumer) {
		t.Fatal("consumer unexpectedly switched to push delivery")
	}
	changedConsumer = consumer
	changedConsumer.InactiveThreshold = time.Hour
	if sameConsumer(changedConsumer, consumer) {
		t.Fatal("durable inactivity expiration drift must be rejected")
	}
	changedConsumer = consumer
	changedConsumer.HeadersOnly = true
	if sameConsumer(changedConsumer, consumer) {
		t.Fatal("headers-only consumer must not silently drop payload")
	}
}
