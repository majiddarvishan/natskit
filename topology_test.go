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
