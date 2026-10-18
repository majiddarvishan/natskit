package natskit

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Client is an application-independent transport. It never creates resources
// during ordinary application startup. A caller owns the Client lifecycle.
type Client struct {
	Config         Config
	Topology       Resolved
	Reconciliation ReconcileReport
	Conn           *nats.Conn
	JS             nats.JetStreamContext
}

// Connect is read-only and preserves the existing verify-only contract.
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	return connect(ctx, cfg, false)
}

// ConnectReconciled is an explicit opt-in managed bootstrap. It creates missing
// resources and adds Stream subjects by default. With an explicitly configured
// subject_update_policy=replace it may replace Stream capture subjects without
// deleting stored messages, a Stream or a Durable. The caller owns external
// publisher/receiver coordination. Consumer filter and retention/policy drift
// always fail closed before changes.
func ConnectReconciled(ctx context.Context, cfg Config) (*Client, error) {
	return connect(ctx, cfg, true)
}

func connect(ctx context.Context, cfg Config, reconcile bool) (*Client, error) {
	topology, err := cfg.Resolve()
	if err != nil {
		return nil, err
	}
	options := []nats.Option{
		nats.Name("nats-" + topology.InstanceID),
		nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second), nats.Timeout(3 * time.Second),
	}
	if s := cfg.Connection.Security; s != nil {
		if s.CredsFile != "" {
			if _, err := os.Stat(s.CredsFile); err != nil {
				return nil, fmt.Errorf("NATS credentials unavailable: %w", err)
			}
			options = append(options, nats.UserCredentials(s.CredsFile))
		}
		if s.TLS != nil {
			t := &tls.Config{MinVersion: tls.VersionTLS12}
			if s.TLS.CAFile != "" {
				data, err := os.ReadFile(s.TLS.CAFile)
				if err != nil {
					return nil, fmt.Errorf("read NATS CA: %w", err)
				}
				roots := x509.NewCertPool()
				if !roots.AppendCertsFromPEM(data) {
					return nil, errors.New("NATS CA is not a valid PEM certificate")
				}
				t.RootCAs = roots
			}
			if s.TLS.CertFile != "" {
				identity, err := tls.LoadX509KeyPair(s.TLS.CertFile, s.TLS.KeyFile)
				if err != nil {
					return nil, fmt.Errorf("read NATS client identity: %w", err)
				}
				t.Certificates = []tls.Certificate{identity}
			}
			options = append(options, nats.Secure(t))
		}
	}
	nc, err := nats.Connect(strings.Join(cfg.Connection.URLs, ","), options...)
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}
	client := &Client{Config: cfg, Topology: topology, Conn: nc}
	if cfg.JetStream.Enabled {
		js, err := nc.JetStream()
		if err != nil {
			nc.Close()
			return nil, err
		}
		client.JS = js
		if reconcile {
			report, err := Reconcile(ctx, js, topology)
			if err != nil {
				nc.Close()
				return nil, fmt.Errorf("JetStream managed startup: %w", err)
			}
			client.Reconciliation = report
		} else if err := Verify(ctx, js, topology); err != nil {
			nc.Close()
			return nil, fmt.Errorf("JetStream topology verify failed (never auto-provisioned): %w", err)
		}
	}
	return client, nil
}

// Subject returns the wire subject for a registry alias.
func (c *Client) Subject(alias string) (string, error) {
	subject, ok := c.Topology.Subjects[alias]
	if !ok {
		return "", fmt.Errorf("unknown subject alias %q", alias)
	}
	if strings.ContainsAny(subject, "*>") {
		return "", fmt.Errorf("cannot publish to wildcard subject %q", alias)
	}
	return subject, nil
}

// Publish uses Core NATS by default and PubAck-confirmed JetStream if enabled.
// A stable messageID must be provided by the application for retrying durable
// commands. A PubAck confirms storage, not downstream business processing.
// PublishReceipt contains only confirmed broker metadata. Core NATS does
// not provide a storage acknowledgement and leaves JetStream false.
type PublishReceipt struct {
	JetStream bool
	Stream    string
	Sequence  uint64
	Duplicate bool
}

// Publish retains backward compatibility for application callers.
func (c *Client) Publish(ctx context.Context, alias string, payload []byte, messageID string) error {
	_, err := c.PublishConfirmed(ctx, alias, payload, messageID)
	return err
}

// PublishOptions controls optional broker-side publish assertions. It does not
// grant the publisher ownership of any Stream or Consumer.
type PublishOptions struct {
	// ExpectedStream, when set, must match the actual capturing JetStream Stream.
	// Leave empty to accept a genuine PubAck from any broker-owned capture.
	ExpectedStream string
}

// PublishConfirmed reports actual JetStream PubAck metadata, not execution
// success in any downstream application. The configured outbound subject does
// not need to belong to any Stream in the caller's managed topology.
func (c *Client) PublishConfirmed(ctx context.Context, alias string, payload []byte, messageID string) (PublishReceipt, error) {
	return c.PublishConfirmedWithOptions(ctx, alias, payload, messageID, PublishOptions{})
}

// PublishConfirmedWithOptions publishes to a configured concrete subject. An
// expected Stream is an assertion on broker capture, not a managed resource.
// Publishing never provisions or changes Streams/Consumers.
func (c *Client) PublishConfirmedWithOptions(ctx context.Context, alias string, payload []byte, messageID string, options PublishOptions) (PublishReceipt, error) {
	if err := ctx.Err(); err != nil {
		return PublishReceipt{}, err
	}
	subject, err := c.Subject(alias)
	if err != nil {
		return PublishReceipt{}, err
	}
	if options.ExpectedStream != "" && !resourceName.MatchString(options.ExpectedStream) {
		return PublishReceipt{}, fmt.Errorf("invalid expected JetStream Stream %q", options.ExpectedStream)
	}
	if c.JS == nil {
		if options.ExpectedStream != "" {
			return PublishReceipt{}, errors.New("expected JetStream Stream requires JetStream publishing")
		}
		if messageID != "" { /* Core NATS does not implement deduplication. */
		}
		if err := c.Conn.Publish(subject, payload); err != nil {
			return PublishReceipt{}, err
		}
		return PublishReceipt{}, c.Conn.FlushWithContext(ctx)
	}
	msg := &nats.Msg{Subject: subject, Data: payload}
	if messageID != "" {
		msg.Header = nats.Header{}
		msg.Header.Set(nats.MsgIdHdr, messageID)
	}
	publishOptions := []nats.PubOpt{nats.Context(ctx)}
	if options.ExpectedStream != "" {
		publishOptions = append(publishOptions, nats.ExpectStream(options.ExpectedStream))
	}
	ack, err := c.JS.PublishMsg(msg, publishOptions...)
	if err != nil {
		return PublishReceipt{}, fmt.Errorf("publish JetStream subject %q: %w", subject, err)
	}
	if ack == nil || ack.Stream == "" || ack.Sequence == 0 {
		return PublishReceipt{}, fmt.Errorf("JetStream broker did not return a valid PubAck for subject %q", subject)
	}
	if options.ExpectedStream != "" && ack.Stream != options.ExpectedStream {
		return PublishReceipt{}, fmt.Errorf("JetStream PubAck Stream %q does not match expected %q", ack.Stream, options.ExpectedStream)
	}
	return PublishReceipt{JetStream: true, Stream: ack.Stream, Sequence: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

func (c *Client) Close() error {
	if c.Conn == nil {
		return nil
	}
	timeout, err := duration(c.Config.Connection.DrainTimeout, 30*time.Second)
	if err != nil {
		c.Conn.Close()
		return err
	}
	done := make(chan error, 1)
	go func() { done <- c.Conn.Drain() }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		c.Conn.Close()
		return errors.New("NATS drain timeout exceeded; connection closed")
	}
}
