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
	JS       nats.JetStreamContext
}

// Connect is read-only and preserves the existing verify-only contract.
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	return connect(ctx, cfg, false)
}

// ConnectReconciled is an explicit opt-in managed bootstrap. It creates missing
// resources and can add Stream subjects, but never removes a subject, deletes a
// resource, changes a Consumer filter, or modifies retention/ACK policies.
// Unsafe drift returns an actionable error before any mutation.
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

// PublishConfirmed reports actual JetStream PubAck metadata, not execution
// success in any downstream application.
func (c *Client) PublishConfirmed(ctx context.Context, alias string, payload []byte, messageID string) (PublishReceipt, error) {
	if err := ctx.Err(); err != nil {
		return PublishReceipt{}, err
	}
	subject, err := c.Subject(alias)
	if err != nil {
		return PublishReceipt{}, err
	}
	if c.JS == nil {
		if messageID != "" { /* Core NATS does not implement deduplication. */
		}
		if err := c.Conn.Publish(subject, payload); err != nil {
			return PublishReceipt{}, err
		}
		return PublishReceipt{}, c.Conn.FlushWithContext(ctx)
	}
	target := ""
	for _, stream := range c.Topology.Streams {
		for _, pattern := range stream.Subjects {
			if patternCovers(pattern, subject) {
				target = stream.Name
				break
			}
		}
	}
	if target == "" {
		return PublishReceipt{}, fmt.Errorf("subject %q not covered by configured Streams", alias)
	}
	msg := &nats.Msg{Subject: subject, Data: payload}
	if messageID != "" {
		msg.Header = nats.Header{}
		msg.Header.Set(nats.MsgIdHdr, messageID)
	}
	ack, err := c.JS.PublishMsg(msg, nats.Context(ctx))
	if err != nil {
		return PublishReceipt{}, fmt.Errorf("publish JetStream: %w", err)
	}
	if ack == nil || ack.Stream != target {
		return PublishReceipt{}, fmt.Errorf("PubAck did not confirm configured Stream %s", target)
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
