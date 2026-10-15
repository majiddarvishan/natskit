package natskit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
)

// Advisory describes an exhausted consumer delivery. No automatic removal or
// ACK of the original message occurs; the application/operator must reconcile
// the stream state independently after examining this notification.
type Advisory struct {
	Stream    string `json:"stream"`
	Consumer  string `json:"consumer"`
	StreamSeq uint64 `json:"stream_seq"`
}

// WatchMaxDeliver subscribes to existing consumer advisories for diagnostics.
// This is best-effort notification, not lossless durable reconciliation.
// All replicas may receive the same advisory; callers must deduplicate it.
func (c *Client) WatchMaxDeliver(ctx context.Context, notify func(Advisory)) (func(), error) {
	if c.JS == nil {
		return nil, errors.New("JetStream is disabled")
	}
	if notify == nil {
		return nil, errors.New("notification handler required")
	}
	var subscriptions []*nats.Subscription
	for _, key := range sortedConsumerKeys(c.Topology.Consumers) {
		consumer := c.Topology.Consumers[key]
		topic := "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES." + consumer.Stream + "." + consumer.Config.Durable
		sub, err := c.Conn.Subscribe(topic, func(m *nats.Msg) {
			if len(m.Data) == 0 || len(m.Data) > 4096 {
				return
			}
			var a Advisory
			if json.Unmarshal(m.Data, &a) != nil || a.StreamSeq == 0 || a.Stream != consumer.Stream || a.Consumer != consumer.Config.Durable {
				return
			}
			notify(a)
		})
		if err != nil {
			for _, s := range subscriptions {
				_ = s.Unsubscribe()
			}
			return nil, fmt.Errorf("subscribe max deliver advisory: %w", err)
		}
		subscriptions = append(subscriptions, sub)
	}
	stop := func() {
		for _, s := range subscriptions {
			_ = s.Unsubscribe()
		}
	}
	go func() { <-ctx.Done(); stop() }()
	return stop, nil
}

// CopyExhaustedToQuarantine creates a quarantined copy after receiving a
// trusted advisory; it does not delete or ACK the exhausted source message.
// Operators must reconcile original retention/position separately. This is
// not a replacement for durable advisory capture or scheduled reconciliation.
func (c *Client) CopyExhaustedToQuarantine(ctx context.Context, a Advisory) error {
	q := c.Topology.Quarantine
	if q == nil || !q.On["max_deliver"] {
		return errors.New("max_deliver quarantine is disabled")
	}
	authorized := false
	for _, v := range c.Topology.Consumers {
		if v.Stream == a.Stream && v.Config.Durable == a.Consumer {
			authorized = true
			break
		}
	}
	if !authorized || a.StreamSeq == 0 {
		return errors.New("unknown max_delivery origin")
	}
	original, err := c.JS.GetMsg(a.Stream, a.StreamSeq, nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("fetch exhausted message: %w", err)
	}
	record := &nats.Msg{Subject: q.Subject, Data: original.Data, Header: nats.Header{}}
	record.Header.Set("Natskit-Origin-Stream", a.Stream)
	record.Header.Set("Natskit-Origin-Sequence", strconv.FormatUint(a.StreamSeq, 10))
	record.Header.Set("Natskit-Origin-Subject", original.Subject)
	record.Header.Set("Natskit-Reason", "max_deliver")
	record.Header.Set(nats.MsgIdHdr, "natskit-quarantine:"+a.Stream+":"+strconv.FormatUint(a.StreamSeq, 10))
	ack, err := c.JS.PublishMsg(record, nats.Context(ctx))
	if err != nil {
		return err
	}
	if ack == nil || ack.Stream != q.Stream {
		return errors.New("quarantine PubAck did not confirm destination")
	}
	return nil
}
