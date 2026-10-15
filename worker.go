package natskit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Handler processes a message but never owns the acknowledgment. Applications
// must make side effects idempotent, e.g. commit a DB transaction before return.
type Handler func(context.Context, *nats.Msg) error

// ProcessingError separates retryable failures from decoded/permanent errors.
// Ordinary untyped errors are always retryable and never ACKed.
type ProcessingError struct {
	Kind string
	Err  error
}

func (e *ProcessingError) Error() string { return e.Kind + ": " + e.Err.Error() }
func (e *ProcessingError) Unwrap() error { return e.Err }
func DecodeError(err error) error        { return &ProcessingError{Kind: "decode_error", Err: err} }
func PermanentError(err error) error     { return &ProcessingError{Kind: "permanent_error", Err: err} }

type Worker struct {
	subscription *nats.Subscription
	done         chan error
	cancel       context.CancelFunc
	stopOnce     sync.Once
	stopped      chan struct{}
	stopErr      error
}

// StartWorker binds to the existing pull Durable; it never creates a Consumer.
// Multiple instances using the same namespace and consumer key share the cursor.
func (c *Client) StartWorker(ctx context.Context, key string, handler Handler) (*Worker, error) {
	if c.JS == nil {
		return nil, errors.New("JetStream is disabled")
	}
	if handler == nil {
		return nil, errors.New("handler is required")
	}
	resolved, ok := c.Topology.Consumers[key]
	if !ok {
		return nil, fmt.Errorf("unknown consumer %q", key)
	}
	// nats.go verifies a single-filter Durable against the subscription
	// subject. Empty is only valid when the Durable uses FilterSubjects.
	subject := resolved.Config.FilterSubject
	sub, err := c.JS.PullSubscribe(subject, resolved.Config.Durable, nats.Bind(resolved.Stream, resolved.Config.Durable))
	if err != nil {
		return nil, fmt.Errorf("bind durable %s: %w", resolved.Config.Durable, err)
	}
	child, cancel := context.WithCancel(ctx)
	w := &Worker{subscription: sub, done: make(chan error, 1), cancel: cancel, stopped: make(chan struct{})}
	go func() { w.done <- c.runWorker(child, resolved, sub, handler) }()
	return w, nil
}

func (c *Client) runWorker(ctx context.Context, def ResolvedConsumer, sub *nats.Subscription, handler Handler) error {
	defer sub.Unsubscribe()
	limiter := make(chan struct{}, def.Worker.MaxConcurrency)
	var active sync.WaitGroup
	defer active.Wait()
	for ctx.Err() == nil {
		// Do not fetch more messages than this instance can process. The
		// durable's broker-side max_ack_pending is shared by all instances.
		available := cap(limiter) - len(limiter)
		if available <= 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(20 * time.Millisecond):
				continue
			}
		}
		batch := def.Worker.FetchBatch
		if available < batch {
			batch = available
		}
		msgs, err := sub.Fetch(batch, nats.Context(ctx), nats.MaxWait(time.Second))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			return fmt.Errorf("fetch %s: %w", def.Config.Durable, err)
		}
		for _, message := range msgs {
			limiter <- struct{}{}
			active.Add(1)
			go func(msg *nats.Msg) {
				defer active.Done()
				defer func() { <-limiter }()
				c.processMessage(ctx, def, msg, handler)
			}(message)
		}
	}
	return nil
}

func (c *Client) processMessage(ctx context.Context, def ResolvedConsumer, msg *nats.Msg, handler Handler) {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan struct{})
	if def.Worker.InProgressInterval > 0 {
		go func() {
			ticker := time.NewTicker(def.Worker.InProgressInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-child.Done():
					return
				case <-ticker.C:
					_ = msg.InProgress()
				}
			}
		}()
	}
	err := callHandler(child, msg, handler)
	close(stop)
	// The application determines when its own transaction is committed; only
	// a nil handler result permits the library to acknowledge delivery.
	if err == nil {
		ackCtx, ackCancel := context.WithTimeout(context.Background(), def.Worker.AckSyncTimeout)
		defer ackCancel()
		_ = msg.AckSync(nats.Context(ackCtx)) // unconfirmed ACK is naturally redelivered; handler must be idempotent
		return
	}
	var classified *ProcessingError
	if errors.As(err, &classified) && c.Topology.Quarantine != nil && c.Topology.Quarantine.On[classified.Kind] {
		if c.quarantine(ctx, msg, classified.Kind) == nil {
			ackCtx, ackCancel := context.WithTimeout(context.Background(), def.Worker.AckSyncTimeout)
			defer ackCancel()
			_ = msg.AckSync(nats.Context(ackCtx))
			return
		}
	}
	// Do not ACK or NAK a retryable failure. Let the broker's configured
	// ACK deadline / BackOff schedule control redelivery across replicas.
	// NAK would bypass the configured BackOff schedule.
}

// quarantine stores the original immutable message bytes with stable origin
// identity. PubAck must confirm the configured dead-letter Stream before ACK.
func (c *Client) quarantine(ctx context.Context, msg *nats.Msg, reason string) error {
	q := c.Topology.Quarantine
	if q == nil {
		return errors.New("quarantine is not configured")
	}
	metadata, err := msg.Metadata()
	if err != nil {
		return err
	}
	record := &nats.Msg{Subject: q.Subject, Data: msg.Data, Header: nats.Header{}}
	record.Header.Set("Natskit-Origin-Stream", metadata.Stream)
	record.Header.Set("Natskit-Origin-Sequence", strconv.FormatUint(metadata.Sequence.Stream, 10))
	record.Header.Set("Natskit-Origin-Subject", msg.Subject)
	record.Header.Set("Natskit-Reason", reason)
	record.Header.Set(nats.MsgIdHdr, "natskit-quarantine:"+metadata.Stream+":"+strconv.FormatUint(metadata.Sequence.Stream, 10))
	ack, err := c.JS.PublishMsg(record, nats.Context(ctx))
	if err != nil {
		return err
	}
	if ack == nil || ack.Stream != q.Stream {
		return errors.New("quarantine PubAck did not confirm destination Stream")
	}
	return nil
}

// Stop cancels the fetch loop and waits for in-flight handlers before returning.
func (w *Worker) Stop() error {
	w.stopOnce.Do(func() {
		w.cancel()
		w.stopErr = <-w.done
		close(w.stopped)
	})
	<-w.stopped
	return w.stopErr
}

// A bad application handler must not crash the entire worker process.
func callHandler(ctx context.Context, msg *nats.Msg, handler Handler) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	return handler(ctx, msg)
}
