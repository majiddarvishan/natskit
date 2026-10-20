package natskit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
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

// WorkerOptions customizes delivery semantics without coupling the library to
// any application's error types, monitoring, or replay header conventions.
// The zero value preserves the original AckWait/BackOff behavior.
type WorkerOptions struct {
	// RetryDelay enables NAK-with-delay for retryable failures. Zero leaves
	// the message unacknowledged until its consumer AckWait/BackOff expires.
	RetryDelay time.Duration
	// BuildQuarantine may build an application-compatible immutable record.
	// It must preserve original bytes, target subject, and a stable Msg-Id.
	BuildQuarantine func(*nats.Msg, *nats.MsgMetadata, string) (*nats.Msg, error)
	// Observe is best-effort reporting and must not affect delivery decisions.
	Observe func(DeliveryEvent)
}

// DeliveryEvent contains only broker metadata; original payloads are omitted.
type DeliveryEvent struct {
	Action       string
	Subject      string
	PayloadBytes int
	Stream       string
	Sequence     uint64
	Deliveries   uint64
	Err          error
}

func (opts WorkerOptions) emit(action string, msg *nats.Msg, err error) {
	if opts.Observe == nil {
		return
	}
	e := DeliveryEvent{Action: action, Err: err}
	if msg != nil {
		e.Subject = msg.Subject
		e.PayloadBytes = len(msg.Data)
		if meta, metaErr := msg.Metadata(); metaErr == nil {
			e.Stream = meta.Stream
			e.Sequence = meta.Sequence.Stream
			e.Deliveries = meta.NumDelivered
		}
	}
	// An observability callback may not disrupt durable message handling.
	defer func() { _ = recover() }()
	opts.Observe(e)
}

type Worker struct {
	subscription *nats.Subscription
	done         chan error
	cancel       context.CancelFunc
	stopOnce     sync.Once
	stopped      chan struct{}
	stopErr      error
	exited       atomic.Bool
}

// StartWorker binds to the existing pull Durable; it never creates a Consumer.
// Multiple instances using the same namespace and consumer key share the cursor.
func (c *Client) StartWorker(ctx context.Context, key string, handler Handler) (*Worker, error) {
	return c.StartWorkerWithOptions(ctx, key, handler, WorkerOptions{})
}

// StartWorkerWithOptions binds the same existing durable, with explicit
// per-application retry/quarantine policy and delivery observability.
func (c *Client) StartWorkerWithOptions(ctx context.Context, key string, handler Handler, options WorkerOptions) (*Worker, error) {
	if options.RetryDelay < 0 {
		return nil, errors.New("retry delay must not be negative")
	}
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
	go func() {
		defer w.exited.Store(true)
		w.done <- c.runWorker(child, resolved, sub, handler, options)
	}()
	return w, nil
}

func (c *Client) runWorker(ctx context.Context, def ResolvedConsumer, sub *nats.Subscription, handler Handler, options WorkerOptions) error {
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
		// nats.go does not permit Context and MaxWait together. A bounded
		// child context keeps polling cancelable during graceful shutdown.
		fetchCtx, fetchCancel := context.WithTimeout(ctx, time.Second)
		msgs, err := sub.Fetch(batch, nats.Context(fetchCtx))
		fetchCancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if errors.Is(err, nats.ErrBadSubscription) ||
				(errors.Is(err, nats.ErrConnectionClosed) && c.Conn.IsClosed()) {
				return fmt.Errorf("consumer %s is permanently unavailable: %w", def.Config.Durable, err)
			}
			// Broker disconnects and short-lived JetStream fetch failures do
			// not terminate a durable worker. NATS reconnects its subscription;
			// topology/readiness checks independently fail closed during outage.
			options.emit("pull_error", nil, err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
				continue
			}
		}
		for _, message := range msgs {
			limiter <- struct{}{}
			active.Add(1)
			go func(msg *nats.Msg) {
				defer active.Done()
				defer func() { <-limiter }()
				c.processMessage(ctx, def, msg, handler, options)
			}(message)
		}
	}
	return nil
}

func (c *Client) processMessage(ctx context.Context, def ResolvedConsumer, msg *nats.Msg, handler Handler, options WorkerOptions) {
	options.emit("received", msg, nil)
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
					if err := msg.InProgress(); err != nil {
						options.emit("in_progress_error", msg, err)
					}
				}
			}
		}()
	}
	err := callHandler(child, msg, handler)
	close(stop)
	if err == nil {
		c.ackMessage(msg, def.Worker.AckSyncTimeout, options)
		return
	}
	options.emit("handler_error", msg, err)
	var classified *ProcessingError
	reason := ""
	if errors.As(err, &classified) && c.Topology.Quarantine != nil && c.Topology.Quarantine.On[classified.Kind] {
		reason = classified.Kind
	}
	if reason == "" && c.Topology.Quarantine != nil && c.Topology.Quarantine.On["max_deliver"] {
		if meta, metaErr := msg.Metadata(); metaErr == nil && meta.NumDelivered >= uint64(def.Config.MaxDeliver) {
			reason = "max_deliver"
		}
	}
	if reason != "" {
		if quarantineErr := c.quarantine(ctx, msg, reason, options); quarantineErr == nil {
			options.emit("quarantined", msg, nil)
			c.ackMessage(msg, def.Worker.AckSyncTimeout, options)
			return
		} else {
			options.emit("quarantine_error", msg, quarantineErr)
		}
	}
	if options.RetryDelay > 0 && ctx.Err() == nil {
		if nakErr := msg.NakWithDelay(options.RetryDelay); nakErr != nil {
			options.emit("nak_error", msg, nakErr)
		} else {
			options.emit("naked", msg, nil)
		}
	}
	// With no configured RetryDelay, let the broker AckWait/BackOff schedule
	// redelivery. At MaxDeliver with failed quarantine, preserve unacked origin
	// and report failure for operator reconciliation; never ACK data loss.
}

func (c *Client) ackMessage(msg *nats.Msg, timeout time.Duration, options WorkerOptions) {
	ackCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := msg.AckSync(nats.Context(ackCtx)); err != nil {
		options.emit("ack_error", msg, err)
		return
	}
	options.emit("acked", msg, nil)
}

// quarantine stores the original immutable bytes with stable origin identity.
// Application builders can supply replay-compatible headers, but may not
// modify the body, subject, or bypass the destination Stream PubAck.
func (c *Client) quarantine(ctx context.Context, msg *nats.Msg, reason string, options WorkerOptions) error {
	q := c.Topology.Quarantine
	if q == nil {
		return errors.New("quarantine is not configured")
	}
	metadata, err := msg.Metadata()
	if err != nil {
		return err
	}
	record := &nats.Msg{Subject: q.Subject, Data: append([]byte(nil), msg.Data...), Header: nats.Header{}}
	record.Header.Set("Natskit-Origin-Stream", metadata.Stream)
	record.Header.Set("Natskit-Origin-Sequence", strconv.FormatUint(metadata.Sequence.Stream, 10))
	record.Header.Set("Natskit-Origin-Subject", msg.Subject)
	record.Header.Set("Natskit-Reason", reason)
	record.Header.Set(nats.MsgIdHdr, "natskit-quarantine:"+metadata.Stream+":"+strconv.FormatUint(metadata.Sequence.Stream, 10))
	if options.BuildQuarantine != nil {
		record, err = buildQuarantineRecord(options.BuildQuarantine, msg, metadata, reason)
		if err != nil {
			return fmt.Errorf("build quarantine: %w", err)
		}
		if record == nil || record.Subject != q.Subject || !bytes.Equal(record.Data, msg.Data) || record.Header == nil || record.Header.Get(nats.MsgIdHdr) == "" {
			return errors.New("quarantine builder must preserve destination, original bytes, and stable message ID")
		}
	}
	ack, err := c.JS.PublishMsg(record, nats.Context(ctx))
	if err != nil {
		return err
	}
	if ack == nil || ack.Stream != q.Stream {
		return errors.New("quarantine PubAck did not confirm destination Stream")
	}
	return nil
}

// buildQuarantineRecord keeps a misbehaving application hook from crashing
// the worker; source delivery is never ACKed if record creation fails.
func buildQuarantineRecord(builder func(*nats.Msg, *nats.MsgMetadata, string) (*nats.Msg, error), msg *nats.Msg, meta *nats.MsgMetadata, reason string) (record *nats.Msg, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			record = nil
			err = fmt.Errorf("quarantine builder panic: %v", recovered)
		}
	}()
	return builder(msg, meta, reason)
}

// Ready indicates that the worker is still fetching on an active subscription.
// Applications must also check broker topology and database readiness.
func (w *Worker) Ready() bool {
	return w != nil && !w.exited.Load() && w.subscription != nil && w.subscription.IsValid()
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
