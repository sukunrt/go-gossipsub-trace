// Package gossipsubtrace measures GossipSub RPC traffic using pubsub.RawTracer.
// It exports Prometheus counters and logs bandwidth summaries periodically.
package gossipsubtrace

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/prometheus/client_golang/prometheus"
)

const DefaultLogInterval = 100 * time.Millisecond

// Config controls reporting. The zero value enables metrics in the default
// Prometheus registry and text logs on stderr every 100 ms.
type Config struct {
	// Registerer receives this tracer's collector. Use separate registries or
	// prometheus.WrapRegistererWith to distinguish multiple tracers.
	Registerer prometheus.Registerer
	// Logger receives Info-level summaries. Nil uses a text logger on stderr.
	Logger *slog.Logger
	// LogInterval defaults to DefaultLogInterval. For finer resolution, use 50 ms.
	LogInterval time.Duration
	// MessageIDFn must match GossipSub's message-ID function, including any
	// per-topic overrides. Nil uses pubsub.DefaultMsgIdFn. It is used to learn
	// topics from published messages; IHAVE learning needs no custom function.
	MessageIDFn pubsub.MsgIdFunction
}

type event int

const (
	recv event = iota
	send
	drop
	eventCount
)

var eventNames = [eventCount]string{"recv", "send", "drop"}

type counters struct {
	rpcs       uint64
	bytes      uint64
	kindBytes  [kindCount]uint64
	messages   [kindCount]uint64
	messageIDs [kindCount]uint64
}

type totals [eventCount]counters

// Tracer implements both pubsub.RawTracer and prometheus.Collector. Callbacks
// only size RPCs and update counters; logging and metric collection happen
// separately. They never modify RPCs or messages. Message ID/topic mappings
// expire after MessageIDRetention and are periodically removed.
//
// SendRPC observes queued sends, not completed stream writes. Byte counts are
// estimates of protobuf RPC bytes plus their unsigned-varint length prefix;
// they exclude transport, encryption, and stream multiplexing overhead.
type Tracer struct {
	mu      sync.Mutex
	totals  totals
	closed  bool
	topics  topicCache
	byTopic map[topicKey]topicCounters
	idFn    pubsub.MsgIdFunction

	logger    *slog.Logger
	interval  time.Duration
	stop      chan struct{}
	done      chan struct{}
	cacheDone chan struct{}
	once      sync.Once

	rpcBytes        *prometheus.Desc
	rpcs            *prometheus.Desc
	kindBytes       *prometheus.Desc
	messages        *prometheus.Desc
	messageIDs      *prometheus.Desc
	topicBytes      *prometheus.Desc
	topicMessageIDs *prometheus.Desc
}

var (
	_ pubsub.RawTracer     = (*Tracer)(nil)
	_ prometheus.Collector = (*Tracer)(nil)
)

// New registers the metrics and starts periodic logging. Registration errors
// (including duplicate registration) are returned before any goroutine starts.
// Install the result with pubsub.WithRawTracer and call Close on shutdown.
func New(cfg Config) (*Tracer, error) {
	if cfg.LogInterval < 0 {
		return nil, errors.New("gossipsubtrace: log interval must be positive")
	}
	if cfg.LogInterval == 0 {
		cfg.LogInterval = DefaultLogInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	if cfg.Registerer == nil {
		cfg.Registerer = prometheus.DefaultRegisterer
	}
	if cfg.MessageIDFn == nil {
		cfg.MessageIDFn = pubsub.DefaultMsgIdFn
	}
	t := &Tracer{
		logger: cfg.Logger, interval: cfg.LogInterval,
		stop: make(chan struct{}), done: make(chan struct{}),
		cacheDone: make(chan struct{}), topics: newTopicCache(),
		byTopic: make(map[topicKey]topicCounters), idFn: cfg.MessageIDFn,
		rpcBytes: prometheus.NewDesc("gossipsub_rpc_bytes_total",
			"Estimated encoded RPC bytes including length prefixes; send means queued and drop means unsent.", []string{"event"}, nil),
		rpcs: prometheus.NewDesc("gossipsub_rpcs_total",
			"RPCs observed by the tracer, including empty RPCs.", []string{"event"}, nil),
		kindBytes: prometheus.NewDesc("gossipsub_message_bytes_total",
			"Estimated RPC bytes by kind; all kinds sum to gossipsub_rpc_bytes_total.", []string{"event", "kind"}, nil),
		messages: prometheus.NewDesc("gossipsub_messages_total",
			"Messages or control entries observed, not individual advertised/requested IDs.", []string{"event", "kind"}, nil),
		messageIDs: prometheus.NewDesc("gossipsub_message_ids_total",
			"Message IDs carried in IHAVE, IWANT, and IDONTWANT entries, including repetitions.", []string{"event", "kind"}, nil),
		topicBytes: prometheus.NewDesc("gossipsub_topic_message_bytes_total",
			"Estimated RPC bytes by kind and topic; empty topic denotes unattributed or shared bytes.", []string{"event", "kind", "topic"}, nil),
		topicMessageIDs: prometheus.NewDesc("gossipsub_topic_message_ids_total",
			"IHAVE, IWANT, and IDONTWANT IDs by topic; empty topic denotes unknown or ambiguous IDs.", []string{"event", "kind", "topic"}, nil),
	}
	if err := cfg.Registerer.Register(t); err != nil {
		return nil, err
	}
	started := time.Now()
	go t.cleanMessageIDs()
	go t.run(started)
	return t, nil
}

// Close stops reporting and logs any remaining traffic. It is safe to call
// concurrently or repeatedly. Later tracer callbacks are ignored. Metrics stay
// registered with their final values; the owner may unregister the collector.
// Close waits for the logger to finish writing.
func (t *Tracer) Close() error {
	t.once.Do(func() {
		t.mu.Lock()
		t.closed = true
		t.topics = topicCache{}
		t.mu.Unlock()
		close(t.stop)
	})
	<-t.done
	<-t.cacheDone
	return nil
}

func (t *Tracer) record(e event, rpc *pubsub.RPC) {
	if rpc == nil {
		return
	}
	n := measureRPC(rpc)
	var published []idTopic
	if e != drop {
		published = t.publishedTopics(rpc)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	now := time.Now()
	if e != drop {
		t.learnTopics(rpc, published, now)
	}
	attributeRPC(rpc, n, func(id string) string { return t.topics.topic(id, now) }, func(k kind, topic string, bytes, ids uint64) {
		key := topicKey{event: e, kind: k, topic: topic}
		c := t.byTopic[key]
		c.bytes += bytes
		c.ids += ids
		t.byTopic[key] = c
	})
	c := &t.totals[e]
	c.rpcs++
	c.bytes += n.bytes
	for k := kind(0); k < kindCount; k++ {
		c.kindBytes[k] += n.kindBytes[k]
		c.messages[k] += n.messages[k]
		c.messageIDs[k] += n.messageIDs[k]
	}
}

func (t *Tracer) snapshot() totals {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.totals
}

func (t *Tracer) Describe(ch chan<- *prometheus.Desc) {
	ch <- t.rpcBytes
	ch <- t.rpcs
	ch <- t.kindBytes
	ch <- t.messages
	ch <- t.messageIDs
	ch <- t.topicBytes
	ch <- t.topicMessageIDs
}

func (t *Tracer) Collect(ch chan<- prometheus.Metric) {
	snapshot, topics := t.snapshotAll()
	for e, c := range snapshot {
		name := eventNames[e]
		ch <- prometheus.MustNewConstMetric(t.rpcBytes, prometheus.CounterValue, float64(c.bytes), name)
		ch <- prometheus.MustNewConstMetric(t.rpcs, prometheus.CounterValue, float64(c.rpcs), name)
		for k, nameOfKind := range kindNames {
			ch <- prometheus.MustNewConstMetric(t.kindBytes, prometheus.CounterValue, float64(c.kindBytes[k]), name, nameOfKind)
			if kind(k) < overhead {
				ch <- prometheus.MustNewConstMetric(t.messages, prometheus.CounterValue, float64(c.messages[k]), name, nameOfKind)
			}
			if kind(k) == ihave || kind(k) == iwant || kind(k) == idontwant {
				ch <- prometheus.MustNewConstMetric(t.messageIDs, prometheus.CounterValue, float64(c.messageIDs[k]), name, nameOfKind)
			}
		}
	}
	for key, c := range topics {
		ch <- prometheus.MustNewConstMetric(t.topicBytes, prometheus.CounterValue, float64(c.bytes), eventNames[key.event], kindNames[key.kind], key.topic)
		if key.kind == ihave || key.kind == iwant || key.kind == idontwant {
			ch <- prometheus.MustNewConstMetric(t.topicMessageIDs, prometheus.CounterValue, float64(c.ids), eventNames[key.event], kindNames[key.kind], key.topic)
		}
	}
}

func (t *Tracer) run(started time.Time) {
	defer close(t.done)
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	var previous totals
	var previousTopics map[topicKey]topicCounters
	last := started
	report := func(final bool) {
		current, topics := t.snapshotAll()
		now := time.Now()
		if !final || current != previous {
			t.logWindow(current, previous, now.Sub(last), final)
			t.logTopicWindows(topics, previousTopics, now.Sub(last), final)
		}
		previousTopics = topics
		previous, last = current, now
	}
	for {
		select {
		case <-ticker.C:
			report(false)
		case <-t.stop:
			report(true)
			return
		}
	}
}

func (t *Tracer) logWindow(current, previous totals, elapsed time.Duration, final bool) {
	attrs := []slog.Attr{slog.Float64("interval_ms", float64(elapsed)/float64(time.Millisecond))}
	if final {
		attrs = append(attrs, slog.Bool("final", true))
	}
	for e, c := range current {
		prefix := eventNames[e] + "_"
		bytes := c.bytes - previous[e].bytes
		attrs = append(attrs,
			slog.Uint64(prefix+"bytes", bytes),
			slog.Uint64(prefix+"rpcs", c.rpcs-previous[e].rpcs),
			slog.Float64(prefix+"bytes_per_second", float64(bytes)/elapsed.Seconds()),
		)
		for k, name := range kindNames {
			delta := c.kindBytes[k] - previous[e].kindBytes[k]
			// Always show IHAVE/IWANT; omit idle fields for the other kinds.
			if delta != 0 || kind(k) == ihave || kind(k) == iwant {
				attrs = append(attrs, slog.Uint64(prefix+name+"_bytes", delta))
			}
		}
	}
	t.logger.LogAttrs(context.Background(), slog.LevelInfo, "gossipsub bandwidth", attrs...)
}

func (t *Tracer) RecvRPC(rpc *pubsub.RPC)            { t.record(recv, rpc) }
func (t *Tracer) SendRPC(rpc *pubsub.RPC, _ peer.ID) { t.record(send, rpc) }
func (t *Tracer) DropRPC(rpc *pubsub.RPC, _ peer.ID) { t.record(drop, rpc) }

// These hooks describe message processing or mesh state, not additional bytes.
func (*Tracer) OnNewOutboundStream(peer.ID, protocol.ID) {}
func (*Tracer) OnClosedOutboundStream(peer.ID)           {}
func (*Tracer) Join(string)                              {}
func (*Tracer) Leave(string)                             {}
func (*Tracer) Graft(peer.ID, string)                    {}
func (*Tracer) Prune(peer.ID, string)                    {}
func (t *Tracer) ValidateMessage(msg *pubsub.Message)    { t.observeMessage(msg) }
func (t *Tracer) DeliverMessage(msg *pubsub.Message)     { t.observeMessage(msg) }
func (*Tracer) RejectMessage(*pubsub.Message, string)    {}
func (t *Tracer) DuplicateMessage(msg *pubsub.Message)   { t.observeMessage(msg) }
func (*Tracer) ThrottlePeer(peer.ID)                     {}
func (*Tracer) UndeliverableMessage(*pubsub.Message)     {}
