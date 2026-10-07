package gossipsubtrace

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"google.golang.org/protobuf/encoding/protowire"
)

type idTopic struct{ id, topic string }
type topicKey struct {
	event event
	kind  kind
	topic string
}
type topicCounters struct{ bytes, ids uint64 }

func (t *Tracer) publishedTopics(rpc *pubsub.RPC) []idTopic {
	var result []idTopic
	for _, msg := range rpc.Publish {
		if msg != nil && msg.GetTopic() != "" {
			result = append(result, idTopic{t.idFn(msg), msg.GetTopic()})
		}
	}
	return result
}

// Caller holds t.mu. Learn the whole RPC before resolving any topic-less IDs.
func (t *Tracer) learnTopics(rpc *pubsub.RPC, published []idTopic, now time.Time) {
	for _, entry := range published {
		t.topics.remember(entry.id, entry.topic, now)
	}
	for _, message := range rpc.GetControl().GetIhave() {
		for _, id := range message.GetMessageIDs() {
			t.topics.remember(id, message.GetTopicID(), now)
		}
	}
}

// Cached pubsub.Message.ID respects pubsub's per-topic ID overrides. These
// callbacks supplement RPC learning without incrementing bandwidth counters.
func (t *Tracer) observeMessage(msg *pubsub.Message) {
	if msg == nil || msg.Message == nil || msg.GetTopic() == "" {
		return
	}
	id := msg.ID
	if id == "" {
		id = t.idFn(msg.Message)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.topics.remember(id, msg.GetTopic(), time.Now())
	}
}

func (t *Tracer) cleanMessageIDs() {
	defer close(t.cacheDone)
	ticker := time.NewTicker(MessageIDSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.mu.Lock()
			t.topics.cleanup(time.Now())
			t.mu.Unlock()
		case <-t.stop:
			return
		}
	}
}

func (t *Tracer) snapshotAll() (totals, map[topicKey]topicCounters) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.totals, maps.Clone(t.byTopic)
}

// attributeRPC preserves the existing per-kind totals. Entries with topics are
// charged directly; IWANT/IDONTWANT use the message-ID cache. Shared RPC framing
// and unknown fields have the empty topic, as do unresolved message IDs.
func attributeRPC(rpc *pubsub.RPC, measured counters, resolve func(string) string, emit func(kind, string, uint64, uint64)) {
	for _, msg := range rpc.Publish {
		emit(publish, msg.GetTopic(), encodedFieldSize(2, msg), 0)
	}
	for _, sub := range rpc.Subscriptions {
		k := unsubscribe
		if sub.GetSubscribe() {
			k = subscribe
		}
		emit(k, sub.GetTopicid(), encodedFieldSize(1, sub), 0)
	}
	if control := rpc.Control; control != nil {
		for _, msg := range control.Ihave {
			emit(ihave, msg.GetTopicID(), encodedFieldSize(1, msg), uint64(len(msg.GetMessageIDs())))
		}
		for _, msg := range control.Iwant {
			attributeIDs(iwant, msg.GetMessageIDs(), encodedFieldSize(2, msg), resolve, emit)
		}
		for _, msg := range control.Idontwant {
			attributeIDs(idontwant, msg.GetMessageIDs(), encodedFieldSize(5, msg), resolve, emit)
		}
		for _, msg := range control.Graft {
			emit(graft, msg.GetTopicID(), encodedFieldSize(3, msg), 0)
		}
		for _, msg := range control.Prune {
			emit(prune, msg.GetTopicID(), encodedFieldSize(4, msg), 0)
		}
	}
	for _, k := range []kind{overhead, other} {
		if measured.kindBytes[k] != 0 {
			emit(k, "", measured.kindBytes[k], 0)
		}
	}
}

// Split mixed-topic requests in proportion to encoded ID-field sizes, including
// each ID's tag and length prefix. Cumulative integer division allocates the
// shared entry framing deterministically and conserves every byte.
func attributeIDs(k kind, ids []string, bytes uint64, resolve func(string) string, emit func(kind, string, uint64, uint64)) {
	var weight uint64
	for _, id := range ids {
		weight += uint64(1 + protowire.SizeBytes(len(id)))
	}
	if weight == 0 {
		emit(k, "", bytes, 0)
		return
	}
	var cumulative, assigned uint64
	for _, id := range ids {
		cumulative += uint64(1 + protowire.SizeBytes(len(id)))
		next := bytes * cumulative / weight
		emit(k, resolve(id), next-assigned, 1)
		assigned = next
	}
}

func (t *Tracer) logTopicWindows(current, previous map[topicKey]topicCounters, elapsed time.Duration, final bool) {
	type group struct {
		topic string
		event event
	}
	groups := make(map[group][kindCount]uint64)
	for key, c := range current {
		delta := c.bytes - previous[key].bytes
		if delta == 0 {
			continue
		}
		g := group{key.topic, key.event}
		counts := groups[g]
		counts[key.kind] += delta
		groups[g] = counts
	}
	keys := slices.Collect(maps.Keys(groups))
	slices.SortFunc(keys, func(a, b group) int {
		if c := strings.Compare(a.topic, b.topic); c != 0 {
			return c
		}
		return int(a.event - b.event)
	})
	for _, key := range keys {
		counts := groups[key]
		var total uint64
		attrs := []slog.Attr{
			slog.String("topic", key.topic), slog.String("event", eventNames[key.event]),
			slog.Float64("interval_ms", float64(elapsed)/float64(time.Millisecond)),
		}
		for k, bytes := range counts {
			total += bytes
			if bytes != 0 {
				attrs = append(attrs, slog.Uint64(kindNames[k]+"_bytes", bytes))
			}
		}
		attrs = append(attrs, slog.Uint64("bytes", total), slog.Float64("bytes_per_second", float64(total)/elapsed.Seconds()))
		if final {
			attrs = append(attrs, slog.Bool("final", true))
		}
		t.logger.LogAttrs(context.Background(), slog.LevelInfo, "gossipsub topic bandwidth", attrs...)
	}
}
