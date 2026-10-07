package gossipsubtrace

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

func advertisement(topic string, ids ...string) *pubsub.RPC {
	return &pubsub.RPC{RPC: pb.RPC{Control: &pb.ControlMessage{
		Ihave: []*pb.ControlIHave{{TopicID: proto.String(topic), MessageIDs: ids}},
	}}}
}

func request(ids ...string) *pubsub.RPC {
	return &pubsub.RPC{RPC: pb.RPC{Control: &pb.ControlMessage{
		Iwant: []*pb.ControlIWant{{MessageIDs: ids}},
	}}}
}

func assertTopicMetric(t *testing.T, registry *prometheus.Registry, name, event, kind, topic string, want float64) {
	t.Helper()
	assertMetric(t, registry, name, map[string]string{"event": event, "kind": kind, "topic": topic}, want)
}

func TestIWantMixedTopicsAndUnknownIDs(t *testing.T) {
	tracer, registry := newTestTracer(t, Config{})
	tracer.RecvRPC(advertisement("blocks", "a"))
	tracer.RecvRPC(advertisement("attestations", "bbb"))
	tracer.SendRPC(request("a", "bbb", "missing", "a"), "peer")

	// ID fields occupy 3, 5, 9, and 3 bytes. Divide the complete 22-byte
	// IWANT entry proportionally, including its two framing bytes.
	for topic, want := range map[string]struct{ bytes, ids float64 }{
		"blocks": {7, 2}, "attestations": {5, 1}, "": {10, 1},
	} {
		assertTopicMetric(t, registry, "gossipsub_topic_message_bytes_total", "send", "iwant", topic, want.bytes)
		assertTopicMetric(t, registry, "gossipsub_topic_message_ids_total", "send", "iwant", topic, want.ids)
	}
	assertMetric(t, registry, "gossipsub_message_bytes_total", map[string]string{"event": "send", "kind": "iwant"}, 22)
	assertTopicSums(t, tracer)
}

func TestIncomingIWantUsesSentIHave(t *testing.T) {
	tracer, registry := newTestTracer(t, Config{})
	tracer.SendRPC(advertisement("blocks", "id"), "peer")
	tracer.RecvRPC(request("id"))
	assertTopicMetric(t, registry, "gossipsub_topic_message_ids_total", "recv", "iwant", "blocks", 1)
	assertTopicSums(t, tracer)
}

func TestSameRPCMappingsAndIDontWant(t *testing.T) {
	tracer, registry := newTestTracer(t, Config{})
	rpc := advertisement("blocks", "id")
	rpc.Control.Iwant = []*pb.ControlIWant{{MessageIDs: []string{"id"}}}
	rpc.Control.Idontwant = []*pb.ControlIDontWant{{MessageIDs: []string{"id"}}}
	tracer.RecvRPC(rpc)
	for _, kind := range []string{"ihave", "iwant", "idontwant"} {
		assertTopicMetric(t, registry, "gossipsub_topic_message_ids_total", "recv", kind, "blocks", 1)
	}
	assertTopicSums(t, tracer)
}

func TestDroppedAdvertisementsDoNotTeachTopics(t *testing.T) {
	tracer, registry := newTestTracer(t, Config{})
	tracer.DropRPC(advertisement("blocks", "id"), "peer")
	tracer.RecvRPC(request("id"))
	assertTopicMetric(t, registry, "gossipsub_topic_message_ids_total", "recv", "iwant", "", 1)
	assertTopicSums(t, tracer)
}

func TestConflictingTopicsAreUnattributed(t *testing.T) {
	tracer, registry := newTestTracer(t, Config{})
	tracer.RecvRPC(advertisement("blocks", "id"))
	tracer.RecvRPC(advertisement("attestations", "id"))
	tracer.SendRPC(request("id"), "peer")
	assertTopicMetric(t, registry, "gossipsub_topic_message_ids_total", "send", "iwant", "", 1)
}

func TestPublishedAndProcessedMessagesTeachTopics(t *testing.T) {
	for _, name := range []string{"default", "custom", "cached"} {
		t.Run(name, func(t *testing.T) {
			msg := &pb.Message{Topic: proto.String("blocks"), From: []byte("source"), Seqno: []byte{1}, Data: []byte("custom-id")}
			cfg := Config{}
			id := pubsub.DefaultMsgIdFn(msg)
			if name == "custom" {
				cfg.MessageIDFn = func(m *pb.Message) string { return string(m.GetData()) }
				id = "custom-id"
			}
			tracer, registry := newTestTracer(t, cfg)
			if name == "cached" {
				id = "pubsub-cached-id"
				tracer.ValidateMessage(&pubsub.Message{Message: msg, ID: id})
				tracer.DeliverMessage(&pubsub.Message{Message: msg, ID: id})
				tracer.DuplicateMessage(&pubsub.Message{Message: msg, ID: id})
			} else {
				tracer.RecvRPC(&pubsub.RPC{RPC: pb.RPC{Publish: []*pb.Message{msg, nil}}})
			}
			tracer.SendRPC(request(id), "peer")
			assertTopicMetric(t, registry, "gossipsub_topic_message_ids_total", "send", "iwant", "blocks", 1)
			if name == "cached" && tracer.snapshot()[recv].bytes != 0 {
				t.Fatal("message processing added bandwidth")
			}
			assertTopicSums(t, tracer)
		})
	}
}

func assertTopicSums(t *testing.T, tracer *Tracer) {
	t.Helper()
	totals, topics := tracer.snapshotAll()
	var sums [eventCount][kindCount]topicCounters
	for key, c := range topics {
		sums[key.event][key.kind].bytes += c.bytes
		sums[key.event][key.kind].ids += c.ids
	}
	for e, c := range totals {
		for k := range kindNames {
			if sums[e][k].bytes != c.kindBytes[k] || sums[e][k].ids != c.messageIDs[k] {
				t.Errorf("%s/%s: topic sums = %+v, total bytes/IDs = %d/%d", eventNames[e], kindNames[k], sums[e][k], c.kindBytes[k], c.messageIDs[k])
			}
		}
	}
}

func TestTopicByteConservationForAllKinds(t *testing.T) {
	tracer, _ := newTestTracer(t, Config{})
	rpc := mixedRPC()
	rpc.Partial = &pb.PartialMessagesExtension{PartialMessage: []byte("extension")}
	rpc.Control.Iwant = append(rpc.Control.Iwant, nil, &pb.ControlIWant{})
	tracer.RecvRPC(rpc)
	tracer.SendRPC(rpc, "peer")
	tracer.DropRPC(rpc, "peer")
	assertTopicSums(t, tracer)
}

func TestTopicBandwidthTextLogs(t *testing.T) {
	var output bytes.Buffer
	tracer, _ := newTestTracer(t, Config{
		LogInterval: time.Hour, Logger: slog.New(slog.NewTextHandler(&output, nil)),
	})
	tracer.RecvRPC(advertisement("blocks", "id"))
	tracer.SendRPC(request("id"), "peer")
	_ = tracer.Close()
	for _, line := range strings.Split(output.String(), "\n") {
		if strings.Contains(line, `msg="gossipsub topic bandwidth"`) && strings.Contains(line, "topic=blocks event=send") {
			if logNumber(t, line, "iwant_bytes") != 6 || logNumber(t, line, "bytes") != 6 {
				t.Fatalf("wrong topic bandwidth: %s", line)
			}
			return
		}
	}
	t.Fatalf("missing per-topic IWANT log: %s", output.String())
}

func TestCacheSweepRunsEveryTenSeconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracer, _ := newTestTracer(t, Config{LogInterval: time.Hour})
		synctest.Wait() // Start both tickers at the bubble's initial time.
		time.Sleep(time.Second)
		tracer.RecvRPC(advertisement("blocks", "id"))
		time.Sleep(119 * time.Second)
		synctest.Wait()
		tracer.mu.Lock()
		retained := len(tracer.topics.entries)
		tracer.mu.Unlock()
		if retained != 1 {
			t.Fatal("sweep removed a mapping before 120 seconds of age")
		}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		tracer.mu.Lock()
		remaining := len(tracer.topics.entries)
		tracer.mu.Unlock()
		if remaining != 0 {
			t.Fatal("next 10-second sweep did not remove the expired mapping")
		}
	})
}

func TestExpiredIDsAreUnattributedBeforeNextSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracer, registry := newTestTracer(t, Config{LogInterval: time.Hour})
		synctest.Wait()
		time.Sleep(time.Second)
		tracer.RecvRPC(advertisement("blocks", "id"))
		time.Sleep(MessageIDRetention)
		tracer.SendRPC(request("id"), "peer")
		assertTopicMetric(t, registry, "gossipsub_topic_message_ids_total", "send", "iwant", "", 1)
	})
}

func TestCloseReleasesMessageIDCache(t *testing.T) {
	tracer, registry := newTestTracer(t, Config{})
	tracer.RecvRPC(advertisement("blocks", "id"))
	_ = tracer.Close()
	tracer.RecvRPC(advertisement("blocks", "later"))
	tracer.ValidateMessage(&pubsub.Message{Message: &pb.Message{Topic: proto.String("blocks")}, ID: "later"})
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	if len(tracer.topics.entries) != 0 || tracer.topics.expiry.Len() != 0 {
		t.Fatal("closed tracer retained message-ID cache")
	}
	// The collector can remain registered, retaining only aggregate counters.
	_ = registry
}
