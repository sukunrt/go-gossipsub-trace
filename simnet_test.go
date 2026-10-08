package gossipsubtrace

import (
	"bytes"
	"context"
	"iter"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sukunrt/go-gossipsub-trace/internal/gossipsubsim"
	"google.golang.org/protobuf/proto"
)

func partialExtension(onRPC func(*pb.PartialMessagesExtension)) *partialmessages.PartialMessagesExtension[bool] {
	return &partialmessages.PartialMessagesExtension[bool]{
		Logger:       slog.New(slog.DiscardHandler),
		OnEmitGossip: func(string, []byte, []peer.ID, map[peer.ID]bool) {},
		OnIncomingRPC: func(_ peer.ID, _ map[peer.ID]bool, rpc *pb.PartialMessagesExtension) error {
			if onRPC != nil {
				onRPC(rpc)
			}
			return nil
		},
	}
}

// The publisher sends a full message to a classic peer and a partial message
// to a peer that requests partial messages, on the same topic.
func TestSimnetPartialAndFullPeers(t *testing.T) {
	// Same workaround as gossipsubsim tests: https://github.com/golang/go/issues/78156.
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	synctest.Test(t, func(t *testing.T) {
		const topicName = "blocks"
		const publisher, pmPeer, fullPeer = 0, 1, 2
		group := []byte("group")
		payload := bytes.Repeat([]byte{7}, 1000)
		metadata := []byte{0b11}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		logs := make([]bytes.Buffer, 3)
		tracers := make([]*Tracer, 3)
		registries := make([]*prometheus.Registry, 3)
		for i := range tracers {
			tracers[i], registries[i] = newTestTracer(t, Config{
				LogInterval: time.Hour, Logger: slog.New(slog.NewTextHandler(&logs[i], nil)),
			})
		}
		received := make(chan *pb.PartialMessagesExtension, 1)
		params := pubsub.DefaultGossipSubParams()
		params.HeartbeatInitialDelay = 20 * time.Millisecond
		params.HeartbeatInterval = 100 * time.Millisecond
		sim, err := gossipsubsim.New(ctx, gossipsubsim.Config{
			Nodes:         3,
			PubSubOptions: []pubsub.Option{pubsub.WithGossipSubParams(params)},
			NodeOptions: func(i int, _ host.Host) []pubsub.Option {
				opts := []pubsub.Option{pubsub.WithRawTracer(tracers[i])}
				switch i {
				case publisher:
					opts = append(opts, pubsub.WithPartialMessagesExtension(partialExtension(nil)))
				case pmPeer:
					opts = append(opts, pubsub.WithPartialMessagesExtension(partialExtension(func(rpc *pb.PartialMessagesExtension) {
						select {
						case received <- proto.Clone(rpc).(*pb.PartialMessagesExtension):
						default:
						}
					})))
				}
				return opts
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sim.Close()

		topicOpts := [][]pubsub.TopicOpt{
			publisher: {pubsub.SupportsPartialMessages()},
			pmPeer:    {pubsub.RequestPartialMessages()},
		}
		topics := make([]*pubsub.Topic, 3)
		subs := make([]*pubsub.Subscription, 3)
		for i, node := range sim.Nodes {
			var opts []pubsub.TopicOpt
			if i < len(topicOpts) {
				opts = topicOpts[i]
			}
			if topics[i], err = node.PubSub.Join(topicName, opts...); err != nil {
				t.Fatal(err)
			}
			if subs[i], err = topics[i].Subscribe(); err != nil {
				t.Fatal(err)
			}
		}
		for _, p := range []int{pmPeer, fullPeer} {
			if err := sim.Connect(ctx, publisher, p); err != nil {
				t.Fatal(err)
			}
		}
		for len(topics[publisher].ListPeers()) != 2 {
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(3 * params.HeartbeatInterval) // Form the mesh.

		if err := topics[publisher].Publish(ctx, payload); err != nil {
			t.Fatal(err)
		}
		err = pubsub.PublishPartial(sim.Nodes[publisher].PubSub, topicName, group,
			func(sent map[peer.ID]bool, requestsPartial func(peer.ID) bool) iter.Seq2[peer.ID, partialmessages.PublishAction] {
				return func(yield func(peer.ID, partialmessages.PublishAction) bool) {
					for p := range sent {
						if sent[p] || !requestsPartial(p) {
							continue
						}
						sent[p] = true
						if !yield(p, partialmessages.PublishAction{EncodedPartialMessage: payload, EncodedPartsMetadata: metadata}) {
							return
						}
					}
				}
			})
		if err != nil {
			t.Fatal(err)
		}
		msg, err := subs[fullPeer].Next(ctx)
		if err != nil || !bytes.Equal(msg.Data, payload) {
			t.Fatalf("classic peer did not receive the full message: %v", err)
		}
		var rpc *pb.PartialMessagesExtension
		select {
		case rpc = <-received:
		case <-time.After(5 * time.Second):
			t.Fatal("PM peer did not receive the partial message")
		}
		if !bytes.Equal(rpc.GetPartialMessage(), payload) {
			t.Fatal("PM peer received a different partial message")
		}
		time.Sleep(3 * params.HeartbeatInterval) // Fail on late full messages.
		if err := sim.Close(); err != nil {
			t.Fatal(err)
		}
		for _, tracer := range tracers {
			_ = tracer.Close()
		}

		count := func(node int, e event, k kind) topicCounters {
			_, topics := tracers[node].snapshotAll()
			return topics[topicKey{event: e, kind: k, topic: topicName}]
		}
		want := topicCounters{
			bytes: encodedFieldSize(10, rpc),
			pm:    pmBytes{message: 1000, metadata: 1, header: uint64(len(topicName) + len(group))},
		}
		if got := count(publisher, send, partial); got != want {
			t.Errorf("publisher sent partial = %+v, want %+v", got, want)
		}
		if got := count(pmPeer, recv, partial); got != want {
			t.Errorf("PM peer received partial = %+v, want %+v", got, want)
		}
		if got := count(fullPeer, recv, partial); got != (topicCounters{}) {
			t.Errorf("classic peer received partial = %+v", got)
		}
		full := count(publisher, send, publish).bytes
		if full < uint64(len(payload)) || count(fullPeer, recv, publish).bytes != full {
			t.Errorf("full message bytes: publisher sent %d, classic peer received %d", full, count(fullPeer, recv, publish).bytes)
		}
		if got := count(pmPeer, recv, publish).bytes; got != 0 {
			t.Errorf("PM peer received %d full-message bytes", got)
		}

		labels := map[string]string{"event": "send", "topic": topicName}
		assertMetric(t, registries[publisher], "gossipsub_pm_bytes_total", labels, float64(want.bytes))
		assertMetric(t, registries[publisher], "gossipsub_pm_message_bytes_total", labels, 1000)
		assertMetric(t, registries[publisher], "gossipsub_pm_metadata_bytes_total", labels, 1)
		assertMetric(t, registries[publisher], "gossipsub_pm_header_bytes_total", labels, float64(want.pm.header))
		labels["event"] = "recv"
		assertMetric(t, registries[pmPeer], "gossipsub_pm_message_bytes_total", labels, 1000)

		for _, line := range strings.Split(logs[publisher].String(), "\n") {
			if strings.Contains(line, "topic="+topicName+" event=send") {
				t.Log(line)
				if logNumber(t, line, "publish_bytes") != float64(full) || logNumber(t, line, "pm_message_bytes") != 1000 {
					t.Errorf("wrong topic log: %s", line)
				}
				return
			}
		}
		t.Fatalf("missing topic log: %s", logs[publisher].String())
	})
}
