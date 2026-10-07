package gossipsubsim

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

func testSimulation(t *testing.T, f func(*testing.T)) {
	t.Helper()
	// Match pubsub's workaround for concurrent synctest timer firing under
	// the race detector: https://github.com/golang/go/issues/78156.
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	synctest.Test(t, f)
}

func newTestSimulation(t *testing.T, config Config) (context.Context, *Simulation) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	sim, err := New(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sim.Close(); err != nil {
			t.Errorf("close simulation: %v", err)
		}
		if err := sim.Close(); err != nil {
			t.Errorf("close simulation again: %v", err)
		}
	})
	return ctx, sim
}

type testTopic struct {
	topics        []*pubsub.Topic
	subscriptions []*pubsub.Subscription
}

func subscribeAll(t *testing.T, sim *Simulation, name string) testTopic {
	t.Helper()
	joined := testTopic{}
	for i, node := range sim.Nodes {
		topic, err := node.PubSub.Join(name)
		if err != nil {
			t.Fatalf("node %d join %q: %v", i, name, err)
		}
		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatalf("node %d subscribe %q: %v", i, name, err)
		}
		joined.topics = append(joined.topics, topic)
		joined.subscriptions = append(joined.subscriptions, sub)
		t.Cleanup(func() {
			sub.Cancel()
			if err := topic.Close(); err != nil {
				t.Errorf("node %d close topic %q: %v", i, name, err)
			}
		})
	}
	return joined
}

func waitForTopicPeers(t *testing.T, joined testTopic, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ready := true
		for _, topic := range joined.topics {
			if len(topic.ListPeers()) != want {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		if !time.Now().Before(deadline) {
			for i, topic := range joined.topics {
				t.Errorf("node %d topic %q has %d peers, want %d", i, topic.String(), len(topic.ListPeers()), want)
			}
			t.Fatal("topic subscriptions did not propagate")
		}
		// synctest.Wait alone does not advance delayed packets or heartbeats.
		time.Sleep(10 * time.Millisecond)
	}
}

func publishAndReceiveAll(t *testing.T, ctx context.Context, joined testTopic, publisher int, data string) {
	t.Helper()
	if err := joined.topics[publisher].Publish(ctx, []byte(data)); err != nil {
		t.Fatalf("node %d publish: %v", publisher, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for i, sub := range joined.subscriptions {
		msg, err := sub.Next(waitCtx)
		if err != nil {
			t.Fatalf("node %d receive message from node %d: %v", i, publisher, err)
		}
		if got := string(msg.Data); got != data {
			t.Errorf("node %d received %q, want %q", i, got, data)
		}
		if got, want := msg.GetTopic(), joined.topics[i].String(); got != want {
			t.Errorf("node %d received topic %q, want %q", i, got, want)
		}
	}
}

func TestSimulationRingDelivery(t *testing.T) {
	testSimulation(t, func(t *testing.T) {
		params := pubsub.DefaultGossipSubParams()
		params.HeartbeatInitialDelay = 20 * time.Millisecond
		params.HeartbeatInterval = 100 * time.Millisecond
		ctx, sim := newTestSimulation(t, Config{
			Nodes:         5,
			BitsPerSecond: 10_000_000,
			Latency:       5 * time.Millisecond,
			PubSubOptions: []pubsub.Option{pubsub.WithGossipSubParams(params)},
		})
		for i, node := range sim.Nodes {
			if got := len(node.Host.Network().Peers()); got != 0 {
				t.Fatalf("node %d starts with %d connections, want none", i, got)
			}
		}

		blocks := subscribeAll(t, sim, "blocks")
		votes := subscribeAll(t, sim, "votes")
		for i := range sim.Nodes {
			if err := sim.Connect(ctx, i, (i+1)%len(sim.Nodes)); err != nil {
				t.Fatalf("connect ring edge %d: %v", i, err)
			}
		}
		waitForTopicPeers(t, blocks, 2)
		waitForTopicPeers(t, votes, 2)
		// Peer subscription visibility precedes mesh formation.
		time.Sleep(2 * params.HeartbeatInterval)
		for i, node := range sim.Nodes {
			if got := len(node.Host.Network().Peers()); got != 2 {
				t.Fatalf("node %d has %d connections, want ring degree 2", i, got)
			}
		}

		for _, publisher := range []int{0, 3} {
			publishAndReceiveAll(t, ctx, blocks, publisher, fmt.Sprintf("block from %d", publisher))
			publishAndReceiveAll(t, ctx, votes, publisher, fmt.Sprintf("vote from %d", publisher))
		}
	})
}

func TestSimulationGossipOnlyDelivery(t *testing.T) {
	testSimulation(t, func(t *testing.T) {
		params := pubsub.DefaultGossipSubParams()
		// A zero-degree mesh is the supported bootstrap configuration. With
		// flood publishing disabled, messages can reach another node only
		// through IHAVE -> IWANT -> PUBLISH on a later heartbeat.
		params.D, params.Dlo, params.Dhi = 0, 0, 0
		params.Dout, params.Dscore = 0, 0
		params.GossipFactor = 1
		params.HeartbeatInitialDelay = 20 * time.Millisecond
		params.HeartbeatInterval = 100 * time.Millisecond
		optionHosts := make([]host.Host, 4)
		ctx, sim := newTestSimulation(t, Config{
			Nodes:         len(optionHosts),
			PubSubOptions: []pubsub.Option{pubsub.WithFloodPublish(false)},
			NodeOptions: func(i int, h host.Host) []pubsub.Option {
				optionHosts[i] = h
				return []pubsub.Option{pubsub.WithGossipSubParams(params)}
			},
		})
		for i, node := range sim.Nodes {
			if optionHosts[i] != node.Host {
				t.Fatalf("node %d option factory received the wrong host", i)
			}
		}
		joined := subscribeAll(t, sim, "gossip-only")
		if err := sim.ConnectAll(ctx); err != nil {
			t.Fatal(err)
		}
		waitForTopicPeers(t, joined, len(sim.Nodes)-1)
		for _, publisher := range []int{0, 2} {
			publishAndReceiveAll(t, ctx, joined, publisher, fmt.Sprintf("gossip from %d", publisher))
		}
	})
}
