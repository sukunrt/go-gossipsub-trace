// Package gossipsubsim runs real libp2p hosts and GossipSub routers over simnet.
// Construct and close a simulation inside testing/synctest.Test to run network
// delays and protocol timers on Go's simulated clock.
package gossipsubsim

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

const (
	DefaultBitsPerSecond = 20 * simlibp2p.OneMbps
	DefaultLatency       = time.Millisecond
)

// Config specifies network conditions and GossipSub options. Nodes is required.
// Zero bandwidth and latency select the defaults, matching pubsub's simnet tests.
type Config struct {
	Nodes int
	// BitsPerSecond applies independently to each node's uplink and downlink.
	BitsPerSecond int
	// Latency is the one-way packet routing delay, in addition to link queuing.
	Latency time.Duration
	// PubSubOptions apply to every node, without changing pubsub package globals.
	PubSubOptions []pubsub.Option
	// NodeOptions are appended after PubSubOptions. Use this to install a
	// different tracer, registry, or router configuration on each node.
	NodeOptions func(index int, h host.Host) []pubsub.Option
}

// Node exposes the native APIs for joining topics, subscribing, and publishing.
type Node struct {
	Host   host.Host
	PubSub *pubsub.PubSub
}

// Simulation owns the hosts, GossipSub contexts, and simulated packet network.
// Nodes initially have no peer connections or topic subscriptions. All traffic
// between their advertised QUIC addresses traverses simnet.
type Simulation struct {
	Nodes []Node

	network   *simnet.Simnet
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error
}

// New starts the packet network and creates a GossipSub router for each host.
// Call Close even if the parent context is canceled. A test should create and
// close the simulation within the same synctest bubble.
//
// Use valid pubsub options: pubsub v0.17.0 leaks an internal address-book
// goroutine when an option fails during construction. This helper closes its
// hosts and simnet on that error, but pubsub exposes no cleanup for the failed
// router. Such errors therefore fail synctest's goroutine shutdown check.
func New(ctx context.Context, cfg Config) (*Simulation, error) {
	if cfg.Nodes < 1 {
		return nil, errors.New("gossipsubsim: Nodes must be positive")
	}
	if cfg.BitsPerSecond < 0 || cfg.Latency < 0 {
		return nil, errors.New("gossipsubsim: bandwidth and latency must not be negative")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.BitsPerSecond == 0 {
		cfg.BitsPerSecond = DefaultBitsPerSecond
	}
	if cfg.Latency == 0 {
		cfg.Latency = DefaultLatency
	}

	// This is the same QUIC-over-simnet setup used by pubsub's getDefaultHosts.
	// SimpleLibp2pNetwork disables identify address discovery so it can run
	// inside synctest without waiting on the operating system's network.
	network, meta, err := simlibp2p.SimpleLibp2pNetwork(
		[]simlibp2p.NodeLinkSettingsAndCount{{
			LinkSettings: simnet.NodeBiDiLinkSettings{
				Downlink: simnet.LinkSettings{BitsPerSecond: cfg.BitsPerSecond},
				Uplink:   simnet.LinkSettings{BitsPerSecond: cfg.BitsPerSecond},
			},
			Count: cfg.Nodes,
		}},
		simnet.StaticLatency(cfg.Latency),
		simlibp2p.NetworkSettings{},
	)
	if err != nil {
		return nil, fmt.Errorf("gossipsubsim: create hosts: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &Simulation{Nodes: make([]Node, cfg.Nodes), network: network, ctx: ctx, cancel: cancel}
	// Retain every host before starting pubsub so any option failure closes all
	// hosts, including those whose routers have not yet been constructed.
	for i, h := range meta.Nodes {
		s.Nodes[i].Host = h
	}
	network.Start()
	for i := range s.Nodes {
		opts := append([]pubsub.Option(nil), cfg.PubSubOptions...)
		if cfg.NodeOptions != nil {
			opts = append(opts, cfg.NodeOptions(i, s.Nodes[i].Host)...)
		}
		ps, err := pubsub.NewGossipSub(ctx, s.Nodes[i].Host, opts...)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("gossipsubsim: node %d: %w", i, err), s.Close())
		}
		s.Nodes[i].PubSub = ps
	}
	return s, nil
}

// Connect adds a bidirectional libp2p connection by dialing from node a to b.
// Topic subscriptions and mesh formation happen asynchronously after dialing.
// Use a context deadline to bound connection setup in simulated time.
func (s *Simulation) Connect(ctx context.Context, a, b int) error {
	if a < 0 || a >= len(s.Nodes) || b < 0 || b >= len(s.Nodes) || a == b {
		return fmt.Errorf("gossipsubsim: invalid edge (%d, %d) for %d nodes", a, b, len(s.Nodes))
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	target := s.Nodes[b].Host
	if err := s.Nodes[a].Host.Connect(ctx, peer.AddrInfo{ID: target.ID(), Addrs: target.Addrs()}); err != nil {
		return fmt.Errorf("gossipsubsim: connect %d to %d: %w", a, b, err)
	}
	return nil
}

// ConnectAll connects every pair of nodes. GossipSub independently chooses its
// topic meshes from these underlying peer connections.
func (s *Simulation) ConnectAll(ctx context.Context) error {
	for a := range s.Nodes {
		for b := a + 1; b < len(s.Nodes); b++ {
			if err := s.Connect(ctx, a, b); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close cancels GossipSub, closes all hosts, then stops the simulated network.
// It is safe to call repeatedly. Any externally installed tracer remains owned
// by the caller and should be closed after the simulation.
func (s *Simulation) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		for _, node := range s.Nodes {
			s.closeErr = errors.Join(s.closeErr, node.Host.Close())
		}
		s.network.Close()
	})
	return s.closeErr
}
