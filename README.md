# go-gossipsub-trace

> **Note:** This code is completely vibecoded: an AI coding assistant wrote it. It has not had a
> careful human review. Use it with care.

A `pubsub.RawTracer` that measures GossipSub RPC bandwidth, exports Prometheus
counters, and writes periodic text summaries with topic attribution. IHAVE,
IWANT, IDONTWANT, published messages, subscriptions, GRAFT, and PRUNE are included.

## Usage

Install the tracer when constructing GossipSub with your existing host:

```go
import (
    "log/slog"
    "os"
    "time"

    pubsub "github.com/libp2p/go-libp2p-pubsub"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    trace "github.com/sukunrt/go-gossipsub-trace"
)

// Inside your application's setup, with ctx, host, and an HTTP mux available:
registry := prometheus.NewRegistry()
tracer, err := trace.New(trace.Config{
    Registerer: registry,
    Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
    LogInterval: 100 * time.Millisecond, // Or 50 * time.Millisecond.
})
if err != nil {
    return err
}
defer tracer.Close() // Keep the tracer alive for the lifetime of GossipSub.

ps, err := pubsub.NewGossipSub(ctx, host, pubsub.WithRawTracer(tracer))
if err != nil {
    return err
}
// Use ps.Join(...), etc.
mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
```

`Config{}` uses the default Prometheus registry and an Info-level text logger on
stderr, with a 100 ms interval. A supplied logger must enable Info to receive
summaries. `Close` stops the reporting goroutine and flushes any remaining
traffic. Shut down GossipSub before closing its tracer. Metrics retain their
final values after close; unregister the tracer explicitly if replacing it.

For multiple tracers in one registry, use
`prometheus.WrapRegistererWith(prometheus.Labels{"node": "node-a"}, registry)`
with a different `node` value for each tracer. Duplicate registration returns an
error. Topic names are labels on the topic counters; peer IDs and message IDs
are never labels. Topic counter series remain for the tracer's lifetime.

## Message ID to topic attribution

The tracer learns message ID → topic mappings from incoming and outgoing IHAVE
entries and published messages. IWANT and IDONTWANT look up those mappings.
Message-processing callbacks also learn from pubsub's cached message IDs without
adding bandwidth. Dropped RPCs do not teach new mappings.

Mappings expire **120 seconds after their last observation**, with a cleanup
tick every **10 seconds**. An ID lookup treats an expired entry as unknown even
before the next sweep. The cache uses a map plus a linked list ordered by expiry:
new entries go to the back, refreshes move the existing node to the back, and
cleanup removes expired nodes from the front until it reaches a fresh entry.
Lookup and refresh are O(1); each sweep is O(number of removed entries), with no
scan of live entries and no size-based eviction. Cleanup runs independently of
the logger. `Close` releases all cached IDs.

If an ID is observed under conflicting topics, it remains unattributed until
that cache entry expires. Unknown, expired, or ambiguous IDs use `topic=""`.
Attribution is based on observed topic claims and is not retroactively changed
when later messages provide more information.

For a custom GossipSub message-ID function, pass the **same function** as
`Config.MessageIDFn` and `pubsub.WithMessageIdFn`. The default is
`pubsub.DefaultMsgIdFn`. If using per-topic ID overrides, supply a function that
dispatches on `msg.GetTopic()` and matches those overrides. IHAVE-based learning
already has the IDs and needs no ID computation.

An IWANT/IDONTWANT entry can reference several topics. Its encoded bytes,
including its own framing, are split in proportion to each ID's encoded field
size, with deterministic integer rounding. Topic byte totals sum exactly to the
existing per-kind totals. Shared RPC/control-envelope framing and unclassified
extension bytes also use the empty topic.

## Text logs

One `msg="gossipsub bandwidth"` line is emitted per interval, including idle
intervals. Each line contains:

- `interval_ms`: actual elapsed time for this window.
- `recv_bytes`, `send_bytes`, `drop_bytes`: byte deltas for the window.
- `recv_rpcs`, `send_rpcs`, `drop_rpcs`: RPC counts for the window.
- `recv_bytes_per_second`, `send_bytes_per_second`, `drop_bytes_per_second`:
  window bytes divided by actual elapsed seconds. Dropped bytes describe unsent
  traffic.
- `recv_ihave_bytes`, `send_ihave_bytes`, `recv_iwant_bytes`,
  `send_iwant_bytes`, and the corresponding dropped fields, even when zero.
- The same per-kind byte fields for other active kinds, such as
  `send_publish_bytes` and `recv_idontwant_bytes`.

Each active topic/event pair also gets a `msg="gossipsub topic bandwidth"` line
with `topic`, `event`, `interval_ms`, `bytes`, `bytes_per_second`, and the active
per-kind byte fields, such as `iwant_bytes`. These are interval deltas as well;
inactive topics emit no extra lines.

Logging happens in its own goroutine. A slow log writer cannot stall tracer
callbacks, but it can lengthen reporting windows; counts are retained, and rates
use the elapsed time. `Close` waits for pending logging to finish.

## Prometheus metrics

| Counter | Labels | Meaning |
| --- | --- | --- |
| `gossipsub_rpc_bytes_total` | `event` | Estimated full RPC frame bytes |
| `gossipsub_rpcs_total` | `event` | Number of RPC callbacks |
| `gossipsub_message_bytes_total` | `event`, `kind` | Byte breakdown by kind |
| `gossipsub_messages_total` | `event`, `kind` | Number of messages/control entries |
| `gossipsub_message_ids_total` | `event`, `kind` | IDs carried in IHAVE/IWANT/IDONTWANT |
| `gossipsub_topic_message_bytes_total` | `event`, `kind`, `topic` | Byte breakdown attributed to topics |
| `gossipsub_topic_message_ids_total` | `event`, `kind`, `topic` | IHAVE/IWANT/IDONTWANT IDs attributed to topics |

`event` is `recv`, `send`, or `drop`. `kind` is `publish`, `subscribe`,
`unsubscribe`, `ihave`, `iwant`, `idontwant`, `graft`, `prune`, `overhead`, or
`other`. The last two kinds only have byte counters. An IHAVE entry advertising
100 IDs counts as one message and 100 message IDs. Repeated IDs and copies sent
to multiple peers are counted each time they appear.

For example, outgoing IHAVE/IWANT bytes per second by topic:

```promql
sum by (topic, kind) (
  rate(gossipsub_topic_message_bytes_total{event="send",kind=~"ihave|iwant"}[1m])
)
```

Prometheus counters are cumulative and updated on every callback, independently
of logging. Scrape at your usual interval; text logs provide the 50–100 ms view.

## Measurement semantics

The tracer uses `RecvRPC`, `SendRPC`, and `DropRPC`, which expose the complete
protobuf RPC. It computes sizes without serializing or retaining the message.
The high-level `EventTracer` only exposes selected metadata and cannot supply
the complete published-message size.

Total bytes are `proto.Size(rpc) + unsigned-varint-length-prefix-size`. Each
known message is charged its protobuf payload, field tag, and length prefix.
The RPC length prefix and shared control-envelope framing go to `overhead`;
extension fields and unknown fields go to `other`. The kinds sum to the full
RPC byte estimate. Published-message bytes include topic and signature fields,
as well as the application payload in its existing encoding.

These are estimates of RPC traffic observed by the hooks:

- `SendRPC` fires after enqueueing, before the stream write completes. A later
  write failure cannot be determined from this hook. `DropRPC` is kept separate
  and never added to sent bytes.
- `RecvRPC` receives parsed RPCs. Malformed frames and RPCs rejected before the
  hook (for example, by an application RPC inspector) are outside its coverage.
  Re-encoded sizes can differ from noncanonical incoming protobuf encodings.
- Stream setup traffic that bypasses the hooks, transport headers, encryption,
  multiplexing, and retransmissions are outside these byte counts.
- IWANT and IDONTWANT contain IDs without topics; attribution depends on the
  retained ID-to-topic observations described above.
- Message validation, delivery, duplicate detection, and mesh callbacks do not
  increment bandwidth, so processing a received message cannot count it twice.

The module targets go-libp2p-pubsub v0.17.0. For development against a local
checkout, use a Go workspace or a main-module `replace` directive.

## Simnet simulation

`internal/gossipsubsim` provides a multi-node simulation using the same setup as
the upstream GossipSub tests: real libp2p QUIC hosts over
`simlibp2p.SimpleLibp2pNetwork`, with a real GossipSub router on every node.
Defaults are 20 Mbps for each node's uplink/downlink and 1 ms one-way packet
latency. These conditions are configurable through `Config`.

The simulation starts with no connections or subscriptions. Use `Connect` to
build a topology or `ConnectAll` for a fully connected network. `Nodes[i].Host`
and `Nodes[i].PubSub` expose the native host and pubsub APIs for subscriptions,
publishing, and inspecting peers. `PubSubOptions` applies common configuration;
`NodeOptions` can install a distinct tracer and registry on each node for later
integration tests.

Create the simulation inside `synctest.Test`, using the inner test's cleanup:

```go
synctest.Test(t, func(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    sim, err := gossipsubsim.New(ctx, gossipsubsim.Config{Nodes: 5})
    if err != nil {
        t.Fatal(err)
    }
    defer sim.Close()
    if err := sim.ConnectAll(ctx); err != nil {
        t.Fatal(err)
    }
    // Join topics and subscribe using sim.Nodes[i].PubSub.Join(...).
    // time.Sleep advances simulated packet delays and protocol timers.
})
```

The simulation's tests exercise multi-hop delivery and delivery through gossip
with eager forwarding disabled. The latter provides an IHAVE → IWANT → PUBLISH
scenario for tracer tests. No socket interception or handcrafted gossip RPCs
are involved. Tests use simulated time; peer identities and GossipSub choices
are still randomized.

Run these scenarios with:

```sh
go test -v ./internal/gossipsubsim
```

`Close` cancels GossipSub contexts, closes hosts, and finally stops simnet.
Installers retain ownership of their tracers and should close them after the
simulation. Keep setup and cleanup inside the same `synctest` bubble.

Known upstream limitation: pubsub v0.17.0 allocates an internal address book
before applying options and does not close it if an option fails. The helper
closes its hosts and simnet on construction errors, but that pubsub goroutine
has no public cleanup API and fails synctest's shutdown check. The simulation
scenarios use valid options and exercise normal shutdown.

## Tests

```sh
go test -race ./...
go vet ./...
```

Tests compare attribution with serialized protobuf fields, exercise mixed and
empty RPCs and extensions, check Prometheus counters and periodic text output,
and cover concurrent callbacks, scrapes, slow log writers, and shutdown. Topic
tests cover mixed requests, unknown/conflicting IDs, custom ID functions, exact
byte conservation, ordered cache refresh, expiry, and periodic cleanup using
Go's fake-time `testing/synctest` environment.
