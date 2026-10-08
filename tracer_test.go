package gossipsubtrace

import (
	"bytes"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/prometheus/client_golang/prometheus"
)

func newTestTracer(t *testing.T, cfg Config) (*Tracer, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	cfg.Registerer = registry
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	tracer, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tracer.Close() })
	return tracer, registry
}

func assertMetric(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string, want float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			matches := len(metric.Label) == len(labels)
			for _, label := range metric.Label {
				matches = matches && labels[label.GetName()] == label.GetValue()
			}
			if matches {
				if got := metric.GetCounter().GetValue(); got != want {
					t.Errorf("%s%v = %v, want %v", name, labels, got, want)
				}
				return
			}
		}
	}
	t.Fatalf("metric %s%v not found", name, labels)
}

func TestRPCMetricsAndNoDoubleCounting(t *testing.T) {
	tracer, registry := newTestTracer(t, Config{})
	rpc := mixedRPC()
	tracer.RecvRPC(rpc)
	tracer.SendRPC(rpc, "peer")
	tracer.SendRPCCompleted(rpc, "peer")
	tracer.SendRPCCompleted(rpc, "another-peer")
	tracer.DropRPC(rpc, "peer")
	tracer.RecvRPC(nil)
	tracer.ValidateMessage(nil)
	tracer.DeliverMessage(nil)
	tracer.DuplicateMessage(nil)
	tracer.RejectMessage(nil, "invalid")
	tracer.Graft("peer", "blocks")
	tracer.Prune("peer", "blocks")
	tracer.Join("blocks")
	tracer.Leave("blocks")
	tracer.OnNewOutboundStream("peer", "/meshsub/1.2.0")
	tracer.OnClosedOutboundStream("peer")
	tracer.ThrottlePeer("peer")
	tracer.UndeliverableMessage(nil)
	frame := encoded(t, &rpc.RPC)
	size := float64(len(frame) + prefixLen(len(frame)))
	for event, count := range map[string]float64{"recv": 1, "queue": 1, "send": 2, "drop": 1} {
		assertMetric(t, registry, "gossipsub_rpcs_total", map[string]string{"event": event}, count)
		assertMetric(t, registry, "gossipsub_rpc_bytes_total", map[string]string{"event": event}, count*size)
		assertMetric(t, registry, "gossipsub_messages_total", map[string]string{"event": event, "kind": "ihave"}, count*2)
		assertMetric(t, registry, "gossipsub_message_ids_total", map[string]string{"event": event, "kind": "iwant"}, count*2)
		var ihaveBytes int
		for _, msg := range rpc.Control.Ihave {
			b := encoded(t, msg)
			ihaveBytes += 1 + prefixLen(len(b)) + len(b)
		}
		assertMetric(t, registry, "gossipsub_message_bytes_total", map[string]string{"event": event, "kind": "ihave"}, count*float64(ihaveBytes))
	}
}

func TestConcurrentCallbacksAndScrapes(t *testing.T) {
	tracer, registry := newTestTracer(t, Config{LogInterval: time.Millisecond})
	const workers, iterations = 8, 1000
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			// Each worker uses its own RPC, as pubsub normally owns its arguments.
			rpc := mixedRPC()
			for range iterations {
				tracer.RecvRPC(rpc)
				tracer.SendRPC(rpc, "peer")
				tracer.SendRPCCompleted(rpc, "peer")
				tracer.DropRPC(rpc, "peer")
			}
		})
	}
	wg.Go(func() {
		for range 100 {
			if _, err := registry.Gather(); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Wait()
	for _, event := range eventNames {
		assertMetric(t, registry, "gossipsub_rpcs_total", map[string]string{"event": event}, workers*iterations)
	}
	for range workers {
		wg.Go(func() { _ = tracer.Close() })
	}
	wg.Wait()
	before := tracer.snapshot()
	tracer.SendRPCCompleted(mixedRPC(), "peer")
	if tracer.snapshot() != before {
		t.Fatal("callback changed counters after Close")
	}
}

func TestConfigAndRegistration(t *testing.T) {
	if _, err := New(Config{LogInterval: -time.Second}); err == nil {
		t.Fatal("negative interval accepted")
	}
	tracer, registry := newTestTracer(t, Config{})
	if tracer.interval != 100*time.Millisecond {
		t.Fatalf("default interval = %v", tracer.interval)
	}
	if _, err := New(Config{Registerer: registry}); err == nil {
		t.Fatal("duplicate registration accepted")
	}
	assertMetric(t, registry, "gossipsub_message_ids_total", map[string]string{"event": "recv", "kind": "iwant"}, 0)
}

type lineWriter chan string

func (w lineWriter) Write(p []byte) (int, error) {
	w <- string(p)
	return len(p), nil
}

func nextLine(t *testing.T, lines lineWriter) string {
	t.Helper()
	select {
	case line := <-lines:
		return line
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for bandwidth log")
		return ""
	}
}

func logNumber(t *testing.T, line, key string) float64 {
	t.Helper()
	for _, field := range strings.Fields(line) {
		if value, found := strings.CutPrefix(field, key+"="); found {
			n, err := strconv.ParseFloat(value, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatalf("missing %s in log: %s", key, line)
	return 0
}

func TestPeriodicTextLogsUseWindowDeltas(t *testing.T) {
	for _, interval := range []time.Duration{50 * time.Millisecond, 100 * time.Millisecond} {
		t.Run(interval.String(), func(t *testing.T) {
			lines := make(lineWriter, 100)
			tracer, _ := newTestTracer(t, Config{
				LogInterval: interval, Logger: slog.New(slog.NewTextHandler(lines, nil)),
			})
			tracer.SendRPCCompleted(mixedRPC(), "peer")
			first := nextLine(t, lines)
			if !strings.Contains(first, `msg="gossipsub bandwidth"`) {
				t.Fatalf("unexpected text log: %s", first)
			}
			if logNumber(t, first, "send_bytes") <= 0 || logNumber(t, first, "send_ihave_bytes") <= 0 || logNumber(t, first, "send_iwant_bytes") <= 0 {
				t.Fatalf("missing traffic: %s", first)
			}
			if logNumber(t, first, "send_bytes_per_second") <= 0 || logNumber(t, first, "interval_ms") <= 0 {
				t.Fatalf("invalid bandwidth rate: %s", first)
			}
			second := nextLine(t, lines)
			for !strings.Contains(second, `msg="gossipsub bandwidth"`) {
				second = nextLine(t, lines)
			}
			if logNumber(t, second, "send_bytes") != 0 || logNumber(t, second, "send_bytes_per_second") != 0 {
				t.Fatalf("window reported cumulative traffic: %s", second)
			}
		})
	}
}

func TestCloseFlushesRemainingTraffic(t *testing.T) {
	var output bytes.Buffer
	tracer, _ := newTestTracer(t, Config{
		LogInterval: time.Hour, Logger: slog.New(slog.NewTextHandler(&output, nil)),
	})
	tracer.RecvRPC(mixedRPC())
	_ = tracer.Close()
	line := output.String()
	if !strings.Contains(line, "final=true") || logNumber(t, line, "recv_bytes") <= 0 {
		t.Fatalf("final window missing: %s", line)
	}
	if got := strings.Count(line, `msg="gossipsub bandwidth"`); got != 1 {
		t.Fatalf("got %d summary lines, want one", got)
	}
}

type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

func TestSlowLoggerDoesNotBlockCallbacks(t *testing.T) {
	w := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	tracer, registry := newTestTracer(t, Config{
		LogInterval: 50 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(w, nil)),
	})
	t.Cleanup(func() { close(w.release) }) // Release before tracer cleanup waits.
	select {
	case <-w.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("logger did not start")
	}
	done := make(chan struct{})
	go func() {
		tracer.RecvRPC(&pubsub.RPC{})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callback blocked on logger")
	}
	assertMetric(t, registry, "gossipsub_rpcs_total", map[string]string{"event": "recv"}, 1)
}
