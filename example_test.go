package gossipsubtrace_test

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	trace "github.com/sukunrt/go-gossipsub-trace"
)

func ExampleNew() {
	registry := prometheus.NewRegistry()
	tracer, err := trace.New(trace.Config{
		Registerer:  registry,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		LogInterval: 50 * time.Millisecond,
	})
	if err != nil {
		panic(err)
	}
	defer tracer.Close()

	// Pass this option to pubsub.NewGossipSub(ctx, host, option).
	option := pubsub.WithRawTracer(tracer)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	fmt.Println(option != nil)
	// Output: true
}
