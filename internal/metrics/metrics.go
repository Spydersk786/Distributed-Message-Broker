package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Tracks total number of produce requests
	MessagesProduced = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "broker_message_produced_total",
			Help: "The total Number of produce messages",
		},
		[]string{"topic"},
	)

	// Throughput
	BytesWritten = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "broker_bytes_written_total",
			Help: "The total number of bytes written to dist",
		},
		[]string{"topic"},
	)

	ActiveConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "broker_active_connections",
			Help: "The total number of active TCP connections",
		},
	)
)