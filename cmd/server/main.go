package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Spydersk786/broker/internal/network"
	"github.com/Spydersk786/broker/internal/protocol"
	"github.com/Spydersk786/broker/internal/topic"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
    dataDir := "data"
    if err := os.MkdirAll(dataDir, 0755); err != nil{
        log.Fatalf("Failed to create Data Directory: %v", err)
    }

    router := protocol.NewRouter()
    topicMgr, err := topic.NewManager(dataDir)
    if err != nil{
        log.Fatalf("Failed to create Topic Manager: %v", err)
    }

    offsetMgr, err := topic.NewOffsetManager(topicMgr)
    if err != nil{
        log.Fatalf("Failed to create Offset Manager: %v", err)
    }

    router.Register(protocol.ProduceCmd, protocol.HandleProduce(topicMgr))
    router.Register(protocol.FetchCmd, protocol.HandleFetch(topicMgr))
    router.Register(protocol.CommitOffset, protocol.HandleCommitOffset(offsetMgr))
    router.Register(protocol.FetchOffset, protocol.HandleFetchOffset(offsetMgr))

    server := network.NewServer(":8090", router)
    mux := http.NewServeMux()

    mux.Handle("/metrics", promhttp.Handler())

    metricsServer := &http.Server{
        Addr: ":2112",
        Handler: mux,
    }

    go func()   {
        fmt.Println("Metrics exposed at http://localhost:2112/metrics")
        if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed{
            log.Fatalf("Metrics server crashed: %v", err)
        }
    }()

    go func ()  {
        if err := server.Start(); err != nil {
            log.Fatalf("Server failed to start %v", err)
        }
    }()

    // see docs/references.md
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop() // Restore default signal behaviour once we are done

    // Block until OS sends a signal, which cancels the context
    <-ctx.Done()
    // Restoring the OS behaviour to the signals before main function ends
    // So a second signal from user can bypass the context timeout
    stop() 

    log.Println("Recieved Shutdown Signal. Shutting down... (Press Ctrl+C again to force quit)")

    shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    log.Println("Shuting down metrics server")
    if err := metricsServer.Shutdown(shutdownCtx); err != nil{
        log.Fatalf("Metrics Server Shutdown failed: %v", err)
    }

    if err := server.Shutdown(shutdownCtx); err != nil{
        log.Fatalf("Server Shutdown failed: %v", err)
    }

    log.Println("Server exited cleanly.")
}