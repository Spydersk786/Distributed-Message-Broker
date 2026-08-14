package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Spydersk786/broker/internal/cluster"
	"github.com/Spydersk786/broker/internal/network"
	"github.com/Spydersk786/broker/internal/protocol"
	"github.com/Spydersk786/broker/internal/topic"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func getEnv(key string, fallback string) string{
    if value, exists := os.LookupEnv(key); exists{
        return value
    }

    return fallback
}
func main() {
    idStr := getEnv("BROKER_ID", "1")
    nodeID, _ := strconv.Atoi(idStr)

    tcpPort := getEnv("TCP_PORT", "8090")
    metricsPort := getEnv("METRICS_PORT", "2112")
    dataDir := getEnv("DATA_DIR", "data")
    seedEnv := getEnv("SEEDS", "")

    var seedNodes []string
    if seedEnv != ""{
        seedNodes = strings.Split(seedEnv, ",")
    }

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

    host := getEnv("HOSTNAME", "localhost")
    localAddr := fmt.Sprintf("%s:%s", host, tcpPort)

    clusterMgr, err := cluster.NewClusterManager(uint32(nodeID), localAddr)
    if err != nil{
        log.Fatalf("Failed to create Offset Manager: %v", err)
    }

    router.Register(protocol.ProduceCmd, protocol.HandleProduce(topicMgr))
    router.Register(protocol.FetchCmd, protocol.HandleFetch(topicMgr))
    router.Register(protocol.CommitOffset, protocol.HandleCommitOffset(offsetMgr))
    router.Register(protocol.FetchOffset, protocol.HandleFetchOffset(offsetMgr))
    router.Register(protocol.GossipCmd, cluster.HandleGossip(clusterMgr))

    server := network.NewServer(":"+tcpPort, router)
    mux := http.NewServeMux()

    mux.Handle("/metrics", promhttp.Handler())

    metricsServer := &http.Server{
        Addr: ":"+metricsPort,
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

    log.Println("Starting background Gossip worker...")
    clusterMgr.StartGossip(ctx, seedNodes)
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

    log.Println("Closing disk Storage...")
    if err := topicMgr.Close(); err != nil{
        log.Printf("Storage teardown finished with error: %v", err)
    }else{
        log.Println("Storage safely closed.")
    }

    log.Println("Server exited cleanly.")
}