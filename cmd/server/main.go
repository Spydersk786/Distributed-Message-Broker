package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/Spydersk786/broker/internal/network"
)

func main() {
    server := network.NewServer(":8090")

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

    if err := server.Shutdown(shutdownCtx); err != nil{
        log.Fatalf("Server Shutdown failed: %v", err)
    }

    log.Println("Server exited cleanly.")
}