package main

import (
    "github.com/Spydersk786/broker/internal/network"
)

func main() {
    server := network.NewServer(":8090")
    server.Start()
}