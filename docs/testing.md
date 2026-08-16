# Testing Strategy

The system is validated using a robust End-to-End (E2E) Integration Test Suite located in `pkg/client/main.go`.

### Integration Test Suite (`client.go`)
The client connects *only* to Broker 1 (`localhost:8090`) and executes a gauntlet of distributed commands:

1. **Distributed Produce:** It iteratively produces messages to Partitions `0, 1, and 2`. Because Broker 1 only leads one of these, it verifies that the Smart Proxy intercept-and-forward mechanism functions flawlessly.
2. **Distributed Fetch:** It reads the payloads back from all partitions, proving the raw bytes were persisted on the target leaders.
3. **Consumer Group Management:** It commits offset `42` for a consumer group across all three partitions. It then issues `FetchOffset` commands to verify the `__consumer_offsets` distributed tracking is persisting state correctly.
4. **Gossip Verification:** It manually injects a `CmdGossip` payload to ensure the broker accepts and processes dynamic peer discovery requests.

### Replication Verification
Replication is verified observationally. Running the client test populates data on the Leader. Checking the Docker Compose stdout confirms that Follower brokers log `[Replicator] Synced offset X for integration.test-Y from Broker Z`, proving the background pull workers are discovering and cloning the partitions.