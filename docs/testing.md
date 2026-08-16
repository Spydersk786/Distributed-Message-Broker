# 🧪 Testing Strategy

The architecture is validated via a comprehensive End-to-End (E2E) Integration Test Suite (`pkg/client/main.go`).

### E2E Integration Suite

The client connects explicitly to **Broker 1** and executes a multi-phase validation against the live Docker cluster:

1. **Distributed Routing Validation:** Iteratively produces payloads targeting Partitions `0, 1, and 2`. Validates that the Smart Proxy intercepts, routes to Broker 2 and Broker 3, and returns valid ACKs.
2. **Distributed Persistence:** Fetches the payloads back across all partitions, proving the raw bytes survived the proxy hop and were flushed to the correct Leader's disk.
3. **Internal State Tracking:** Commits offset `42` for a mock consumer group across all three partitions, then retrieves them. This validates the `__consumer_offsets` hashing algorithm.
4. **Epidemic Gossip Injection:** Manually injects a `CmdGossip` frame to verify the Cluster Manager successfully parses and updates its peer map from raw byte streams.

**Replication Verification:** Validated observationally. Post-test execution, cluster logs explicitly show Followers executing `CmdMetadataSync`, dynamically provisioning folders, and pulling the integration test payloads.