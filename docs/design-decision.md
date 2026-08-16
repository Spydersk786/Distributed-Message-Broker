### 5. `docs/design-decisions.md`

```markdown
# 🧠 Design Decisions & Trade-offs

| Component | Decision | Alternative | Trade-off / Rationale |
| :--- | :--- | :--- | :--- |
| **Routing** | **Smart Broker (Proxy)** | Smart Client SDK | Simplifies client logic tremendously by allowing connection to any node. *Trade-off:* Adds one network hop for non-local requests, slightly increasing broker CPU/latency. |
| **Cluster State** | **Deterministic Hashing** | Raft / ZooKeeper | `(Hash + PID) % N` enables instant, zero-coordination leader assignment. *Trade-off:* Cannot dynamically failover if a node dies, as changing `N` breaks the hash ring. |
| **Replication** | **Pull-Based Fetchers** | Push-Based | Followers request data sequentially. If a Follower has a slow disk, it does not block the Leader. *Trade-off:* Slight latency gap between Leader write and Follower sync. |
| **Networking** | **Custom TCP Binary** | HTTP/JSON or gRPC | Zero-overhead binary framing allows near "zero-copy" behavior when reading from disk. *Trade-off:* Harder to debug without a custom client, compared to `curl`. |
| **Dead Peers** | **Tombstone Graveyard** | Direct Deletion | Prevents "Ghost Flapping" where delayed gossip reconstructs dead nodes. *Trade-off:* Requires a background TTL purger to clear memory over time. |