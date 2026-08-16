# Design Decisions & Trade-offs

### 1. Smart Broker vs. Smart Client
* **Decision:** Built an in-memory TCP proxy inside the broker to route requests to the correct partition leader.
* **Alternative:** Kafka's approach, where the client fetches metadata and manages a connection pool to talk to specific leaders directly.
* **Trade-off:** Our "Smart Broker" approach vastly simplifies client SDK development and makes the integration test extremely clean. However, it introduces an extra network hop for routed requests, slightly increasing latency and broker CPU load compared to direct client-to-leader communication.

### 2. Deterministic Hashing vs. Consensus (Raft/ZooKeeper)
* **Decision:** Partition leaders are assigned via math (`Hash % N`).
* **Alternative:** Using a Raft-based controller to explicitly assign and store cluster metadata.
* **Trade-off:** Hashing eliminates the massive complexity of building a distributed consensus module, allowing the system to boot instantly. However, if the node count `N` changes (a broker dies or is added), the hash ring breaks. Dynamic failover is impossible without a Controller.

### 3. Pull-Based Replication vs. Push-Based
* **Decision:** Followers actively pull bytes from Leaders.
* **Alternative:** Leaders explicitly open sockets to Followers and push bytes on every write.
* **Trade-off:** Pull-based replication is much safer. If a Follower dies or has a slow disk, a Push-based Leader would block or run out of memory buffering the data. In our Pull-based system, a slow Follower only impacts itself; the Leader's write performance remains unaffected.

### 4. Custom Binary TCP vs. HTTP/REST
* **Decision:** Wrote a raw TCP server using a custom framed binary protocol.
* **Alternative:** gRPC or HTTP JSON.
* **Trade-off:** HTTP overhead (headers, JSON parsing) is unacceptable for a high-throughput storage engine. The custom binary protocol enables near "zero-copy" behavior where we can stream bytes directly from disk to the network card.