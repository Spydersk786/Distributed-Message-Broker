# 🏛️ Architecture Overview

The broker is composed of decoupled, concurrent subsystems operating over a shared, lock-protected state. 

## Component Topology

1. **Protocol Server (`internal/protocol`)**
   The entry point. A custom TCP server parsing binary frames. It routes specific command bytes (e.g., `CmdProduce=1`, `CmdFetch=2`) to injected handler functions.
2. **Cluster Manager (`internal/cluster`)**
   Maintains the dynamic peer topology. Broadcasts node state via an Epidemic Gossip Protocol and tracks dead peers using a Tombstone map to prevent ghost flapping.
3. **Topic Manager (`internal/topic`)**
   The logical routing layer. Translates a logical `Topic` + `PartitionID` request into physical disk mapping. It executes deterministic hashing `(Hash(Topic) + Partition) % TotalBrokers` to designate `RoleLeader` or `RoleFollower`.
4. **Storage Engine (`internal/storage`)**
   The physical boundary. Agnostic to network and clustering, it safely controls concurrent I/O to physical `.log` and `.index` segment files.

## Request Lifecycle: Smart Proxy Produce

To simplify client SDKs, the broker acts as a smart gateway.

```mermaid
sequenceDiagram
    participant Client
    participant Broker1 as Broker 1 (Proxy)
    participant Broker2 as Broker 2 (Leader)
    participant Disk as B2 Disk
    
    Client->>Broker1: TCP Produce (Topic: "orders", PID: 1)
    Broker1->>Broker1: Hash("orders")+1 % 3 = Broker 2
    Broker1->>Broker2: Forward Raw TCP Payload
    Broker2->>Disk: Append to .log & .index
    Disk-->>Broker2: Return Offset
    Broker2-->>Broker1: Return ACK
    Broker1-->>Client: Return ACK
```