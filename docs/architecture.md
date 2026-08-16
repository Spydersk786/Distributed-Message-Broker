# Architecture Overview

The broker is composed of several decoupled, concurrent subsystems that work together to provide distributed messaging.

## Major Components

1. **Protocol Server (`internal/protocol`)**
   A custom TCP server that parses binary frames. Every request is framed as `[4-byte Size][1-byte Command][Payload]`. It routes commands to the appropriate handler.

2. **Cluster Manager (`internal/cluster`)**
   Maintains the cluster topology. Uses an Epidemic Gossip Protocol to broadcast node state. Maintains a map of active peers and a "graveyard" of tombstones for dead peers to prevent ghost flapping.

3. **Topic Manager (`internal/topic`)**
   The logical routing layer. It maps a logical Topic Name and Partition ID to a physical `Partition` struct. It computes the deterministic hash to assign `RoleLeader` or `RoleFollower`.

4. **Storage Engine (`internal/storage`)**
   The physical layer. Completely agnostic to the distributed system. It takes a directory path and writes/reads raw bytes to `.log` and `.index` segment files.

5. **Offset Manager**
   Handles Consumer Group tracking. It hashes the group name to one of 50 partitions of an internal `__consumer_offsets` topic, guaranteeing offset state is distributed across the cluster.

## Data Flow: Produce

1. Client connects to *any* broker and sends a `Produce` command (Topic, Partition, Payload).
2. The Protocol Handler decodes the binary frame.
3. The Topic Manager calculates `Leader = (Hash(Topic) + Partition) % TotalBrokers`.
4. **Proxy Route:** If `Leader != LocalBrokerID`, the payload is forwarded over a temporary TCP socket to the correct broker, and the ACK is proxied back to the client.
5. **Local Route:** If `Leader == LocalBrokerID`, the bytes are passed to the physical Storage Engine, appended to the active `.log` file, indexed, and an Offset is returned.