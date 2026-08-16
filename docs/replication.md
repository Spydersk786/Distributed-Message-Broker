# Replication Model

The broker utilizes a **Pull-Based Replication** strategy, operating completely asynchronously from the client write path.

## Leader/Follower Assignment
Partition roles are assigned via a deterministic hash function: `(Hash(Topic) + PartitionID) % TotalBrokers`. This guarantees that every node in the cluster reaches the exact same conclusion about who owns a partition without needing a central coordinator.

## The Background Replicator
Followers do not wait for Leaders to push data. 
1. **Metadata Sync:** Every 2 seconds, the Replicator worker polls known peers via a custom `CmdMetadataSync` TCP command.
2. **Discovery:** If it discovers a partition where it is a Follower, it creates the underlying folder structure on its local disk.
3. **Synchronization:** It sends a standard `CmdFetch` to the Leader requesting `NextOffset`. 
4. **Append:** If the Leader returns bytes, the Follower bypasses the `RoleLeader` write checks and appends the raw bytes directly to its own storage engine.

## Consistency Guarantees
Currently, the broker provides **Eventual Consistency**. Because replication is asynchronous, a Leader will ACK a client immediately after writing to its local disk. If the Leader crashes milliseconds later before Followers can pull the data, that specific message may be lost. (Kafka solves this with `acks=all`, requiring ISR syncs before ACKing—a planned future enhancement).