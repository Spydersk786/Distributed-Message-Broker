# Failure Scenarios & Behavior

This document outlines how the *current* implementation behaves under various failure conditions.

### 1. Broker Crash & Recovery
**Behavior:** If a broker crashes and restarts, it scans its local `data/` directory. It reads the `.log` and `.index` files to rebuild its segment map, determining its `NextOffset` natively from disk. It immediately resumes its role as Leader or Follower based on the deterministic hash. No data already synced to disk is lost.

### 2. Follower Failure
**Behavior:** If a Follower crashes, the Leader is entirely unaffected (due to pull-based replication). The Follower's local replica falls behind. When it restarts, it reads its local disk to find its last offset, resumes its background fetch loop, and automatically catches up to the Leader.

### 3. Leader Failure
**Behavior:** *Write Availability Loss.* Because leader assignment relies on deterministic hashing (`Hash % TotalBrokers`), the Followers know they have a copy of the data, but they will *not* dynamically promote themselves to Leader. Writes to that partition will fail with a connection error until the specific Leader broker comes back online.

### 4. Zombie Nodes (Network Partitions)
**Behavior:** If a node goes offline temporarily, the Gossip protocol evicts it. To prevent "Ghost Flapping" (where stale gossip from another node resurrects the dead node), the cluster manager utilizes a **Tombstone** map. Dead peers are marked in the graveyard, guaranteeing they are permanently ignored until a fresh TCP connection proves they are alive.