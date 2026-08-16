# 🌩️ Failure Scenarios & Behavior

This document outlines the exact behavior of the cluster under degraded network conditions or node failures based on the *current* implementation.

### 🟢 Broker Crash & Recovery
* **Behavior:** Seamless recovery. The broker reads `.log` and `.index` names from the `data/` directory to reconstruct segment mappings and the exact `nextOffset` pointer. It resumes its role as Leader/Follower automatically.

### 🟡 Follower Failure
* **Behavior:** No cluster impact. Leaders use pull-based replication, so they are entirely unaware of dead followers. When the Follower recovers, it reads its local disk, queries the Leader for the next offset, and automatically catches up.

### 🔴 Leader Failure
* **Behavior:** **Write Availability Loss.** Because leader assignment relies on deterministic math, Followers know they possess the replica data but will *not* dynamically promote themselves to Leader. Writes to that partition will fail until the specific Leader node recovers.

### 🟡 Consumer Group Leader Failure
* **Behavior:** **Offset Commit Loss.** `__consumer_offsets` is replicated to Followers, guaranteeing data durability. However, the `OffsetManager` populates its in-memory map exclusively on boot. If the Leader dies, the group cannot commit offsets until the Leader recovers (or until dynamic map synchronization is implemented).