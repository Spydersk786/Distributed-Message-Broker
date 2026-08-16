# 💾 Storage Architecture

The broker's storage engine heavily mirrors Apache Kafka’s log-structured design, prioritizing sequential disk I/O to achieve high throughput.

## Immutable Append-Only Segments

Data is not stored in a single monolithic file. A partition folder (e.g., `data/orders-0/`) rotates through **Segments** based on size thresholds.

```text
data/orders-0/
 ├── 00000000.log    # Raw message bytes
 ├── 00000000.index  # Dense offset index
 ├── 00000250.log    # Rotated segment starting at offset 250
 └── 00000250.index
```

## O(1) Reads via Dense Indexing

Sequential appends are fast, but random reads are historically slow. The broker solves this using a dense `.index` file containing 8-byte records:

- **[4 bytes]** Relative Offset
- **[4 bytes]** Physical Byte Position in the `.log`

### The Lookup Algorithm

1. Locate the correct segment via binary search on the baseOffset array.
2. Calculate the exact index position: `(TargetOffset - BaseOffset) * 8`.
3. Perform an `os.ReadAt` on the `.index` file to extract the Physical Byte Position.
4. Perform an `os.ReadAt` on the `.log` file to extract the exact message payload.

## Persistence & Crash Recovery

Because records are strictly append-only, the system is inherently crash-resistant. On boot, the `TopicManager` scans the `data/` directory, parses file names, reconstructs the in-memory segment list, and safely resumes the `nextOffset` counter without requiring a complete file scan.