# Storage Architecture

The broker's storage engine is heavily inspired by Apache Kafka's log-structured design. It prioritizes sequential disk I/O over random access.

## Append-Only Logs & Segments
Data is not stored in a single massive file. A partition folder (e.g., `data/orders-0/`) contains multiple **Segments**.
* A segment is rotated when it reaches a specific size threshold.
* Files are named by their base offset (e.g., `00000000.log`, `00000250.log`).

## Dense Indexing
Every `.log` file has a corresponding `.index` file.
* **Log Record:** `[4-byte Message Size][Raw Bytes]`
* **Index Record:** `[4-byte Relative Offset][4-byte Physical Byte Position]`

**O(1) Reads:**
Because the index entries are a fixed 8 bytes, reading an arbitrary offset requires zero searching:
1. Find the correct segment using binary search on the in-memory segment list.
2. Calculate the index file offset: `(TargetOffset - BaseOffset) * 8 bytes`.
3. Use `os.File.ReadAt` to read the exact 8 bytes from the `.index` file.
4. Extract the physical byte position and size, then use `os.File.ReadAt` to read the exact payload from the `.log` file.

## Persistence & Crash Recovery
On boot, the `TopicManager` scans the `data/` directory. For every partition folder, it parses the `.log` file names to rebuild the segment list in memory. Data is inherently crash-safe because it is strictly append-only; partial or corrupted tail-writes can be truncated on startup.