# Write-Ahead Log (WAL) Architecture

The `wal` package provides a durable, append-only log for the Hermes key-value store. It guarantees that mutations are persisted to disk before they are considered committed in memory.

## 1. Core Architecture

The implementation follows the **Actor Model** pattern to manage file I/O without lock contention.

### Data Flow
1. **Producer:** Multiple goroutines call `Append()`.
2. **Channel:** Requests are sent to a single unbuffered channel.
3. **Consumer (Actor):** A dedicated background goroutine (`run`) owns the file handle.
4. **Disk:** The actor writes to the file and calls `fsync`.



This design eliminates the need for `sync.Mutex` locks around file operations and ensures writes are serialized naturally.

## 2. Key Design Decisions

### A. Configurable Durability (Sync Policy)
The WAL supports two durability modes via `SyncPolicy`:
* **SyncEveryWrite (0):** Strong Consistency. Every `Append()` blocks until `fsync` completes. Slow but safe.
* **SyncEverySecond (1s):** Eventual Consistency. Writes are flushed to the OS buffer immediately but `fsync` happens on a 1-second ticker. Improves throughput at the risk of losing 1 second of data on power loss.
* **Flush-on-Close:** Regardless of policy, `Close()` always forces a final `fsync` to ensure graceful shutdowns persist all pending data.

### B. Binary Safety (Base64 Encoding)
The WAL format is text-based but uses Base64 for values.
* **Format:** `SET <key> <base64_value>\n`
* This handles edge cases (newlines, null bytes, whitespace) in user data without complex binary framing logic. It remains human-readable for debugging.

### C. Shutdown Safety (Circuit Breaker)
The `Close()` method uses a `select` with `time.After`.
* If the background worker panics or deadlocks, the main thread will not hang forever waiting for a shutdown signal. It forces a timeout to allow the application to restart gracefully.

### D. Idempotency (`sync.Once`)
We use `sync.Once` to manage the shutdown signal.
* Ensures `Close()` is safe to call multiple times (e.g., from `defer` and explicit calls) without causing a "close of closed channel" panic.

## 3. Package Structure

| File | Responsibility |
| :--- | :--- |
| **`record.go`** | Pure data transformation. Defines the `WALRecord` struct and handles Serialization/Deserialization logic. |
| **`wal.go`** | The public API (`Append`, `Close`, `Replay`). Handles the lifecycle and error propagation. |
| **`worker.go`** | The internal engine. Contains the event loop (`run`) and low-level `os.File` operations. |

## 4. Integration Strategy (Decorator Pattern)

The WAL is integrated via `wal_store.go`, which wraps any in-memory store implementation (`Locked`, `Sharded`, etc.).

* **Write Path:** `WAL Append` (Disk) -> `Memory Write` (RAM).
* **Phantom Write Protection:** Logic checks (e.g., `PutIfAbsent`) are performed **before** appending to the log to prevent failed operations from corrupting the history.
* **Recovery:** On startup, `Replay()` reads the log sequentially and reconstructs the memory state.