//! A shared memory queue (`shmem`) for inter-process communication (IPC) or intra-process
//! messaging where high performance is desired.
//!
//! # Overview
//! This crate provides mechanisms to create and interact with a queue that resides in
//! shared memory. It's designed for scenarios where multiple processes or threads need
//! to exchange data efficiently.
//!
//! Key components:
//! - [`ShmemConfig`](core::ShmemConfig): Configuration for setting up the shared memory,
//!   including size parameters and file paths. Use [`ShmemConfig::builder()`](core::ShmemConfig::builder)
//!   to construct.
//! - [`MessageWriter`](writer::MessageWriter): For writing messages to the queue.
//! - [`MessageReader`](reader::MessageReader): For reading messages from the queue, offering
//!   different APIs for various use cases (allocating, user-buffered, zero-copy segments).
//! - [`ShmemLibError`](errors::ShmemLibError): Custom error type for the crate.
//!
//! # Concurrency Model and Safety
//!
//! The core shared memory access is managed by `shmem::core::ShmemService`. Understanding
//! its concurrency model is crucial for safe and correct use:
//!
//! - **Index Writes (`ShmemService::write_index`):** Operations that modify the main queue
//!   index (e.g., allocating a new row for a message) are **internally locked** using a
//!   mutex within the shared memory. This ensures that updates to the queue's metadata
//!   are serialized, preventing corruption of the index itself.
//!
//! - **Slot Writes (`ShmemService::write_slot`):** Writes to individual data slots are
//!   **NOT internally locked** by default for performance reasons. This means:
//!     - If multiple threads/processes attempt to write to the *exact same slot index*
//!       concurrently without external synchronization, data races and corruption WILL occur.
//!     - **`MessageWriter::add()`**: This method is now **internally thread-safe**. It uses
//!       a global lock to ensure that all operations within `add` (including metadata
//!       updates via `write_index` and data writing via `write_slot`) are serialized
//!       across all `MessageWriter` instances and threads. This simplifies concurrent
//!       producer scenarios, as individual calls to `add()` are atomic.
//!     - Custom low-level use of `ShmemService::write_slot` (outside of `MessageWriter::add`)
//!       still requires careful consideration of synchronization if concurrent access to the
//!       same slot is possible.
//!
//! - **Index Reads (`ShmemService::read_index`):** Reads of the main queue index are
//!   **NOT internally locked**. This means a reader might observe a partially updated
//!   (torn) state if a `write_index` operation is concurrently in progress. While this
//!   can improve read performance, applications needing a perfectly consistent snapshot
//!   of the index in the presence of concurrent writes may need to implement their own
//!   retry mechanisms or external synchronization.
//!
//! - **Slot Reads (`ShmemService::read_slot`):** Reads of individual data slots are
//!   **NOT internally locked**. Similar to index reads, this means a `read_slot` operation
//!   might see partially written or inconsistent data if a `write_slot` operation to the
//!   same slot is concurrently in progress. The `MessageReader` methods (`read`,
//!   `read_into_buffer`, `read_segments`) rely on these lock-free slot reads.
//!
//! **User Responsibilities:**
//! - **Writer Synchronization:** If multiple distinct `MessageWriter` instances (or direct
//!   `ShmemService` users) could potentially write data that might map to the *same physical
//!   slot* concurrently, users must implement external synchronization. Typically, the design
//!   of using `write_index` to claim a row should prevent distinct `add` calls from directly
//!   colliding on slots for *new* messages, but care is needed for custom low-level access.
//! - **Reader Consistency:** Readers should be aware of the possibility of torn reads for
//!   both index and slot data. If absolute consistency is paramount for a given read,
//!   external synchronization between writers and readers might be necessary. The zero-copy
//!   `MessageReader::read_segments` API, in particular, exposes segments directly from
//!   shared memory; users of its callback are responsible for validating if the sequence
//!   of segments forms a coherent message if concurrent writes are a concern.
//!   Similarly, direct use of `ShmemService` for reading slot/index data remains lock-free.
//!
//! This model means that while writes via `MessageWriter::add` are serialized and safe,
//! read operations (`MessageReader` methods or direct `ShmemService` reads) can still
//! observe torn data if they occur concurrently with these writes. Users requiring
//! strong read consistency (e.g., ensuring a read operation sees the complete state of a
//! message written by a specific `add` call without any intermediate states) may need
//! to implement external synchronization mechanisms (e.g., semaphores, channels, or
//! application-level versioning/checksums) between their writer and reader logic.

pub mod errors;
pub mod core;
pub mod reader;
pub mod writer;
mod replica;

pub use errors::ShmemLibError;

pub const MAX_ROWS: usize = core::MAX_ROWS; // This is the compile-time array limit
// MAX_ROW_SIZE is now configurable via ShmemConfig, so remove this const export.
// Users should get max_row_size from their ShmemConfig instance.
// pub const MAX_ROW_SIZE: usize = core::MAX_ROW_SIZE;

#[cfg(test)]
mod tests;
