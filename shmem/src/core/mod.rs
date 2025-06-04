use std::mem;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use raw_sync::locks::*;
use raw_sync::Timeout;
use shared_memory::*;

use serde_derive::{Deserialize, Serialize};
use signal_hook::consts::signal::*;
use signal_hook::iterator::Signals;

// Import the new error type
use crate::ShmemLibError;

#[cfg(not(test))]
pub const MAX_ROWS: usize = 262_144;
#[cfg(test)]
pub const MAX_ROWS: usize = 1200; // Stays for Index struct, ShmemConfig.max_rows is runtime limit

// Global constants below are now fully replaced by values from ShmemConfig
// or COMPILE_TIME_MAX_SLOT_SIZE for struct definitions.

// Compile-time constant for Slot::data array size.
// ShmemConfig.max_slot_size will be a runtime check against this physical size.
pub const COMPILE_TIME_MAX_SLOT_SIZE: usize = 65536; // Made pub for visibility inside pub struct ShmemConfig


#[derive(Default, Copy, Clone, PartialEq, Debug)]
pub struct RowIndex {
    pub start_slot_index: usize,
    // Inclusive
    pub start_data_index: usize,
    // Inclusive
    pub end_slot_index: usize,
    // Exclusive
    pub end_data_index: usize,
    // Exclusive
    pub row_size: usize,
}

impl RowIndex {
    pub fn new(
        start_slot_index: usize,
        start_data_index: usize,
        end_slot_index: usize,
        end_data_index: usize,
        max_slot_size_param: usize, // New parameter
    ) -> RowIndex {
        RowIndex {
            start_slot_index: start_slot_index,
            start_data_index: start_data_index,
            end_slot_index: end_slot_index,
            end_data_index: end_data_index,
            row_size: RowIndex::get_row_size(
                start_slot_index,
                start_data_index,
                end_slot_index,
                end_data_index,
                max_slot_size_param, // Pass it down
            ),
        }
    }

    #[inline]
    fn get_row_size(
        start_slot_index: usize,
        start_data_index: usize,
        end_slot_index: usize,
        end_data_index: usize,
        max_slot_size_param: usize, // New parameter
    ) -> usize {
        debug_assert!(end_slot_index >= start_slot_index);
        debug_assert!(end_data_index != 0);
        // Ensure max_slot_size_param is not zero to prevent division by zero if it were used that way,
        // though here it's a multiplier. It also must be <= COMPILE_TIME_MAX_SLOT_SIZE.
        // For this calculation, it defines the logical size of a slot.
        debug_assert!(max_slot_size_param > 0);


        if end_slot_index == start_slot_index {
            debug_assert!(end_data_index > start_data_index);
            end_data_index - start_data_index
        } else {
            (end_slot_index - start_slot_index) * max_slot_size_param - start_data_index + end_data_index
        }
    }

    fn is_empty(self) -> bool {
        self.end_data_index == 0
    }
}

pub struct Index {
    pub first_row_index: usize,
    pub end_row_index: usize,
    pub count: usize, // Added count for circular buffer management
    pub rows: [RowIndex; MAX_ROWS],
}

pub struct Slot {
    pub data: [u8; COMPILE_TIME_MAX_SLOT_SIZE], // Use compile-time constant
}

pub static SHMEM_FILE_NAME: &'static str = "middlerim-queue"; // Stays for ShmemConfig default

const SIZE_OF_META: usize = 128; // TODO Use `Mutex::size_of(Some(...))`. At least the meta has the size of pthread_mutex_t.

const SIZE_OF_INDEX: usize = mem::size_of::<Index>();
const SIZE_OF_SLOT: usize = mem::size_of::<Slot>();

#[inline]
fn get_slot_offset(slot_index: usize) -> usize {
    SIZE_OF_META + SIZE_OF_INDEX + ((SIZE_OF_META + SIZE_OF_SLOT) * slot_index)
}

#[inline]
unsafe fn get_slot_ptr(ptr: *mut u8, slot_index: usize) -> *mut u8 {
    ptr.add(get_slot_offset(slot_index))
}

// Removed shmem_file function as path logic is now in writer_context/reader_context

/// Configuration for shared memory setup and behavior.
///
/// This struct holds all configurable parameters for the shared memory queue,
/// including directory paths, file names, and various size limits.
///
/// Instances are typically created using [`ShmemConfig::builder()`].
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShmemConfig {
    /// The directory for OS-specific ID-based shared memory.
    /// Not used if `use_flink_backing` is true.
    pub data_dir: Option<String>,
    /// If `use_flink_backing` is true, this is the full path to the backing file.
    /// Otherwise, this is the OS-specific ID for shared memory.
    pub shmem_file_name: String,
    /// Determines if file-backed shared memory (`flink`) is used.
    /// If true, `shmem_file_name` is a full path, `data_dir` is ignored.
    /// If false, `os_id` based shared memory is used; `shmem_file_name` is an ID,
    /// and `data_dir` is used as the prefix for the ID.
    pub use_flink_backing: bool,
    /// The logical maximum number of rows (messages) the queue can hold.
    /// This must be less than or equal to the compile-time `MAX_ROWS` constant,
    /// which defines the physical array size in the shared memory index.
    pub max_rows: usize,
    /// The maximum size in bytes for a single message (row).
    pub max_row_size: usize,
    /// The logical maximum number of slots available for storing message data.
    /// This, along with `max_slot_size` (specifically `COMPILE_TIME_MAX_SLOT_SIZE`),
    /// determines the total size of the shared memory region.
    pub max_slots: usize,
    /// The logical maximum size in bytes for a single data slot.
    /// Messages larger than this will span multiple slots. This must be less than
    /// or equal to `COMPILE_TIME_MAX_SLOT_SIZE`.
    pub max_slot_size: usize,
}

/// Builder for [`ShmemConfig`].
///
/// Provides a chained interface for setting configuration values.
/// Call [`build()`](ShmemConfigBuilder::build) to construct the `ShmemConfig`.
/// If a field is not explicitly set, a default value will be used.
///
/// # Example
/// ```
/// use shmem::core::{ShmemConfig, MAX_ROWS, COMPILE_TIME_MAX_SLOT_SIZE};
/// # use shmem::ShmemLibError;
///
/// # fn main() -> Result<(), ShmemLibError> {
/// let config = ShmemConfig::builder()
///     .data_dir("/tmp/my_shmem".to_string())
///     .shmem_file_name("my_queue.ipc".to_string())
///     .max_rows(1000) // Must be <= compile-time MAX_ROWS
///     .max_row_size(1024 * 1024) // 1MB max message size
///     .max_slots(128)
///     .max_slot_size(65536) // Must be <= COMPILE_TIME_MAX_SLOT_SIZE
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Default)] // Default for builder sets all options to None
pub struct ShmemConfigBuilder {
    data_dir: Option<String>,
    shmem_file_name: Option<String>,
    use_flink_backing: Option<bool>, // Changed to Option<bool>
    max_rows: Option<usize>,
    max_row_size: Option<usize>,
    max_slots: Option<usize>,
    max_slot_size: Option<usize>,
}

impl ShmemConfigBuilder {
    /// Creates a new `ShmemConfigBuilder` with all fields unset (will use defaults).
    pub fn new() -> Self {
        Self::default() // Relies on derive(Default) for ShmemConfigBuilder
    }

    /// Sets the directory for OS-specific ID-based shared memory.
    /// This is ignored if `use_flink_backing` is true.
    /// Default: `"."` if not `use_flink_backing`, otherwise `None`.
    pub fn data_dir(mut self, path: String) -> Self { self.data_dir = Some(path); self }

    /// Sets the shared memory file name or OS ID.
    /// If `use_flink_backing` is true, this should be a full path.
    /// Default: `"middlerim-queue"`.
    pub fn shmem_file_name(mut self, name: String) -> Self { self.shmem_file_name = Some(name); self }

    /// Sets whether to use file-backed shared memory.
    /// If true, `shmem_file_name` must be a full path and `data_dir` is ignored.
    /// If false (default), `os_id` based shared memory is used.
    pub fn use_flink_backing(mut self, use_flink: bool) -> Self { self.use_flink_backing = Some(use_flink); self }

    /// Sets the logical maximum number of rows (messages).
    /// Must be `> 0` and `<= MAX_ROWS` (compile-time constant).
    /// Default: `262_144` (production) or `16` (test).
    pub fn max_rows(mut self, count: usize) -> Self { self.max_rows = Some(count); self }
    /// Sets the maximum size of a single message (row), in bytes.
    /// Must be `> 0`.
    /// Default: `524_288`.
    pub fn max_row_size(mut self, size: usize) -> Self { self.max_row_size = Some(size); self }
    /// Sets the logical maximum number of data slots.
    /// Must be `> 0`.
    /// Default: `256`.
    pub fn max_slots(mut self, count: usize) -> Self { self.max_slots = Some(count); self }
    /// Sets the logical maximum size of a single data slot, in bytes.
    /// Must be `> 0` and `<= COMPILE_TIME_MAX_SLOT_SIZE`.
    /// Default: `COMPILE_TIME_MAX_SLOT_SIZE` (currently 65536).
    pub fn max_slot_size(mut self, size: usize) -> Self { self.max_slot_size = Some(size); self }

    /// Builds the `ShmemConfig` instance.
    ///
    /// Applies default values for any fields not explicitly set and performs validation.
    ///
    /// # Errors
    /// Returns `ShmemLibError::Logic` if any configuration values are invalid
    /// (e.g., zero values for sizes/counts, or values exceeding compile-time limits,
    /// or missing `data_dir` when `use_flink_backing` is false).
    pub fn build(self) -> Result<ShmemConfig, ShmemLibError> {
        let use_flink_backing = self.use_flink_backing.unwrap_or(false);
        let _data_dir_check = self.data_dir.clone(); // Clone to check later, prefixed

        let config = ShmemConfig {
            data_dir: self.data_dir,
            shmem_file_name: self.shmem_file_name.unwrap_or_else(|| SHMEM_FILE_NAME.to_string()),
            use_flink_backing,
            // Use MAX_ROWS directly here to get the context-aware (test/non-test) default.
            max_rows: self.max_rows.unwrap_or(MAX_ROWS),
            max_row_size: self.max_row_size.unwrap_or(524_288),
            max_slots: self.max_slots.unwrap_or(256),
            max_slot_size: self.max_slot_size.unwrap_or(COMPILE_TIME_MAX_SLOT_SIZE),
        };

        // Validations
        if !config.use_flink_backing && _data_dir_check.is_none() { // Use _data_dir_check
            return Err(ShmemLibError::Logic(
                "data_dir must be specified when not using flink_backing".to_string()
            ));
        }
        if config.max_rows == 0 || config.max_rows > MAX_ROWS { // MAX_ROWS is compile-time const
            return Err(ShmemLibError::Logic(format!(
                "Configured max_rows ({}) must be > 0 and <= compile-time MAX_ROWS ({})",
                config.max_rows, MAX_ROWS
            )));
        }
        if config.max_slot_size == 0 || config.max_slot_size > COMPILE_TIME_MAX_SLOT_SIZE {
             return Err(ShmemLibError::Logic(format!(
                "Configured max_slot_size ({}) must be > 0 and <= compile-time COMPILE_TIME_MAX_SLOT_SIZE ({})",
                config.max_slot_size, COMPILE_TIME_MAX_SLOT_SIZE
            )));
        }
        if config.max_row_size == 0 {
            return Err(ShmemLibError::Logic("Configured max_row_size must be > 0".to_string()));
        }
        if config.max_slots == 0 {
            return Err(ShmemLibError::Logic("Configured max_slots must be > 0".to_string()));
        }
        // Ensure max_row_size is at least somewhat plausible compared to slot_size, though complex rows can span slots.
        // This is a basic check. A row could be small but start mid-slot and end mid-slot, spanning 3 slots.
        if config.max_row_size < config.max_slot_size / 2 && config.max_slots > 1 { // Arbitrary heuristic
            // This is more of a warning/lint, not a hard error usually. For now, keep it simple.
        }


        Ok(config)
    }
}

impl ShmemConfig {
    pub fn builder() -> ShmemConfigBuilder {
        ShmemConfigBuilder::new()
    }
}

// Re-adding Default for ShmemConfig as it's needed by ReaderConfig/WriterConfig #[derive(Default)]
// The builder provides a more expressive way to set values, but a basic default is still useful.
impl Default for ShmemConfig {
    fn default() -> Self {
        // Default behavior is os_id based, requiring data_dir.
        // Users must override if flink_backing is desired or if a different data_dir is needed.
        ShmemConfig {
            data_dir: Some(".".to_string()), // Default data_dir for non-flink
            shmem_file_name: SHMEM_FILE_NAME.to_string(),
            use_flink_backing: false,
            max_rows: 262_144, // Default non-test MAX_ROWS
            max_row_size: 524_288,
            max_slots: 256,
            max_slot_size: COMPILE_TIME_MAX_SLOT_SIZE,
        }
    }
}

fn get_map_size(cfg: &ShmemConfig) -> usize {
    // get_slot_offset uses SIZE_OF_SLOT which is based on compile-time COMPILE_TIME_MAX_SLOT_SIZE.
    // So, map size is determined by number of slots (cfg.max_slots) and compile-time slot data size.
    // cfg.max_slot_size is a logical limit for operations, not affecting physical layout here.
    get_slot_offset(cfg.max_slots + 1)
}

pub fn writer_context(config: &ShmemConfig) -> Result<Box<Shmem>, ShmemLibError> {
    let map_size = get_map_size(config);
    if config.use_flink_backing {
        // shmem_file_name is the full path
        // Attempt to remove the file if it already exists
        match std::fs::remove_file(&config.shmem_file_name) { // Use directly
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(ShmemLibError::Logic(format!(
                    "Failed to remove existing shmem file {}: {}",
                    &config.shmem_file_name, e // Use directly
                )));
            }
        }
        ShmemConf::new()
            .size(map_size)
            .flink(&config.shmem_file_name) // Use directly
            .create()
            .map(Box::new)
            .map_err(ShmemLibError::SharedMemory)
    } else {
        // os_id based
        let _data_dir = config.data_dir.as_ref().ok_or_else(|| { // Prefixed
            ShmemLibError::Logic("data_dir is None when use_flink_backing is false".to_string())
        })?;
        // Note: shared_memory crate's os_id() with set_os_id_prefix() might attempt to create
        // the prefix directory. This behavior can be platform-specific or version-specific.
        // For POSIX, it typically creates /prefix/id or /prefixid in /dev/shm.
        // If data_dir is an absolute path intended to *be* the /dev/shm equivalent,
        // then flink is more appropriate. This assumes data_dir is a prefix.
        // If not using flink_backing, shmem_file_name is the ID. data_dir is not used here
        // directly with os_id() but validated in build().
        ShmemConf::new()
            .size(map_size)
            .os_id(&config.shmem_file_name)
            .create()
            .map(Box::new)
            .map_err(ShmemLibError::SharedMemory)
    }
}

pub fn reader_context(config: &ShmemConfig) -> Result<Box<Shmem>, ShmemLibError> {
    if config.use_flink_backing {
        // shmem_file_name is the full path
        ShmemConf::new()
            .flink(&config.shmem_file_name)
            .open()
            .map(Box::new)
            .map_err(ShmemLibError::SharedMemory)
    } else {
        // os_id based
        let _data_dir = config.data_dir.as_ref().ok_or_else(|| { // Prefixed
            ShmemLibError::Logic("data_dir is None when use_flink_backing is false".to_string())
        })?;
        // If not using flink_backing, shmem_file_name is the ID. data_dir is not used here.
        ShmemConf::new()
            .os_id(&config.shmem_file_name)
            .open()
            .map(Box::new)
            .map_err(ShmemLibError::SharedMemory)
    }
}

pub struct ShmemService {
    pub shmem: Box<Shmem>,
    closing: Arc<AtomicBool>,
}

#[inline]
fn on_killed() -> () {
    // println!("The process has been killed."); // Removed
    // wait for completing other I/O threads.
    thread::sleep(Duration::from_secs(3));
    process::exit(0);
}

impl ShmemService {
    pub fn new(shmem: Box<Shmem>) -> Box<ShmemService> {
        let v = Box::new(ShmemService {
            shmem: shmem,
            closing: Arc::new(AtomicBool::new(false)),
        });
        let closing = v.closing.clone();
        // Revert to unwrap for Signals::new as ShmemService::new is not fallible
        let mut signals = Signals::new(&[SIGHUP, SIGINT, SIGQUIT, SIGTERM]).unwrap();
        thread::spawn(move || {
            for _ in signals.forever() {
                closing.store(true, Ordering::SeqCst);
                on_killed();
            }
        });
        v
    }

    pub fn close(&self) -> () {
        self.closing.store(true, Ordering::SeqCst);
    }

    #[inline]
    fn ensure_process_not_killed(&self) -> () {
        if self.closing.load(Ordering::Relaxed) {
            on_killed();
        }
    }

    #[inline]
    fn extract_data<R>(
        &self,
        base_ptr: *mut u8,
        size_of_data: usize,
        lock: bool,
    ) -> Result<&mut R, ShmemLibError> { // Changed to ShmemLibError
        let (mutex, _) = unsafe { Mutex::new(base_ptr, base_ptr.add(size_of_data)) }
            .map_err(ShmemLibError::Lock)?; // e is Box<dyn Error>, map to ShmemLibError::Lock
        if lock {
            let raw_data = mutex.try_lock(Timeout::Val(Duration::from_secs(30)))
                .map_err(ShmemLibError::Lock)?; // e is Box<dyn Error>, map to ShmemLibError::Lock
            Ok(unsafe { &mut *(*raw_data as *mut R) })
        } else {
            let raw_data = unsafe { mutex.get_inner() };
            Ok(unsafe { &mut *(*raw_data as *mut R) })
        }
    }

    pub fn write_index<R, F>(&mut self, f: F) -> Result<R, ShmemLibError> // Changed
    where
        F: FnOnce(&mut Index) -> R,
    {
        self.ensure_process_not_killed();
        let lock_ptr_index = (*self.shmem).as_ptr();
        // extract_data now returns ShmemLibError, use ?
        Ok(f(self.extract_data(lock_ptr_index, SIZE_OF_META, true)?))
    }

    pub fn write_slot<R, F>(&mut self, slot_index: usize, f: F) -> Result<R, ShmemLibError> // Changed
    where
        F: FnOnce(&mut Slot) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = (*self.shmem).as_ptr();
        let slot_ptr = unsafe { get_slot_ptr(base_ptr, slot_index) };
        // Concurrency Note: `lock = false` means this operation is NOT internally serialized
        // for writes to the SAME slot from different threads/processes.
        // - Concurrent calls to `write_slot` for the *same* `slot_index` without
        //   external synchronization can lead to data races and corrupted slot data.
        // - Users MUST ensure that writes to a particular slot are synchronized if multiple
        //   writers might access it (e.g., by using distinct slot indices per writer, or
        //   by implementing an external locking mechanism around calls to `MessageWriter::add`
        //   if it could lead to concurrent `write_slot` for the same slot).
        // - Reading this slot via `read_slot` while a `write_slot` (with lock=false) is in
        //   progress might result in reading partially updated/torn data.
        Ok(f(self.extract_data(slot_ptr, SIZE_OF_META, false)?))
    }

    pub fn read_index<R, F>(&self, f: F) -> Result<R, ShmemLibError> // Changed
    where
        F: FnOnce(&Index) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = (*self.shmem).as_ptr();
        // Concurrency Note: `lock = false` means this operation does NOT acquire a lock on the index.
        // - If a concurrent write to the index (via `write_index`, which IS locked) occurs,
        //   this read might observe an inconsistent state of the `Index` struct (a "torn read"),
        //   reflecting a partially updated view.
        // - For applications requiring a consistent snapshot of the index, external synchronization
        //   or retry mechanisms might be needed if concurrent `write_index` calls are possible
        //   and strict consistency is required for reads.
        Ok(f(self.extract_data(base_ptr, SIZE_OF_META, false)?))
    }

    pub fn read_slot<R, F>(&self, slot_index: usize, f: F) -> Result<R, ShmemLibError> // Changed
    where
        F: FnOnce(&Slot) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = (*self.shmem).as_ptr();
        let slot_ptr = unsafe { get_slot_ptr(base_ptr, slot_index) };
        // Concurrency Note: `lock = false` means this operation does NOT acquire a lock on the slot data.
        // - If a concurrent `write_slot` to the *same* `slot_index` is in progress (even if that
        //   `write_slot` itself isn't properly synchronized externally), this read might see
        //   partially written or inconsistent data (a "torn read").
        // - For applications requiring consistent slot data where concurrent writes to that
        //   slot are possible, external synchronization between writer and reader might be necessary.
        Ok(f(self.extract_data(slot_ptr, SIZE_OF_META, false)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use tempfile::{tempdir, TempDir}; // Import TempDir here

    // Atomic counter for generating unique IDs for shared memory file names
    static TEST_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

    // Helper struct to ensure TempDir is dropped when ShmemService is dropped
    // This is no longer needed if get_shmem_service returns Box<ShmemServiceWithTempDir>
    // or if TempDir is managed directly by the test function scope.
    // For simplicity, get_shmem_service will now also return the TempDir so it can be kept alive.
    // However, the subtask asks for each test to create its own, so get_shmem_service will return Box<ShmemService>
    // and also the TempDir. The test function will be responsible for keeping TempDir alive.
    // Let's make get_shmem_service return a tuple: (Box<ShmemService>, TempDir)
    // This helper will now use flink_backing with a temp directory.

    #[inline]
    fn get_shmem_service() -> (Box<ShmemService>, TempDir) {
        let test_id = TEST_ID_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
        let temp_dir = tempdir().expect("Failed to create tempdir for test");
        let unique_shmem_file_path = temp_dir.path().join(format!("{}-{}", SHMEM_FILE_NAME, test_id));

        // Use the builder for ShmemConfig in tests, configured for flink_backing
        let config = ShmemConfig::builder()
            .shmem_file_name(unique_shmem_file_path.to_str().expect("Path is not valid UTF-8").to_string())
            .use_flink_backing(true) // Enable flink_backing for tests
            // data_dir is not needed for flink_backing
            .max_rows(MAX_ROWS) // Test-specific MAX_ROWS (compile-time const for tests)
            .max_row_size(524_288) // Default or test-specific
            .max_slots(256)        // Default or test-specific
            .max_slot_size(COMPILE_TIME_MAX_SLOT_SIZE) // Default or test-specific
            .build()
            .expect("Failed to build ShmemConfig for test");

        let ctx = writer_context(&config).expect("Failed to create writer_context for test");
        (ShmemService::new(ctx), temp_dir)
    }

    #[test]
    fn is_empty() -> Result<(), ShmemLibError> { // Test results also use ShmemLibError
        let (shmem_service, _temp_dir) = get_shmem_service(); // _temp_dir ensures it's not dropped prematurely
        let default_row: RowIndex = Default::default();
        assert!(default_row.is_empty());
        let actual_row = { shmem_service.read_index(|index| index.rows[0])? };
        assert!(actual_row.is_empty());
        Ok(())
    }

    #[test]
    fn stored_row_can_be_read() -> Result<(), ShmemLibError> { // Test results also use ShmemLibError
        let (mut shmem_service, _temp_dir) = get_shmem_service();
        let row_index = 0;
        let expected_start_data_index = 3;
        // Use builder for config, ensuring test-appropriate max_rows
        let test_config = ShmemConfig::builder()
            .shmem_file_name("test_config_stored_row.ipc".to_string())
            .max_rows(MAX_ROWS) // Use compile-time test MAX_ROWS
            .use_flink_backing(true)
            .build()
            .unwrap();
        let expected_row = {
            shmem_service.write_index(|index| {
                let row = RowIndex::new(0, expected_start_data_index, 2, 1, test_config.max_slot_size);
                index.rows[row_index] = row;
                row
            })?
        };
        let actual_row = { shmem_service.read_index(|index| index.rows[row_index])? };
        assert_eq!(actual_row.start_data_index, expected_start_data_index);
        assert_eq!(actual_row, expected_row);
        Ok(())
    }

    #[test]
    fn stored_slot_can_be_read() -> Result<(), ShmemLibError> { // Test results also use ShmemLibError
        let (mut shmem_service, _temp_dir) = get_shmem_service();
        // let config_for_test = ShmemConfig::default(); // Removed unused variable
        let slot_index = 0;
        // char_index should use COMPILE_TIME_MAX_SLOT_SIZE if tests write to physical end of slot
        // or config_for_test.max_slot_size if tests respect logical limit.
        // Test writes directly to slot.data, which is COMPILE_TIME_MAX_SLOT_SIZE.
        let char_index = COMPILE_TIME_MAX_SLOT_SIZE - 1;
        let expected_chars = [b'a', b'b', b'c'];
        let decoy_chars = [b'x', b'y', b'z'];
        {
            // Add decoy before the target slot
            shmem_service.write_slot(slot_index, |slot| {
                for i in 0..decoy_chars.len() {
                    // Head of the slot
                    slot.data[i] = decoy_chars[i];
                    // Tail of the slot
                    slot.data[char_index - i] = decoy_chars[i];
                }
            })?;
            // Set target slot
            shmem_service.write_slot(slot_index + 1, |slot| {
                for i in 0..expected_chars.len() {
                    // Head of the slot
                    slot.data[i] = expected_chars[i];
                    // Tail of the slot
                    slot.data[char_index - i] = expected_chars[i];
                }
            })?;
            // Add decoy after the taret slot
            shmem_service.write_slot(slot_index + 2, |slot| {
                for i in 0..decoy_chars.len() {
                    slot.data[i] = decoy_chars[i];
                    slot.data[char_index - i] = decoy_chars[i];
                }
            })?;
        };
        let actual_chars = {
            shmem_service.read_slot(slot_index + 1, |slot| {
                [
                    // Head
                    slot.data[0],
                    slot.data[1],
                    slot.data[2],
                    // Tail
                    slot.data[char_index],
                    slot.data[char_index - 1],
                    slot.data[char_index - 2],
                ]
            })?
        };
        assert_eq!(
            actual_chars,
            [
                b'a', b'b', b'c', // Head
                b'a', b'b', b'c' // Tail
            ]
        );
        Ok(())
    }

    #[test]
    fn use_multiple_slots() -> Result<(), ShmemLibError> { // Test results also use ShmemLibError
        let (mut shmem_service, _temp_dir) = get_shmem_service();
        // let config_for_test = ShmemConfig::default(); // Not needed if using COMPILE_TIME
        let char_index = COMPILE_TIME_MAX_SLOT_SIZE - 1; // Use compile-time for direct data access
        let expected_char_0 = b'a';
        let expected_char_1 = b'b';
        // Store value to slot#0
        {
            shmem_service.write_slot(0, |slot| {
                slot.data[char_index] = expected_char_0;
            })?
        };
        // Store value to slot#1
        {
            shmem_service.write_slot(1, |slot| {
                slot.data[char_index] = expected_char_1;
            })?
        };
        // Assert value at slot#0
        let actual_char_0 = { shmem_service.read_slot(0, |slot| slot.data[char_index])? };
        assert_eq!(actual_char_0, expected_char_0);
        // Assert value at slot#1
        let actual_char_1 = { shmem_service.read_slot(1, |slot| slot.data[char_index])? };
        assert_eq!(actual_char_1, expected_char_1);
        Ok(())
    }

    // --- Row sizes

    #[test]
    fn row_size_same_slot_1() -> Result<(), ShmemLibError> { // Test results also use ShmemLibError
        let test_config = ShmemConfig::builder()
            .shmem_file_name("dummy_row_test_1.ipc".to_string())
            .max_rows(MAX_ROWS) // Use compile-time test MAX_ROWS
            // No data_dir needed for flink
            .use_flink_backing(true) // Ensure this is consistent if flink is always used for these tests
            .build()
            .unwrap();
        let row = RowIndex::new(1, 3, 1, 4, test_config.max_slot_size);
        assert_eq!(row.row_size, 1);
        Ok(())
    }

    #[test]
    fn row_size_same_slot_max() -> Result<(), ShmemLibError> { // Test results also use ShmemLibError
        let test_config = ShmemConfig::builder()
            .shmem_file_name("dummy_row_test_2.ipc".to_string())
            .max_rows(MAX_ROWS) // Use compile-time test MAX_ROWS
            .use_flink_backing(true)
            .build()
            .unwrap();
        let row = RowIndex::new(1, 3, 1, test_config.max_slot_size, test_config.max_slot_size);
        assert_eq!(row.row_size, test_config.max_slot_size - 3); // Use test_config
        Ok(())
    }

    #[test]
    fn row_size_next_slot_1() -> Result<(), ShmemLibError> { // Test results also use ShmemLibError
        let test_config = ShmemConfig::builder()
            .shmem_file_name("dummy_row_test_3.ipc".to_string())
            .max_rows(MAX_ROWS) // Use compile-time test MAX_ROWS
            .use_flink_backing(true)
            .build()
            .unwrap();
        let row = RowIndex::new(1, 3, 2, 1, test_config.max_slot_size);
        assert_eq!(row.row_size, test_config.max_slot_size - 3 + 1); // Use test_config
        Ok(())
    }

    #[test]
    fn row_size_large() -> Result<(), ShmemLibError> { // Test results also use ShmemLibError
        let test_config = ShmemConfig::builder()
            .shmem_file_name("dummy_row_test_4.ipc".to_string())
            .max_rows(MAX_ROWS) // Use compile-time test MAX_ROWS
            .use_flink_backing(true)
            .build()
            .unwrap();
        let row = RowIndex::new(1, 0, 100, test_config.max_slot_size, test_config.max_slot_size);
        assert_eq!(row.row_size, test_config.max_slot_size * 100); // Use test_config
        Ok(())
    }
}
