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
pub const MAX_ROWS: usize = 16; // Stays for Index struct, ShmemConfig.max_rows is runtime limit

// Global constants below are now fully replaced by values from ShmemConfig
// or COMPILE_TIME_MAX_SLOT_SIZE for struct definitions.

// Compile-time constant for Slot::data array size.
// ShmemConfig.max_slot_size will be a runtime check against this physical size.
pub(crate) const COMPILE_TIME_MAX_SLOT_SIZE: usize = 65536; // Made pub(crate) for visibility


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

fn shmem_file(cfg: &ShmemConfig) -> String {
    // Use shmem_file_name from config for the actual file name component
    format!("{}/{}", &cfg.data_dir, &cfg.shmem_file_name)
}

#[derive(Debug, Serialize, Deserialize, Clone)] // Added Clone
pub struct ShmemConfig {
    pub data_dir: String,
    pub shmem_file_name: String,
    // New configurable fields
    pub max_rows: usize,
    pub max_row_size: usize,
    pub max_slots: usize,
    pub max_slot_size: usize,
}

impl Default for ShmemConfig {
    fn default() -> Self {
        ShmemConfig {
            data_dir: String::from("."),
            shmem_file_name: String::from(SHMEM_FILE_NAME),
            max_rows: 262_144,
            max_row_size: 524_288, // Default MAX_ROW_SIZE
            max_slots: 256,        // Default MAX_SLOTS
            max_slot_size: COMPILE_TIME_MAX_SLOT_SIZE, // Default uses compile-time const
        }
    }
}

fn get_map_size(cfg: &ShmemConfig) -> usize {
    // get_slot_offset uses SIZE_OF_SLOT which is based on compile-time COMPILE_TIME_MAX_SLOT_SIZE.
    // So, map size is determined by number of slots (cfg.max_slots) and compile-time slot data size.
    // cfg.max_slot_size is a logical limit for operations, not affecting physical layout here.
    get_slot_offset(cfg.max_slots + 1)
}

fn open_linked(cfg: &ShmemConfig) -> Result<Box<Shmem>, ShmemLibError> {
    // Use ? for ShmemError -> ShmemLibError conversion
    Ok(Box::new(ShmemConf::new().flink(shmem_file(cfg)).open()?))
}

pub fn writer_context(cfg: &ShmemConfig) -> Result<Box<Shmem>, ShmemLibError> {
    match ShmemConf::new()
        .size(get_map_size(cfg)) // Pass cfg to get_map_size
        .flink(shmem_file(cfg))
        .create()
    {
        Ok(v) => Ok(Box::new(v)),
        Err(shmem_err) => match shmem_err { // Renamed e to shmem_err for clarity
            shared_memory::ShmemError::LinkExists => open_linked(cfg),
            _ => Err(ShmemLibError::SharedMemory(shmem_err)), // Explicit conversion
        },
    }
}

pub fn reader_context<'a>(cfg: &'a ShmemConfig) -> Result<Box<Shmem>, ShmemLibError> {
    open_linked(cfg)
}

pub struct ShmemService {
    pub shmem: Box<Shmem>,
    closing: Arc<AtomicBool>,
}

#[inline]
fn on_killed() -> () {
    println!("The process has been killed.");
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
        // extract_data now returns ShmemLibError, use ?
        Ok(f(self.extract_data(slot_ptr, SIZE_OF_META, true)?)) // Changed lock to true
    }

    pub fn read_index<R, F>(&self, f: F) -> Result<R, ShmemLibError> // Changed
    where
        F: FnOnce(&Index) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = (*self.shmem).as_ptr();
        // extract_data now returns ShmemLibError, use ?
        Ok(f(self.extract_data(base_ptr, SIZE_OF_META, true)?)) // Changed lock to true
    }

    pub fn read_slot<R, F>(&self, slot_index: usize, f: F) -> Result<R, ShmemLibError> // Changed
    where
        F: FnOnce(&Slot) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = (*self.shmem).as_ptr();
        let slot_ptr = unsafe { get_slot_ptr(base_ptr, slot_index) };
        Ok(f(self.extract_data(slot_ptr, SIZE_OF_META, true)?)) // Changed lock to true
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

    #[inline]
    fn get_shmem_service() -> (Box<ShmemService>, TempDir) {
        let test_id = TEST_ID_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
        let unique_shmem_file_name = format!("{}-{}", SHMEM_FILE_NAME, test_id);

        let temp_dir = tempdir().expect("Failed to create tempdir for test");
        let config = ShmemConfig {
            data_dir: temp_dir.path().to_str().expect("Path is not valid UTF-8").to_string(),
            shmem_file_name: unique_shmem_file_name,
            max_rows: MAX_ROWS, // Use compile-time test MAX_ROWS
            max_row_size: 524_288, // Default
            max_slots: 256,        // Default
            max_slot_size: 65536,  // Default
        };

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
        let config_for_test = ShmemConfig::default(); // Get default config for max_slot_size
        let expected_row = {
            shmem_service.write_index(|index| {
                let row = RowIndex::new(0, expected_start_data_index, 2, 1, config_for_test.max_slot_size);
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
        let config_for_test = ShmemConfig::default();
        let row = RowIndex::new(1, 3, 1, 4, config_for_test.max_slot_size);
        assert_eq!(row.row_size, 1);
        Ok(())
    }

    #[test]
    fn row_size_same_slot_max() -> Result<(), ShmemLibError> { // Test results also use ShmemLibError
        let config_for_test = ShmemConfig::default();
        let row = RowIndex::new(1, 3, 1, config_for_test.max_slot_size, config_for_test.max_slot_size);
        assert_eq!(row.row_size, config_for_test.max_slot_size - 3);
        Ok(())
    }

    #[test]
    fn row_size_next_slot_1() -> Result<(), ShmemLibError> { // Test results also use ShmemLibError
        let config_for_test = ShmemConfig::default();
        let row = RowIndex::new(1, 3, 2, 1, config_for_test.max_slot_size);
        assert_eq!(row.row_size, config_for_test.max_slot_size - 3 + 1);
        Ok(())
    }

    #[test]
    fn row_size_large() -> Result<(), ShmemLibError> { // Test results also use ShmemLibError
        let config_for_test = ShmemConfig::default();
        let row = RowIndex::new(1, 0, 100, config_for_test.max_slot_size, config_for_test.max_slot_size);
        assert_eq!(row.row_size, config_for_test.max_slot_size * 100);
        Ok(())
    }
}
