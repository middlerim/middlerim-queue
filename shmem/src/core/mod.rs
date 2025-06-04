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
// Removed: use signal_hook::consts::signal::*;
use signal_hook::iterator::Signals;
use libc::{SIGHUP, SIGINT, SIGQUIT, SIGTERM}; // Added libc imports

// Import the new error type
use crate::ShmemLibError;

#[cfg(not(test))]
pub const MAX_ROWS: usize = 262_144;
#[cfg(test)]
pub const MAX_ROWS: usize = 1200; // Stays for Index struct, ShmemConfig.max_rows is runtime limit

pub const COMPILE_TIME_MAX_SLOT_SIZE: usize = 65536;


#[derive(Default, Copy, Clone, PartialEq, Debug)]
pub struct RowIndex {
    pub start_slot_index: usize,
    pub start_data_index: usize,
    pub end_slot_index: usize,
    pub end_data_index: usize,
    pub row_size: usize,
}

impl RowIndex {
    pub fn new(
        start_slot_index: usize,
        start_data_index: usize,
        end_slot_index: usize,
        end_data_index: usize,
        max_slot_size_param: usize,
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
                max_slot_size_param,
            ),
        }
    }

    #[inline]
    fn get_row_size(
        start_slot_index: usize,
        start_data_index: usize,
        end_slot_index: usize,
        end_data_index: usize,
        max_slot_size_param: usize,
    ) -> usize {
        debug_assert!(end_slot_index >= start_slot_index);
        debug_assert!(end_data_index != 0);
        debug_assert!(max_slot_size_param > 0);

        if end_slot_index == start_slot_index {
            debug_assert!(end_data_index > start_data_index);
            end_data_index - start_data_index
        } else {
            (end_slot_index - start_slot_index) * max_slot_size_param - start_data_index + end_data_index
        }
    }

    #[allow(dead_code)] // Method is unused as per warning
    fn is_empty(self) -> bool {
        self.end_data_index == 0
    }
}

pub struct Index {
    pub first_row_index: usize,
    pub end_row_index: usize,
    pub count: usize,
    pub rows: [RowIndex; MAX_ROWS],
}

pub struct Slot {
    pub data: [u8; COMPILE_TIME_MAX_SLOT_SIZE],
}

pub static SHMEM_FILE_NAME: &'static str = "middlerim-queue";

const SIZE_OF_META: usize = 128;
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


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShmemConfig {
    pub data_dir: Option<String>,
    pub shmem_file_name: String,
    #[serde(default)]
    pub use_flink_backing: bool,
    pub max_rows: usize,
    pub max_row_size: usize,
    pub max_slots: usize,
    pub max_slot_size: usize,
}

#[derive(Default)]
pub struct ShmemConfigBuilder {
    data_dir: Option<String>,
    shmem_file_name: Option<String>,
    use_flink_backing: Option<bool>,
    max_rows: Option<usize>,
    max_row_size: Option<usize>,
    max_slots: Option<usize>,
    max_slot_size: Option<usize>,
}

impl ShmemConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn data_dir(mut self, path: String) -> Self { self.data_dir = Some(path); self }
    pub fn shmem_file_name(mut self, name: String) -> Self { self.shmem_file_name = Some(name); self }
    pub fn use_flink_backing(mut self, use_flink: bool) -> Self { self.use_flink_backing = Some(use_flink); self }
    pub fn max_rows(mut self, count: usize) -> Self { self.max_rows = Some(count); self }
    pub fn max_row_size(mut self, size: usize) -> Self { self.max_row_size = Some(size); self }
    pub fn max_slots(mut self, count: usize) -> Self { self.max_slots = Some(count); self }
    pub fn max_slot_size(mut self, size: usize) -> Self { self.max_slot_size = Some(size); self }

    pub fn build(self) -> Result<ShmemConfig, ShmemLibError> {
        let use_flink_backing = self.use_flink_backing.unwrap_or(false);
        let _data_dir_check = self.data_dir.clone();

        let config = ShmemConfig {
            data_dir: self.data_dir,
            shmem_file_name: self.shmem_file_name.unwrap_or_else(|| SHMEM_FILE_NAME.to_string()),
            use_flink_backing,
            max_rows: self.max_rows.unwrap_or(MAX_ROWS),
            max_row_size: self.max_row_size.unwrap_or(524_288),
            max_slots: self.max_slots.unwrap_or(256),
            max_slot_size: self.max_slot_size.unwrap_or(COMPILE_TIME_MAX_SLOT_SIZE),
        };

        if !config.use_flink_backing && _data_dir_check.is_none() {
            return Err(ShmemLibError::Logic(
                "data_dir must be specified when not using flink_backing".to_string()
            ));
        }
        if config.max_rows == 0 || config.max_rows > MAX_ROWS {
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
        if config.max_row_size < config.max_slot_size / 2 && config.max_slots > 1 {
            // This is more of a warning/lint
        }
        Ok(config)
    }
}

impl ShmemConfig {
    pub fn builder() -> ShmemConfigBuilder {
        ShmemConfigBuilder::new()
    }
}

impl Default for ShmemConfig {
    fn default() -> Self {
        // Default behavior is os_id based, requiring data_dir.
        // Users must override if flink_backing is desired or if a different data_dir is needed.
        ShmemConfig {
            data_dir: Some(".".to_string()),
            shmem_file_name: SHMEM_FILE_NAME.to_string(),
            use_flink_backing: false,
            max_rows: 262_144,
            max_row_size: 524_288,
            max_slots: 256,
            max_slot_size: COMPILE_TIME_MAX_SLOT_SIZE,
        }
    }
}

fn get_map_size(cfg: &ShmemConfig) -> usize {
    get_slot_offset(cfg.max_slots + 1)
}

pub fn writer_context(config: &ShmemConfig) -> Result<Box<Shmem>, ShmemLibError> {
    let map_size = get_map_size(config);
    if config.use_flink_backing { // FLINK PATH
        let shmem_path = std::path::Path::new(&config.shmem_file_name);
        if let Some(parent_dir) = shmem_path.parent() {
            eprintln!("[Rust Core writer_context] Flink path: {}", shmem_path.display());
            eprintln!("[Rust Core writer_context] Checking parent directory: {}", parent_dir.display());
            if !parent_dir.exists() {
                eprintln!("[Rust Core writer_context] Parent directory DOES NOT EXIST before create_dir_all.");
                match std::fs::create_dir_all(parent_dir) {
                    Ok(()) => eprintln!("[Rust Core writer_context] create_dir_all successful for {}", parent_dir.display()),
                    Err(e) => {
                        eprintln!("[Rust Core writer_context] create_dir_all FAILED for {}: {}", parent_dir.display(), e);
                        return Err(ShmemLibError::Logic(format!(
                            "Failed to create parent directory {:?} for flink shmem file: {}",
                            parent_dir, e
                        )));
                    }
                }
            } else {
                eprintln!("[Rust Core writer_context] Parent directory already exists before create_dir_all.");
            }
            if parent_dir.exists() {
                eprintln!("[Rust Core writer_context] Parent directory EXISTS after create_dir_all check/attempt.");
            } else {
                eprintln!("[Rust Core writer_context] Parent directory STILL DOES NOT EXIST after create_dir_all.");
                return Err(ShmemLibError::Logic(format!(
                    "Parent directory {:?} for flink shmem file does not exist and could not be created.",
                    parent_dir
                )));
            }
        } else {
             eprintln!("[Rust Core writer_context] Could not get parent directory for shmem_flink_path: {}", config.shmem_file_name);
             return Err(ShmemLibError::Logic(format!(
                "Invalid shmem_flink_path, cannot determine parent directory: {}",
                config.shmem_file_name
            )));
        }

        match std::fs::remove_file(&config.shmem_file_name) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(ShmemLibError::Logic(format!(
                    "Failed to remove existing shmem file {}: {}",
                    &config.shmem_file_name, e
                )));
            }
        }
        ShmemConf::new()
            .size(map_size)
            .flink(&config.shmem_file_name)
            .create()
            .map(Box::new)
            .map_err(ShmemLibError::SharedMemory)
    } else { // OS_ID PATH (simplified)
        if config.data_dir.is_some() {
             eprintln!("[shmem::core::writer_context] Info: data_dir field was Some for os_id mode, but it's not used as a path prefix by this logic. OS default location will be used for ID {}.", config.shmem_file_name);
        }
        ShmemConf::new()
            .size(map_size)
            .os_id(&config.shmem_file_name)
            .create()
            .map(Box::new)
            .map_err(ShmemLibError::SharedMemory)
    }
}

pub fn reader_context(config: &ShmemConfig) -> Result<Box<Shmem>, ShmemLibError> {
    if config.use_flink_backing { // FLINK PATH
        ShmemConf::new()
            .flink(&config.shmem_file_name)
            .open()
            .map(Box::new)
            .map_err(ShmemLibError::SharedMemory)
    } else { // OS_ID PATH (simplified)
        if config.data_dir.is_some() {
            eprintln!("[shmem::core::reader_context] Info: data_dir field was Some for os_id mode, but it's not used as a path prefix by this logic. OS default location will be used for ID {}.", config.shmem_file_name);
        }
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
    ) -> Result<&mut R, ShmemLibError> {
        let (mutex, _) = unsafe { Mutex::new(base_ptr, base_ptr.add(size_of_data)) }
            .map_err(ShmemLibError::Lock)?;
        if lock {
            let raw_data = mutex.try_lock(Timeout::Val(Duration::from_secs(30)))
                .map_err(ShmemLibError::Lock)?;
            Ok(unsafe { &mut *(*raw_data as *mut R) })
        } else {
            let raw_data = unsafe { mutex.get_inner() };
            Ok(unsafe { &mut *(*raw_data as *mut R) })
        }
    }

    pub fn write_index<R, F>(&mut self, f: F) -> Result<R, ShmemLibError>
    where
        F: FnOnce(&mut Index) -> R,
    {
        self.ensure_process_not_killed();
        let lock_ptr_index = (*self.shmem).as_ptr();
        Ok(f(self.extract_data(lock_ptr_index, SIZE_OF_META, true)?))
    }

    pub fn write_slot<R, F>(&mut self, slot_index: usize, f: F) -> Result<R, ShmemLibError>
    where
        F: FnOnce(&mut Slot) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = (*self.shmem).as_ptr();
        let slot_ptr = unsafe { get_slot_ptr(base_ptr, slot_index) };
        Ok(f(self.extract_data(slot_ptr, SIZE_OF_META, false)?))
    }

    pub fn read_index<R, F>(&self, f: F) -> Result<R, ShmemLibError>
    where
        F: FnOnce(&Index) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = (*self.shmem).as_ptr();
        Ok(f(self.extract_data(base_ptr, SIZE_OF_META, false)?))
    }

    pub fn read_slot<R, F>(&self, slot_index: usize, f: F) -> Result<R, ShmemLibError>
    where
        F: FnOnce(&Slot) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = (*self.shmem).as_ptr();
        let slot_ptr = unsafe { get_slot_ptr(base_ptr, slot_index) };
        Ok(f(self.extract_data(slot_ptr, SIZE_OF_META, false)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use tempfile::{tempdir, TempDir};

    static TEST_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

    #[inline]
    fn get_shmem_service() -> (Box<ShmemService>, TempDir) {
        let test_id = TEST_ID_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
        let temp_dir = tempdir().expect("Failed to create tempdir for test");
        let unique_shmem_file_path = temp_dir.path().join(format!("{}-{}", SHMEM_FILE_NAME, test_id));

        let config = ShmemConfig::builder()
            .shmem_file_name(unique_shmem_file_path.to_str().expect("Path is not valid UTF-8").to_string())
            .use_flink_backing(true)
            .max_rows(MAX_ROWS)
            .max_row_size(524_288)
            .max_slots(256)
            .max_slot_size(COMPILE_TIME_MAX_SLOT_SIZE)
            .build()
            .expect("Failed to build ShmemConfig for test");

        let ctx = writer_context(&config).expect("Failed to create writer_context for test");
        (ShmemService::new(ctx), temp_dir)
    }

    #[test]
    fn is_empty() -> Result<(), ShmemLibError> {
        let (shmem_service, _temp_dir) = get_shmem_service();
        let default_row: RowIndex = Default::default();
        assert!(default_row.is_empty()); // This test uses is_empty
        let actual_row = { shmem_service.read_index(|index| index.rows[0])? };
        assert!(actual_row.is_empty()); // And here
        Ok(())
    }

    #[test]
    fn stored_row_can_be_read() -> Result<(), ShmemLibError> {
        let (mut shmem_service, _temp_dir) = get_shmem_service();
        let row_index = 0;
        let expected_start_data_index = 3;
        let test_config = ShmemConfig::builder()
            .shmem_file_name("test_config_stored_row.ipc".to_string())
            .max_rows(MAX_ROWS)
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
    fn stored_slot_can_be_read() -> Result<(), ShmemLibError> {
        let (mut shmem_service, _temp_dir) = get_shmem_service();
        let slot_index = 0;
        let char_index = COMPILE_TIME_MAX_SLOT_SIZE - 1;
        let expected_chars = [b'a', b'b', b'c'];
        let decoy_chars = [b'x', b'y', b'z'];
        {
            shmem_service.write_slot(slot_index, |slot| {
                for i in 0..decoy_chars.len() {
                    slot.data[i] = decoy_chars[i];
                    slot.data[char_index - i] = decoy_chars[i];
                }
            })?;
            shmem_service.write_slot(slot_index + 1, |slot| {
                for i in 0..expected_chars.len() {
                    slot.data[i] = expected_chars[i];
                    slot.data[char_index - i] = expected_chars[i];
                }
            })?;
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
                    slot.data[0],
                    slot.data[1],
                    slot.data[2],
                    slot.data[char_index],
                    slot.data[char_index - 1],
                    slot.data[char_index - 2],
                ]
            })?
        };
        assert_eq!(
            actual_chars,
            [
                b'a', b'b', b'c',
                b'a', b'b', b'c'
            ]
        );
        Ok(())
    }

    #[test]
    fn use_multiple_slots() -> Result<(), ShmemLibError> {
        let (mut shmem_service, _temp_dir) = get_shmem_service();
        let char_index = COMPILE_TIME_MAX_SLOT_SIZE - 1;
        let expected_char_0 = b'a';
        let expected_char_1 = b'b';
        {
            shmem_service.write_slot(0, |slot| {
                slot.data[char_index] = expected_char_0;
            })?
        };
        {
            shmem_service.write_slot(1, |slot| {
                slot.data[char_index] = expected_char_1;
            })?
        };
        let actual_char_0 = { shmem_service.read_slot(0, |slot| slot.data[char_index])? };
        assert_eq!(actual_char_0, expected_char_0);
        let actual_char_1 = { shmem_service.read_slot(1, |slot| slot.data[char_index])? };
        assert_eq!(actual_char_1, expected_char_1);
        Ok(())
    }

    #[test]
    fn row_size_same_slot_1() -> Result<(), ShmemLibError> {
        let test_config = ShmemConfig::builder()
            .shmem_file_name("dummy_row_test_1.ipc".to_string())
            .max_rows(MAX_ROWS)
            .use_flink_backing(true)
            .build()
            .unwrap();
        let row = RowIndex::new(1, 3, 1, 4, test_config.max_slot_size);
        assert_eq!(row.row_size, 1);
        Ok(())
    }

    #[test]
    fn row_size_same_slot_max() -> Result<(), ShmemLibError> {
        let test_config = ShmemConfig::builder()
            .shmem_file_name("dummy_row_test_2.ipc".to_string())
            .max_rows(MAX_ROWS)
            .use_flink_backing(true)
            .build()
            .unwrap();
        let row = RowIndex::new(1, 3, 1, test_config.max_slot_size, test_config.max_slot_size);
        assert_eq!(row.row_size, test_config.max_slot_size - 3);
        Ok(())
    }

    #[test]
    fn row_size_next_slot_1() -> Result<(), ShmemLibError> {
        let test_config = ShmemConfig::builder()
            .shmem_file_name("dummy_row_test_3.ipc".to_string())
            .max_rows(MAX_ROWS)
            .use_flink_backing(true)
            .build()
            .unwrap();
        let row = RowIndex::new(1, 3, 2, 1, test_config.max_slot_size);
        assert_eq!(row.row_size, test_config.max_slot_size - 3 + 1);
        Ok(())
    }

    #[test]
    fn row_size_large() -> Result<(), ShmemLibError> {
        let test_config = ShmemConfig::builder()
            .shmem_file_name("dummy_row_test_4.ipc".to_string())
            .max_rows(MAX_ROWS)
            .use_flink_backing(true)
            .build()
            .unwrap();
        let row = RowIndex::new(1, 0, 100, test_config.max_slot_size, test_config.max_slot_size);
        assert_eq!(row.row_size, test_config.max_slot_size * 100);
        Ok(())
    }
}
