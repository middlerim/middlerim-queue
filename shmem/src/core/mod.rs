use std::error::Error;
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

#[cfg(not(test))]
pub const MAX_ROWS: usize = 262_144;
#[cfg(test)]
pub const MAX_ROWS: usize = 16;

pub const MAX_ROW_SIZE: usize = 524_288;
pub const MAX_SLOTS: usize = 256;
pub const MAX_SLOT_SIZE: usize = 65536;

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
            ),
        }
    }

    #[inline]
    fn get_row_size(
        start_slot_index: usize,
        start_data_index: usize,
        end_slot_index: usize,
        end_data_index: usize,
    ) -> usize {
        debug_assert!(end_slot_index >= start_slot_index);
        debug_assert!(end_data_index != 0);

        if end_slot_index == start_slot_index {
            debug_assert!(end_data_index > start_data_index);
            end_data_index - start_data_index
        } else {
            (end_slot_index - start_slot_index) * MAX_SLOT_SIZE - start_data_index + end_data_index
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
    pub data: [u8; MAX_SLOT_SIZE],
}

pub static SHMEM_FILE_NAME: &'static str = "middlerim-queue";

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
    format!("{}/{}", &cfg.data_dir, SHMEM_FILE_NAME)
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ShmemConfig {
    pub data_dir: String,
}

fn get_map_size() -> usize {
    get_slot_offset(MAX_SLOTS + 1)
}

fn open_linked(cfg: &ShmemConfig) -> Result<Box<Shmem>, Box<dyn Error>> {
    match ShmemConf::new().flink(shmem_file(cfg)).open() {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn writer_context(cfg: &ShmemConfig) -> Result<Box<Shmem>, Box<dyn Error>> {
    match ShmemConf::new()
        .size(get_map_size())
        .flink(shmem_file(cfg))
        .create()
    {
        Ok(v) => Ok(Box::new(v)),
        Err(ShmemError::LinkExists) => open_linked(cfg),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn reader_context<'a>(cfg: &'a ShmemConfig) -> Result<Box<Shmem>, Box<dyn Error>> {
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
    ) -> Result<&mut R, Box<dyn Error>> {
        let (mutex, _) = unsafe { Mutex::new(base_ptr, base_ptr.add(size_of_data))? };
        if lock {
            let raw_data = mutex.try_lock(Timeout::Val(Duration::from_secs(30)))?;
            Ok(unsafe { &mut *(*raw_data as *mut R) })
        } else {
            let raw_data = unsafe { mutex.get_inner() };
            Ok(unsafe { &mut *(*raw_data as *mut R) })
        }
    }

    pub fn write_index<R, F>(&mut self, f: F) -> Result<R, Box<dyn Error>>
    where
        F: FnOnce(&mut Index) -> R,
    {
        self.ensure_process_not_killed();
        let lock_ptr_index = (*self.shmem).as_ptr();
        Ok(f(self.extract_data(lock_ptr_index, SIZE_OF_META, true)?))
    }

    pub fn write_slot<R, F>(&mut self, slot_index: usize, f: F) -> Result<R, Box<dyn Error>>
    where
        F: FnOnce(&mut Slot) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = (*self.shmem).as_ptr();
        let slot_ptr = unsafe { get_slot_ptr(base_ptr, slot_index) };
        Ok(f(self.extract_data(slot_ptr, SIZE_OF_META, false)?))
    }

    pub fn read_index<R, F>(&self, f: F) -> Result<R, Box<dyn Error>>
    where
        F: FnOnce(&Index) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = (*self.shmem).as_ptr();
        Ok(f(self.extract_data(base_ptr, SIZE_OF_META, false)?))
    }

    pub fn read_slot<R, F>(&self, slot_index: usize, f: F) -> Result<R, Box<dyn Error>>
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

    const CACHE: Option<Box<ShmemService>> = Option::None;

    #[inline]
    fn get_shmem_service() -> Box<ShmemService> {
        CACHE.unwrap_or_else(|| {
            let config = ShmemConfig {
                data_dir: String::from("../data"),
            };
            let ctx = writer_context(&config).unwrap();
            ShmemService::new(ctx)
        })
    }

    #[test]
    fn is_empty() -> Result<(), Box<dyn Error>> {
        let shmem_service = get_shmem_service();
        let default_row: RowIndex = Default::default();
        assert!(default_row.is_empty());
        let actual_row = { shmem_service.read_index(|index| index.rows[0])? };
        assert!(actual_row.is_empty());
        Ok(())
    }

    #[test]
    fn stored_row_can_be_read() -> Result<(), Box<dyn Error>> {
        let mut shmem_service = get_shmem_service();
        let row_index = 0;
        let expected_start_data_index = 3;
        let expected_row = {
            shmem_service.write_index(|index| {
                let row = RowIndex::new(0, expected_start_data_index, 2, 1);
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
    fn stored_slot_can_be_read() -> Result<(), Box<dyn Error>> {
        let mut shmem_service = get_shmem_service();
        let slot_index = 0;
        let char_index = MAX_SLOT_SIZE - 1;
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
    fn use_multiple_slots() -> Result<(), Box<dyn Error>> {
        let mut shmem_service = get_shmem_service();
        let char_index = MAX_SLOT_SIZE - 1;
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
    fn row_size_same_slot_1() -> Result<(), Box<dyn Error>> {
        let row = RowIndex::new(1, 3, 1, 4);
        assert_eq!(row.row_size, 1);
        Ok(())
    }

    #[test]
    fn row_size_same_slot_max() -> Result<(), Box<dyn Error>> {
        let row = RowIndex::new(1, 3, 1, MAX_SLOT_SIZE);
        assert_eq!(row.row_size, MAX_SLOT_SIZE - 3);
        Ok(())
    }

    #[test]
    fn row_size_next_slot_1() -> Result<(), Box<dyn Error>> {
        let row = RowIndex::new(1, 3, 2, 1);
        assert_eq!(row.row_size, MAX_SLOT_SIZE - 3 + 1);
        Ok(())
    }

    #[test]
    fn row_size_large() -> Result<(), Box<dyn Error>> {
        let row = RowIndex::new(1, 0, 100, MAX_SLOT_SIZE);
        assert_eq!(row.row_size, MAX_SLOT_SIZE * 100);
        Ok(())
    }
}
