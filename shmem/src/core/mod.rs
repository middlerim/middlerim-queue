use std::error::Error;
use std::mem;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use raw_sync::locks::*;
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

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ShmemConfig {
    pub data_dir: String,
}

fn get_map_size() -> usize {
    get_slot_offset(MAX_SLOTS + 1)
}

fn open_linked(cfg: &ShmemConfig) -> Result<Box<Shmem>, Box<dyn Error>> {
    match ShmemConf::new()
        .flink(format!("{}/{}", &cfg.data_dir, SHMEM_FILE_NAME)).open() {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn writer_context(cfg: &ShmemConfig) -> Result<Box<Shmem>, Box<dyn Error>> {
    match ShmemConf::new()
        .size(get_map_size())
        .flink(format!("{}/{}", &cfg.data_dir, SHMEM_FILE_NAME))
        .create() {
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

    pub fn write_index<R, F>(&mut self, f: F) -> Result<R, Box<dyn Error>>
        where F: FnOnce(&mut Index) -> R,
    {
        self.ensure_process_not_killed();
        let lock_ptr_index = self.shmem.as_ptr();

        let (mutex, _) = unsafe {
            Mutex::new(lock_ptr_index, lock_ptr_index.add(SIZE_OF_META))?
        };
        let guard = mutex.lock()?;
        let data = unsafe { &mut *(*guard as *mut Index) };
        Ok(f(data))
    }

    pub fn write_slot<R, F>(&mut self, slot_index: usize, f: F) -> Result<R, Box<dyn Error>>
        where F: FnOnce(&mut Slot) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = self.shmem.as_ptr();
        let (mutex, _) = unsafe {
            let slot_ptr = get_slot_ptr(base_ptr, slot_index);
            Mutex::new(slot_ptr, slot_ptr.add(SIZE_OF_META))?
        };
        let guard = mutex.lock()?;
        let data = unsafe { &mut *(*guard as *mut Slot) };
        Ok(f(data))
    }

    pub fn read_index<R, F>(&self, f: F) -> Result<R, Box<dyn Error>>
        where F: FnOnce(&Index) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = self.shmem.as_ptr();

        let (mutex, _) = unsafe {
            Mutex::new(base_ptr, base_ptr.add(SIZE_OF_META))?
        };
        let guard = mutex.rlock()?;
        let data = unsafe { &*(*guard as *const Index) };
        Ok(f(data))
    }

    pub fn read_slot<R, F>(&self, slot_index: usize, f: F) -> Result<R, Box<dyn Error>>
        where F: FnOnce(&Slot) -> R,
    {
        self.ensure_process_not_killed();
        let base_ptr = self.shmem.as_ptr();
        let (mutex, _) = unsafe {
            let slot_ptr = get_slot_ptr(base_ptr, slot_index);
            Mutex::new(slot_ptr, slot_ptr.add(SIZE_OF_META))?
        };
        let guard = mutex.rlock()?;
        let data = unsafe { &*(*guard as *const Slot) };
        Ok(f(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CACHE: Option<Box<ShmemService>> = Option::None;

    #[inline]
    fn get_shmem_service() -> Box<ShmemService> {
        let config = ShmemConfig {
            data_dir: String::from("../data"),
        };
        CACHE.unwrap_or_else(|| {
            let ctx = writer_context(&config).unwrap();
            ShmemService::new(ctx)
        })
    }

    #[test]
    fn default_row_is_not_equal_to_stored() -> Result<(), Box<dyn Error>> {
        let shmem_service = get_shmem_service();
        let row_index = 0;
        let expected_row: RowIndex = Default::default();
        let actual_row = {
            shmem_service.read_index(|index| {
                index.rows[row_index]
            })?
        };
        assert_ne!(actual_row, expected_row);
        Ok(())
    }

    #[test]
    fn stored_row_can_be_read() -> Result<(), Box<dyn Error>> {
        let mut shmem_service = get_shmem_service();
        let row_index = 0;
        let expected_row = {
            shmem_service.write_index(|index| {
                let row = RowIndex::new(
                    0,
                    3,
                    2,
                    1,
                );
                index.rows[row_index] = row;
                row
            })?
        };
        let actual_row = {
            shmem_service.read_index(|index| {
                index.rows[row_index]
            })?
        };
        assert_eq!(actual_row, expected_row);
        Ok(())
    }


    #[test]
    fn stored_slot_can_be_read() -> Result<(), Box<dyn Error>> {
        let mut shmem_service = get_shmem_service();
        let slot_index = 0;
        let char_index = MAX_SLOT_SIZE - 1;
        let expected_char = b'a';
        {
            shmem_service.write_slot(slot_index, |slot| {
                slot.data[char_index] = expected_char;
            })?
        };
        let actual_char_0 = {
            shmem_service.read_slot(slot_index, |slot| {
                slot.data[char_index]
            })?
        };
        assert_eq!(actual_char_0, expected_char);
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
        let actual_char_0 = {
            shmem_service.read_slot(0, |slot| {
                slot.data[char_index]
            })?
        };
        assert_eq!(actual_char_0, expected_char_0);
        // Assert value at slot#1
        let actual_char_1 = {
            shmem_service.read_slot(1, |slot| {
                slot.data[char_index]
            })?
        };
        assert_eq!(actual_char_1, expected_char_1);
        Ok(())
    }

    // --- Row sizes

    #[test]
    fn row_size_same_slot_1() -> Result<(), Box<dyn Error>> {
        let row = RowIndex::new(
            1,
            3,
            1,
            4,
        );
        assert_eq!(row.row_size, 1);
        Ok(())
    }

    #[test]
    fn row_size_same_slot_max() -> Result<(), Box<dyn Error>> {
        let row = RowIndex::new(
            1,
            3,
            1,
            MAX_SLOT_SIZE,
        );
        assert_eq!(row.row_size, MAX_SLOT_SIZE - 3);
        Ok(())
    }

    #[test]
    fn row_size_next_slot_1() -> Result<(), Box<dyn Error>> {
        let row = RowIndex::new(
            1,
            3,
            2,
            1,
        );
        assert_eq!(row.row_size, MAX_SLOT_SIZE - 3 + 1);
        Ok(())
    }

    #[test]
    fn row_size_large() -> Result<(), Box<dyn Error>> {
        let row = RowIndex::new(
            1,
            0,
            100,
            MAX_SLOT_SIZE,
        );
        assert_eq!(row.row_size, MAX_SLOT_SIZE * 100);
        Ok(())
    }
}
