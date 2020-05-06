use std::cmp;
use std::error::Error;
use std::mem;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use ::shared_memory::*;
use serde_derive::{Deserialize, Serialize};
use signal_hook::{iterator::Signals, SIGHUP, SIGINT, SIGQUIT, SIGTERM};

pub const MAX_ROWS: usize = 262_144;
pub const MAX_ROW_SIZE: usize = 524_288;
pub const MAX_SLOTS: usize = 8192;
pub const MAX_SLOT_SIZE: usize = 65536;

#[derive(Default, Copy, Clone, PartialEq, Debug, SharedMemCast)]
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

#[derive(SharedMemCast)]
pub struct Index {
    pub first_row_index: usize,
    pub end_row_index: usize,
    pub rows: [RowIndex; MAX_ROWS],
}

#[derive(SharedMemCast)]
pub struct Slot {
    pub data: [u8; MAX_SLOT_SIZE],
}

pub static SHMEM_FILE_NAME: &'static str = "middlerim-queue";

const LOCK_ID_INDEX: usize = 0;

#[inline]
fn get_lock_id_slot(slot_index: usize) -> usize {
    1 + slot_index
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ShmemConfig {
    pub data_dir: String,
}

fn open_linked(cfg: &ShmemConfig) -> Result<Box<SharedMem>, Box<dyn Error>> {
    match SharedMem::open_linked(format!("{}/{}", &cfg.data_dir, SHMEM_FILE_NAME)) {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn writer_context(cfg: &ShmemConfig) -> Result<Box<SharedMem>, Box<dyn Error>> {
    let size_of_index = mem::size_of::<Index>();
    let size_of_slot = mem::size_of::<Slot>();

    let mut underlying_cfg = SharedMemConf::default()
        .set_link_path(format!("{}/{}", &cfg.data_dir, SHMEM_FILE_NAME))
        .set_size(size_of_index + size_of_slot * MAX_SLOTS)
        // Index
        .add_lock(LockType::RwLock, 0, size_of_index)?;
    for x in 0..MAX_SLOTS {
        underlying_cfg = underlying_cfg.add_lock(
            LockType::RwLock,
            size_of_index + x * size_of_slot,
            size_of_slot,
        )?;
    }
    match underlying_cfg.create() {
        Ok(v) => Ok(Box::new(v)),
        Err(SharedMemError::LinkExists) => open_linked(cfg),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn reader_context<'a>(cfg: &'a ShmemConfig) -> Result<Box<SharedMem>, Box<dyn Error>> {
    open_linked(cfg)
}

pub struct ShmemService {
    pub shmem: Box<SharedMem>,
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
    pub fn new(shmem: Box<SharedMem>) -> Box<ShmemService> {
        println!("shmem info: {}", shmem);
        let v = Box::new(ShmemService {
            shmem: shmem,
            closing: Arc::new(AtomicBool::new(false)),
        });
        let closing = v.closing.clone();
        let signals = Signals::new(&[SIGHUP, SIGINT, SIGQUIT, SIGTERM]).unwrap();
        thread::spawn(move || {
            for _ in signals.forever() {
                closing.store(true, Ordering::SeqCst);
                on_killed();
            }
        });
        v
    }

    #[inline]
    fn ensure_process_not_killed(&self) -> () {
        if self.closing.load(Ordering::Relaxed) {
            on_killed();
        }
    }

    pub fn write_index<R, F>(&mut self, f: F) -> Result<R, Box<dyn Error>>
        where F: FnOnce(&mut WriteLockGuard<Index>) -> R,
    {
        self.ensure_process_not_killed();
        let data = &mut self.shmem.wlock::<Index>(LOCK_ID_INDEX)?;
        Ok(f(data))
    }

    pub fn write_slot<R, F>(&mut self, slot_index: usize, f: F) -> Result<R, Box<dyn Error>>
        where F: FnOnce(&mut WriteLockGuard<Slot>) -> R,
    {
        self.ensure_process_not_killed();
        let data = &mut self.shmem.wlock::<Slot>(get_lock_id_slot(slot_index))?;
        Ok(f(data))
    }

    pub fn read_index<R, F>(&self, f: F) -> Result<R, Box<dyn Error>>
        where F: FnOnce(&ReadLockGuard<Index>) -> R,
    {
        self.ensure_process_not_killed();
        let data = &self.shmem.rlock::<Index>(LOCK_ID_INDEX)?;
        Ok(f(data))
    }

    pub fn read_slot<R, F>(&self, slot_index: usize, f: F) -> Result<R, Box<dyn Error>>
        where F: FnOnce(&ReadLockGuard<Slot>) -> R,
    {
        self.ensure_process_not_killed();
        let data = &self.shmem.rlock::<Slot>(get_lock_id_slot(slot_index))?;
        Ok(f(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA_DIR: &str = "../data";
    const CACHE: Option<Box<ShmemService>> = Option::None;

    #[inline]
    fn get_shmem_service() -> Box<ShmemService> {
        if CACHE.is_some() {
            CACHE.unwrap()
        } else {
            let ctx = writer_context(&ShmemConfig {
                data_dir: String::from(DATA_DIR),
            }).unwrap();
            ShmemService::new(ctx)
        }
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
