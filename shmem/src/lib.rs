use std::error::Error;
use serde_derive::{Serialize, Deserialize};
use ::shared_memory::*;

pub const MAX_ROW_COUNT: usize = 5;
pub const MAX_SLOT_SIZE: usize = 1024 * 1024;

#[derive(SharedMemCast)]
struct Slot {
    data: [u8; MAX_SLOT_SIZE],
}

#[derive(SharedMemCast)]
pub struct Index {
    pub first_row_index: u16,
    pub end_row_index: u16,
    // 0: Start slot index
    // 1: Start index of the data
    // 2: End slot index
    // 3: End index of the data
    pub rows: [[u16; 4]; MAX_ROW_COUNT],
}

pub const SHMEM_SIZE_INDEX: usize = 1000 + (4 * 8 * MAX_ROW_COUNT);

pub static SHMEM_PATH_INDEX: &'static str = "./middlerim-queue-index.link";
pub static SHMEM_PATH_SLOT_PREFIX: &'static str = "middlerim-queue-slot";

const INDEX_LOCK_ID: usize = 0;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ShmemConfig {
    pub path_index: String,
}

fn open_linked<'a>(cfg: &'a ShmemConfig) -> Result<Box<SharedMem>, Box<dyn Error>> {
    match SharedMem::open_linked(&cfg.path_index) {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn writer_context<'a>(cfg: &'a ShmemConfig) -> Result<Box<SharedMem>, Box<dyn Error>> {
    match SharedMem::create_linked(&cfg.path_index, LockType::RwLock, SHMEM_SIZE_INDEX) {
        Ok(v) => Ok(Box::new(v)),
        Err(SharedMemError::LinkExists) => {
            std::fs::remove_file(&cfg.path_index)?;
            open_linked(cfg)
        }
        Err(e) => Err(Box::new(e)),
    }
}

pub fn reader_context<'a>(cfg: &'a ShmemConfig) -> Result<Box<SharedMem>, Box<dyn Error>> {
    open_linked(cfg)
}

pub struct IndexService<'a> {
    pub shmem: &'a mut SharedMem,
}


impl <'a> IndexService<'a> {
    pub fn new(shmem: &'a mut SharedMem) -> IndexService<'a> {
        println!("shmem info: {}", shmem);
        IndexService {
            shmem: shmem,
        }
    }

    pub fn write<R, F>(&mut self, f: F) -> Result<R, SharedMemError>
        where F: FnOnce(WriteLockGuard<Index>) -> R {
        let index = self.shmem.wlock::<Index>(INDEX_LOCK_ID)?;
        Ok(f(index))
    }

    pub fn read<R, F>(&mut self, f: F) -> Result<R, SharedMemError>
        where F: FnOnce(ReadLockGuard<Index>) -> R {
        let index = self.shmem.rlock::<Index>(INDEX_LOCK_ID)?;
        Ok(f(index))
    }
}
