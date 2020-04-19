use ::shared_memory::*;
use serde_derive::{Deserialize, Serialize};
use std::cmp;
use std::error::Error;

pub const MAX_ROWS: usize = 65536;
pub const MAX_ROW_SIZE: usize = 524_288;
pub const MAX_SLOTS: usize = 256;
pub const MAX_SLOT_SIZE: usize = 65536;

#[derive(Default, Copy, Clone, Debug, SharedMemCast)]
pub struct RowIndex {
    pub start_slot_index: u8,
    pub start_data_index: usize,
    pub end_slot_index: u8,
    pub end_data_index: usize,
    pub row_size: usize,
}

impl RowIndex {
    pub fn new(
        start_slot_index: u8,
        start_data_index: usize,
        end_slot_index: u8,
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
        start_slot_index: u8,
        start_data_index: usize,
        end_slot_index: u8,
        end_data_index: usize,
    ) -> usize {
        assert!(end_slot_index >= start_slot_index);

        if end_slot_index == start_slot_index {
            cmp::max(0, end_data_index - start_data_index)
        } else {
            MAX_SLOT_SIZE - start_data_index
            + (end_slot_index - start_slot_index) as usize * MAX_SLOT_SIZE
            + end_data_index
        }
    }
}

#[derive(SharedMemCast)]
pub struct Index {
    pub first_row_index: u16, // 0..65536
    pub end_row_index: u16,   // 0..65536
    pub rows: [RowIndex; MAX_ROWS],
}

#[derive(SharedMemCast)]
pub struct Slot {
    pub data: [u8; MAX_SLOT_SIZE],
}

pub const SHMEM_SIZE_INDEX: usize = 16 + 16 + (16 * 4 * MAX_ROWS);
pub const SHMEM_SIZE_SLOT: usize = 8 * MAX_SLOT_SIZE;

pub static SHMEM_FILE_NAME: &'static str = "middlerim-queue";

const LOCK_ID_INDEX: usize = 0;

#[inline]
fn get_lock_id_slot(slot_index: u8) -> usize {
    1 + slot_index as usize
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ShmemConfig {
    pub data_dir: String,
}

fn open_linked<'a>(cfg: &'a ShmemConfig) -> Result<Box<SharedMem>, Box<dyn Error>> {
    match SharedMem::open_linked(format!("{}/{}", &cfg.data_dir, SHMEM_FILE_NAME)) {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn writer_context<'a>(cfg: &'a ShmemConfig) -> Result<Box<SharedMem>, Box<dyn Error>> {
    let mut underlying_cfg = Box::new(
        SharedMemConf::default()
            .set_link_path(format!("{}/{}", &cfg.data_dir, SHMEM_FILE_NAME))
            .set_size(SHMEM_SIZE_INDEX + SHMEM_SIZE_SLOT * MAX_SLOTS)
            // Index
            .add_lock(LockType::RwLock, 0, SHMEM_SIZE_INDEX)?,
    );
    for x in 0..MAX_SLOTS {
        underlying_cfg = Box::new(underlying_cfg.add_lock(
            LockType::RwLock,
            SHMEM_SIZE_INDEX + x * SHMEM_SIZE_SLOT,
            SHMEM_SIZE_SLOT,
        )?);
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

pub struct ShmemService<'a> {
    pub shmem: &'a mut SharedMem,
}

impl<'a> ShmemService<'a> {
    pub fn new(shmem: &'a mut SharedMem) -> ShmemService<'a> {
        println!("shmem info: {}", shmem);
        ShmemService { shmem: shmem }
    }

    pub fn write_index<R, F>(&mut self, f: F) -> Result<R, Box<dyn Error>>
    where
        F: FnOnce(&mut WriteLockGuard<Index>) -> R,
    {
        let mem = &mut self.shmem.wlock::<Index>(LOCK_ID_INDEX)?;
        Ok(f(mem))
    }

    pub fn write_slot<R, F>(&mut self, slot_index: u8, f: F) -> Result<R, Box<dyn Error>>
    where
        F: FnOnce(&mut WriteLockGuard<Slot>) -> R,
    {
        let mem = &mut self.shmem.wlock::<Slot>(get_lock_id_slot(slot_index))?;
        Ok(f(mem))
    }

    pub fn read_index<R, F>(&mut self, f: F) -> Result<R, Box<dyn Error>>
    where
        F: FnOnce(&ReadLockGuard<Index>) -> R,
    {
        let mem = &self.shmem.rlock::<Index>(LOCK_ID_INDEX)?;
        Ok(f(mem))
    }

    pub fn read_slot<R, F>(&mut self, slot_index: u8, f: F) -> Result<R, Box<dyn Error>>
    where
        F: FnOnce(&ReadLockGuard<Slot>) -> R,
    {
        let mem = &self.shmem.rlock::<Slot>(get_lock_id_slot(slot_index))?;
        Ok(f(mem))
    }
}
