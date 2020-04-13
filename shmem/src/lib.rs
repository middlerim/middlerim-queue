pub use ::shared_memory::*;

pub const MAX_ROW_COUNT: usize = 5;
pub const MAX_SLOT_SIZE: usize = 1024 * 1024;

#[derive(SharedMemCast)]
struct Slot {
    data: [u8; MAX_SLOT_SIZE],
}

#[derive(SharedMemCast)]
pub struct Index {
    pub first_row_index: u8,
    pub end_row_index: u8,
    // 0: Start slot index
    // 1: Start index of the data
    // 2: End slot index
    // 3: End index of the data
    pub rows: [[u8; 4]; MAX_ROW_COUNT],
}

pub const SHMEM_SIZE_INDEX: usize = 1000 + (4 * 8 * MAX_ROW_COUNT);

pub static SHMEM_PATH_INDEX: &'static str = "./middlerim-queue-index.link";
pub static SHMEM_PATH_SLOT_PREFIX: &'static str = "middlerim-queue-slot";

const INDEX_LOCK_ID: usize = 0;

#[macro_export]
macro_rules! writer_context {
    ( $( $x:expr ),* ) => {
        match shmem::SharedMem::create_linked(shmem::SHMEM_PATH_INDEX, shmem::LockType::RwLock, shmem::SHMEM_SIZE_INDEX) {
            Ok(v) => v,
            Err(shmem::SharedMemError::LinkExists) => {
                let link = shmem::SharedMem::open_linked(shmem::SHMEM_PATH_INDEX)?;
                std::fs::remove_file(shmem::SHMEM_PATH_INDEX);
                shmem::SharedMem::create_linked(shmem::SHMEM_PATH_INDEX, shmem::LockType::RwLock, shmem::SHMEM_SIZE_INDEX).unwrap()
            }
            Err(e) => return Err(e),
        }
    };
}

#[macro_export]
macro_rules! reader_context {
    ( $( $x:expr ),* ) => {
        shmem::SharedMem::open_linked(shmem::SHMEM_PATH_INDEX).unwrap()
    };
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
