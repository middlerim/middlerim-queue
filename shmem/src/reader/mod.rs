use std::alloc;
use std::error::Error;
use std::ptr;

use serde_derive::{Deserialize, Serialize};

use super::core::*;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ReaderConfig {
    shmem: ShmemConfig,
}

pub struct MessageReader {
    shmem_service: Box<ShmemService>,
}

impl MessageReader {
    pub fn new(cfg: &ReaderConfig) -> Result<MessageReader, Box<dyn Error>> {
        let ctx = reader_context(&cfg.shmem)?;
        let shmem_service = ShmemService::new(ctx);
        Ok(MessageReader { shmem_service: shmem_service })
    }

    pub fn close(&self) -> () {
        self.shmem_service.close()
    }

    pub fn read<F, C, R>(&self, row_index: usize, f: &F, context: &mut C) -> Result<R, Box<dyn Error>>
        where F: Fn(*mut u8, usize, &mut C) -> R,
    {
        let row = self.shmem_service.read_index(|index| {
            index.rows[row_index]
        })?;
        let layout = unsafe { alloc::Layout::from_size_align_unchecked(row.row_size, 1) };
        let mut curr_buff_index = 0;
        let buff = unsafe {
            alloc::alloc(layout)
        };
        for slot_index in row.start_slot_index..=row.end_slot_index {
            let start_data_index = if slot_index == row.start_slot_index {
                row.start_data_index
            } else {
                0
            };
            let end_data_index = if slot_index == row.end_slot_index {
                row.end_data_index
            } else {
                MAX_SLOT_SIZE
            };
            let pertial_row_size = end_data_index - start_data_index;
            self.shmem_service.read_slot(slot_index, |slot| {
                unsafe {
                    let src_p = slot.data.as_ptr().add(start_data_index);
                    let dest_p = buff.add(curr_buff_index);
                    ptr::copy(src_p, dest_p, pertial_row_size);
                }
                curr_buff_index += pertial_row_size;
            })?;
        }
        // TODO Validate a hash value of the message being same as a value in the index.
        let result = Ok(f(buff, row.row_size, context));
        unsafe { alloc::dealloc(buff, layout) };
        result
    }
}
