use std::alloc;
use std::ptr;

use serde_derive::{Deserialize, Serialize};

// Import the new error type
use crate::ShmemLibError;
use super::core::*;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ReaderConfig {
    shmem: ShmemConfig,
}

pub struct MessageReader {
    shmem_service: Box<ShmemService>,
    config_max_slot_size: usize, // Store for use in read()
}

impl MessageReader {
    pub fn new(cfg: &ReaderConfig) -> Result<MessageReader, ShmemLibError> {
        // It might be useful to check cfg.shmem.max_slot_size against COMPILE_TIME_MAX_SLOT_SIZE here too,
        // if MessageReader's logic strictly depends on it not exceeding the physical.
        // For now, assume ShmemConfig is validated by the writer or is inherently consistent.
        if cfg.shmem.max_slot_size == 0 || cfg.shmem.max_slot_size > COMPILE_TIME_MAX_SLOT_SIZE {
             return Err(ShmemLibError::Logic(format!(
                "Configured max_slot_size ({}) must be > 0 and <= compile-time COMPILE_TIME_MAX_SLOT_SIZE ({})",
                cfg.shmem.max_slot_size, COMPILE_TIME_MAX_SLOT_SIZE
            )));
        }

        let ctx = reader_context(&cfg.shmem)?;
        let shmem_service = ShmemService::new(ctx);
        Ok(MessageReader {
            shmem_service: shmem_service,
            config_max_slot_size: cfg.shmem.max_slot_size,
        })
    }

    pub fn close(&self) -> () {
        self.shmem_service.close()
    }

    pub fn read<F, C, R>(
        &self,
        row_index: usize,
        f: &F,
        context: &mut C,
    ) -> Result<R, ShmemLibError> // Changed
    where
        F: Fn(*mut u8, usize, &mut C) -> R,
    {
        let row = self
            .shmem_service
            .read_index(|index| index.rows[row_index])?; // This now returns ShmemLibError
        let layout = unsafe { alloc::Layout::from_size_align_unchecked(row.row_size, 1) };
        let mut curr_buff_index = 0;
        let buff = unsafe { alloc::alloc(layout) };
        for slot_index in row.start_slot_index..=row.end_slot_index {
            let start_data_index = if slot_index == row.start_slot_index {
                row.start_data_index
            } else {
                0
            };
            let end_data_index = if slot_index == row.end_slot_index {
                row.end_data_index
            } else {
                self.config_max_slot_size // Use stored config value
            };
            let pertial_row_size = end_data_index - start_data_index;
            self.shmem_service.read_slot(slot_index, |slot| {
                unsafe {
                    let src_p = slot.data.as_ptr().add(start_data_index);
                    let dest_p = buff.add(curr_buff_index);
                    ptr::copy(src_p, dest_p, pertial_row_size);
                }
                curr_buff_index += pertial_row_size;
            })?; // This now returns ShmemLibError
        }
        // TODO Validate a hash value of the message being same as a value in the index.
        let result = Ok(f(buff, row.row_size, context));
        unsafe { alloc::dealloc(buff, layout) };
        result
    }
}
