use std::cell::RefCell;
use std::ptr;

use serde_derive::{Deserialize, Serialize};

// Import the new error type
use crate::ShmemLibError;
use super::core::*;
use super::replica;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct WriterConfig {
    shmem: ShmemConfig,
}

pub trait AfterAdd {
    fn apply(&mut self, row_index: usize, message: *const u8, length: usize) -> ();
}

pub struct MessageWriter {
    pub shmem_service: Box<ShmemService>,
    pub callback_after_add: Vec<RefCell<Box<dyn AfterAdd>>>,
    // Store relevant config values
    cfg_max_rows: usize,
    cfg_max_slot_size: usize,
    cfg_max_row_size: usize,
}

// This static FIRST_ROW's row_size is 0, which is fine as a placeholder.
// It's used when the row index wraps around.
static FIRST_ROW: RowIndex = RowIndex {
    start_slot_index: 0,
    start_data_index: 0,
    end_slot_index: 0,
    end_data_index: 0,
    row_size: 0, // Explicitly 0, no calculation needed for this placeholder
};

impl MessageWriter {
    pub fn new(cfg: &WriterConfig) -> Result<MessageWriter, ShmemLibError> {
        // Check if configured max_rows exceeds compile-time MAX_ROWS (from core)
        if cfg.shmem.max_rows == 0 || cfg.shmem.max_rows > MAX_ROWS {
            return Err(ShmemLibError::Logic(format!(
                "Configured max_rows ({}) must be > 0 and <= compile-time MAX_ROWS ({})",
                cfg.shmem.max_rows, MAX_ROWS
            )));
        }
        // Check configured max_slot_size against compile-time COMPILE_TIME_MAX_SLOT_SIZE (from core)
        if cfg.shmem.max_slot_size == 0 || cfg.shmem.max_slot_size > COMPILE_TIME_MAX_SLOT_SIZE {
             return Err(ShmemLibError::Logic(format!(
                "Configured max_slot_size ({}) must be > 0 and <= compile-time COMPILE_TIME_MAX_SLOT_SIZE ({})",
                cfg.shmem.max_slot_size, COMPILE_TIME_MAX_SLOT_SIZE
            )));
        }
        if cfg.shmem.max_row_size == 0 {
            return Err(ShmemLibError::Logic("Configured max_row_size must be > 0".to_string()));
        }


        let ctx = writer_context(&cfg.shmem)?;
        let shmem_service = ShmemService::new(ctx);

        let mut writer = MessageWriter {
            shmem_service: shmem_service,
            callback_after_add: Vec::<RefCell<Box<dyn AfterAdd>>>::with_capacity(2),
            cfg_max_rows: cfg.shmem.max_rows,
            cfg_max_slot_size: cfg.shmem.max_slot_size,
            cfg_max_row_size: cfg.shmem.max_row_size,
        };
        // Assuming replica::setup doesn't need changes related to these config values directly.
        replica::setup(&mut writer);
        writer.callback_after_add.shrink_to_fit();
        Ok(writer)
    }

    pub fn close(&self) -> () {
        self.shmem_service.close()
    }

    // get_next_row_logic is now a free function defined above
}

// Made this a free function to simplify testing; pass config values.
#[inline]
fn get_next_row_logic(
    index: &Index,
    length: usize,
    cfg_max_rows: usize,
    cfg_max_slot_size: usize,
) -> (usize, RowIndex) {
    // Ensure length is not zero, as it can lead to issues with end_data_index calculation.
    // This check should ideally be in `add` before calling this.
    debug_assert!(length > 0);

    let (last_row, next_row_index) = if index.end_row_index >= cfg_max_rows - 1 {
        (&FIRST_ROW, 0) // FIRST_ROW is a placeholder with size 0.
    } else {
        // Accessing index.rows directly. Ensure end_row_index is within compile-time MAX_ROWS bounds.
        // cfg_max_rows check in MessageWriter::new ensures index.end_row_index will be < MAX_ROWS.
        (&index.rows[index.end_row_index], index.end_row_index + 1)
    };

    let (next_slot_index, next_data_index) =
        if last_row.end_data_index < cfg_max_slot_size {
            (last_row.end_slot_index, last_row.end_data_index)
        } else {
            // Slot is full, move to the start of the next slot (or same slot if end_slot_index wasn't incremented yet)
            // This logic implies that end_slot_index from last_row might not be "after" the slot if it was full.
            // If last_row.end_data_index == cfg_max_slot_size, it means the slot is full.
            // So, the next data should start at index 0 of the *next* slot.
            (last_row.end_slot_index + 1, 0)
        };

    // Calculate end_slot_index:
    // `(next_data_index + length - 1)` ensures that if `length` is a multiple of `cfg_max_slot_size`
    // (and fills up to the boundary), it doesn't spill into an unnecessary next slot.
    // E.g., if next_data_index=0, length=cfg_max_slot_size, then (cfg_max_slot_size-1)/cfg_max_slot_size = 0 additional slots.
    // If next_data_index=0, length=cfg_max_slot_size+1, then (cfg_max_slot_size)/cfg_max_slot_size = 1 additional slot.
    let end_slot_index = next_slot_index + ((next_data_index + length -1) / cfg_max_slot_size);
    let mut end_data_index = (next_data_index + length) % cfg_max_slot_size;

    if end_data_index == 0 && length > 0 { // If it perfectly fills the slot, end_data_index becomes full slot size
        end_data_index = cfg_max_slot_size;
    } else if length > 0 && end_slot_index > next_slot_index && (next_data_index + length) % cfg_max_slot_size == 0 {
        // This case is when it fills up one or more slots and ends exactly at slot boundary
        // but end_data_index became 0 due to modulo. It should be cfg_max_slot_size.
        // This is covered by the previous if end_data_index == 0.
    }


    (
        next_row_index,
        RowIndex::new(
            next_slot_index,
            next_data_index,
            end_slot_index,
            end_data_index,
            cfg_max_slot_size, // Pass cfg_max_slot_size to RowIndex::new
        ),
    )
}


impl MessageWriter { // Re-open impl block to add methods back
    pub fn add(&mut self, message: *const u8, length: usize) -> Result<usize, ShmemLibError> {
        if length == 0 {
            return Err(ShmemLibError::Logic("Message length cannot be zero".to_string()));
        }
        if length > self.cfg_max_row_size {
            return Err(ShmemLibError::Logic(format!(
                "Message length ({}) exceeds configured max_row_size ({})",
                length, self.cfg_max_row_size
            )));
        }
        // Ensure message length does not exceed physical limits imposed by slot size if that's a concern.
        // Current logic in get_next_row_logic handles spanning slots.
        // Max row size check above should be primary.

        // Copy config values to be moved into the closure, avoiding borrowing self.
        let cfg_max_rows = self.cfg_max_rows;
        let cfg_max_slot_size = self.cfg_max_slot_size;

        let (next_row_index, row) = self.shmem_service.write_index(move |index| { // move closure
            // Use the new free function, passing stored config values
            let (next_row_index, row) = get_next_row_logic(
                index,
                length,
                cfg_max_rows, // Use copied value
                cfg_max_slot_size, // Use copied value
            );
            index.end_row_index = next_row_index;
            index.rows[next_row_index] = row; // Assumes next_row_index < compile-time MAX_ROWS
            (next_row_index, row)
        })?;
        let mut curr_message_index = 0;
        for slot_index in row.start_slot_index..=row.end_slot_index {
            let start_data_index = if slot_index == row.start_slot_index {
                row.start_data_index
            } else {
                0
            };
            let end_data_index = if slot_index == row.end_slot_index {
                row.end_data_index
            } else {
                self.cfg_max_slot_size // Use configured max_slot_size
            };
            let pertial_row_size = end_data_index - start_data_index;
            self.shmem_service.write_slot(slot_index, |slot| unsafe {
                let src_p = message.add(curr_message_index);
                let dest_p = slot.data.as_mut_ptr().add(start_data_index);
                ptr::copy(src_p, dest_p, pertial_row_size);
            })?; // This now returns ShmemLibError

            curr_message_index += pertial_row_size;
        }

        for cb in self.callback_after_add.iter() {
            cb.borrow_mut().apply(next_row_index, message, length);
        }
        Ok(next_row_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // No need to import ShmemLibError if tests don't explicitly return it,
    // but good practice if they might. Standard library Result might be fine for simple test assertions.
    // However, if any operations within tests could return ShmemLibError and use ?,
    // then the test signature must be compatible. For now, these tests don't do fallible shmem ops.

    // then the test signature must be compatible. For now, these tests don't do fallible shmem ops.

    #[test]
    fn next_row_1() -> Result<(), ShmemLibError> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            rows: [Default::default(); MAX_ROWS], // Test uses compile-time MAX_ROWS
        };
        // Use default config values for testing the logic
        let test_cfg_max_rows = MAX_ROWS; // Test with compile-time value for this test's scope
        let test_cfg_max_slot_size = COMPILE_TIME_MAX_SLOT_SIZE; // Test with compile-time value

        let (next_row_index, row) =
            get_next_row_logic(index, 1, test_cfg_max_rows, test_cfg_max_slot_size);
        let expected_row = RowIndex::new(0,0,0,1, test_cfg_max_slot_size);
        assert_eq!(next_row_index, 1);
        assert_eq!(row, expected_row);
        Ok(())
    }

    #[test]
    fn next_row_max() -> Result<(), ShmemLibError> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            rows: [Default::default(); MAX_ROWS],
        };
        let test_cfg_max_rows = MAX_ROWS;
        let test_cfg_max_slot_size = COMPILE_TIME_MAX_SLOT_SIZE;

        let (next_row_index, row) = get_next_row_logic(
            index,
            test_cfg_max_slot_size, // length is full slot size
            test_cfg_max_rows,
            test_cfg_max_slot_size,
        );
        let expected_row = RowIndex::new(0,0,0,test_cfg_max_slot_size, test_cfg_max_slot_size);
        assert_eq!(next_row_index, 1);
        assert_eq!(row, expected_row);
        Ok(())
    }

    #[test]
    fn next_row_multiple_slots() -> Result<(), ShmemLibError> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            rows: [Default::default(); MAX_ROWS],
        };
        let test_cfg_max_rows = MAX_ROWS;
        let test_cfg_max_slot_size = COMPILE_TIME_MAX_SLOT_SIZE;
        let length = test_cfg_max_slot_size + 1;

        let (next_row_index, row) =
            get_next_row_logic(index, length, test_cfg_max_rows, test_cfg_max_slot_size);
        // Expected: starts slot 0, index 0. Length is one full slot + 1 byte.
        // Ends in slot 1, index 1.
        let expected_row = RowIndex::new(0,0,1,1, test_cfg_max_slot_size);
        assert_eq!(next_row_index, 1);
        assert_eq!(row, expected_row);
        Ok(())
    }
}
