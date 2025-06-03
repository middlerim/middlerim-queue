use std::cell::RefCell;
use std::ptr;

use serde_derive::{Deserialize, Serialize};

// Import the new error type
use crate::ShmemLibError;
use super::core::*;
use super::replica;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct WriterConfig {
    pub shmem: ShmemConfig, // Made public
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
    cfg_shmem_max_slots: usize, // Added to store max_slots from ShmemConfig
}

// This static FIRST_ROW's row_size is 0, which is fine as a placeholder.
// It's used when the row index wraps around.
// Renamed to AVOID_STATIC_MUT warning, and it's effectively const.
const EMPTY_ROW_FOR_INIT: RowIndex = RowIndex {
    start_slot_index: 0,
    start_data_index: 0,
    end_slot_index: 0,
    end_data_index: 0,
    row_size: 0, // Explicitly 0, no calculation needed for this placeholder
};

impl MessageWriter {
    pub fn new(cfg: &WriterConfig) -> Result<MessageWriter, ShmemLibError> {
        // Validations are now handled by ShmemConfigBuilder::build()
        // ShmemConfig received here is assumed to be valid.

        let ctx = writer_context(&cfg.shmem)?;
        let shmem_service = ShmemService::new(ctx);

        let mut writer = MessageWriter {
            shmem_service: shmem_service,
            callback_after_add: Vec::<RefCell<Box<dyn AfterAdd>>>::with_capacity(2),
            cfg_max_rows: cfg.shmem.max_rows,
            cfg_max_slot_size: cfg.shmem.max_slot_size,
            cfg_max_row_size: cfg.shmem.max_row_size,
            cfg_shmem_max_slots: cfg.shmem.max_slots, // Initialize cfg_shmem_max_slots
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
    cfg_max_slots: usize, // Added for logging & potential modulo logic
) -> (usize, RowIndex) {
    println!("[get_next_row_logic] Entry: index.first_row_index={}, index.end_row_index={}, index.count={}, msg_length={}",
        index.first_row_index, index.end_row_index, index.count, length);
    println!("[get_next_row_logic] Config: cfg_max_rows={}, cfg_max_slots={}, cfg_max_slot_size={}",
        cfg_max_rows, cfg_max_slots, cfg_max_slot_size);
    debug_assert!(length > 0);

    // 1. Determine shmem_idx_for_metadata_storage (where in Index.rows this new RowIndex metadata will be stored)
    let shmem_idx_for_metadata_storage = if index.count == 0 {
        0
    } else if index.end_row_index >= cfg_max_rows - 1 {
        0 // Wrap RowIndex storage for metadata
    } else {
        index.end_row_index + 1
    };
    println!("[get_next_row_logic] Calculated shmem_idx_for_metadata_storage: {}", shmem_idx_for_metadata_storage);

    // 2. Determine actual_last_row_metadata to decide where physical data writing continues from
    let actual_last_row_metadata_for_slots = if index.count == 0 {
        println!("[get_next_row_logic] Using EMPTY_ROW_FOR_INIT for slot calculation (queue empty).");
        &EMPTY_ROW_FOR_INIT
    } else {
        println!("[get_next_row_logic] Using index.rows[{}] for slot calculation.", index.end_row_index);
        &index.rows[index.end_row_index]
    };
    println!("[get_next_row_logic] actual_last_row_metadata_for_slots: start_slot={}, start_data={}, end_slot={}, end_data={}",
        actual_last_row_metadata_for_slots.start_slot_index, actual_last_row_metadata_for_slots.start_data_index,
        actual_last_row_metadata_for_slots.end_slot_index, actual_last_row_metadata_for_slots.end_data_index);

    // 3. Calculate next_slot_index and next_data_index for the new message's data (these are RAW, not wrapped by cfg_max_slots yet)
    let (raw_next_slot_index, raw_next_data_index) =
        if actual_last_row_metadata_for_slots.end_data_index < cfg_max_slot_size {
            // Continue in the current slot of the last message
            (actual_last_row_metadata_for_slots.end_slot_index, actual_last_row_metadata_for_slots.end_data_index)
        } else {
            // Last message ended exactly at slot boundary, so start new message in the next slot, at offset 0
            (actual_last_row_metadata_for_slots.end_slot_index + 1, 0)
        };
    println!("[get_next_row_logic] Raw next slot/data for new message: slot_idx={}, data_idx={}", raw_next_slot_index, raw_next_data_index);

    // 4. Calculate end slot/data for the new message (these are also RAW, not wrapped by cfg_max_slots yet)
    let num_additional_slots_spanned = (raw_next_data_index + length - 1) / cfg_max_slot_size;
    let raw_end_slot_index = raw_next_slot_index + num_additional_slots_spanned;
    let mut raw_end_data_index = (raw_next_data_index + length) % cfg_max_slot_size;

    if raw_end_data_index == 0 && length > 0 { // If it perfectly fills a slot
        raw_end_data_index = cfg_max_slot_size;
    }
    println!("[get_next_row_logic] Raw end slot/data for new message: end_slot_idx={}, end_data_idx={}", raw_end_slot_index, raw_end_data_index);

    // For this logging pass, RowIndex continues to store raw, ever-increasing slot indices.
    // The fix will be to apply % cfg_max_slots to these before storing in RowIndex,
    // and ensuring ShmemService also uses % cfg_max_slots.
    let new_row_metadata = RowIndex::new(
        raw_next_slot_index,
        raw_next_data_index,
        raw_end_slot_index,
        raw_end_data_index,
        cfg_max_slot_size,
    );
    println!("[get_next_row_logic] Created RowIndex (with raw slots): {:?}", new_row_metadata);

    (
        shmem_idx_for_metadata_storage, // This is the index in Index.rows[]
        new_row_metadata                // This is the RowIndex data to store
    )
}


impl MessageWriter { // Re-open impl block to add methods back
    /// Adds a message to the shared memory queue.
    ///
    /// # Arguments
    /// * `message`: A byte slice containing the message data.
    ///
    /// # Returns
    /// The `row_index` where the message was written, or an error.
    pub fn add(&mut self, message: &[u8]) -> Result<usize, ShmemLibError> { // Changed signature
        let length = message.len(); // Get length from slice
        let message_ptr = message.as_ptr(); // Get pointer from slice

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
        let cfg_max_slots = self.cfg_shmem_max_slots; // Use stored cfg_shmem_max_slots

        let (returned_shmem_idx, row_metadata_for_msg) = self.shmem_service.write_index(move |index_ptr| {
            let (shmem_idx_for_next_meta, new_row_meta) = get_next_row_logic(
                index_ptr,
                length,
                cfg_max_rows,
                cfg_max_slot_size,
                cfg_max_slots, // Pass cfg_max_slots
            );

            println!("[MessageWriter::add write_index closure] Before update: index.first={}, index.end={}, index.count={}",
                index_ptr.first_row_index, index_ptr.end_row_index, index_ptr.count);
            println!("[MessageWriter::add write_index closure] Writing RowIndex {:?} to shmem_idx {}",
                new_row_meta, shmem_idx_for_next_meta);

            index_ptr.rows[shmem_idx_for_next_meta] = new_row_meta;
            index_ptr.end_row_index = shmem_idx_for_next_meta;

            if index_ptr.count == cfg_max_rows {
                index_ptr.first_row_index = (index_ptr.first_row_index + 1) % cfg_max_rows;
            } else {
                index_ptr.count += 1;
            }

            println!("[MessageWriter::add write_index closure] After update: index.first={}, index.end={}, index.count={}",
                index_ptr.first_row_index, index_ptr.end_row_index, index_ptr.count);

            (shmem_idx_for_next_meta, new_row_meta)
        })?;

        let mut curr_message_index = 0;

        for slot_idx_absolute in row_metadata_for_msg.start_slot_index..=row_metadata_for_msg.end_slot_index {
            let physical_slot_idx_to_write = slot_idx_absolute % cfg_max_slots; // Apply modulo for actual write

            let start_data_offset_in_this_segment = if slot_idx_absolute == row_metadata_for_msg.start_slot_index {
                row_metadata_for_msg.start_data_index
            } else {
                0
            };
            let end_data_offset_in_this_segment = if slot_idx_absolute == row_metadata_for_msg.end_slot_index {
                row_metadata_for_msg.end_data_index
            } else {
                self.cfg_max_slot_size
            };
            let pertial_row_size = end_data_offset_in_this_segment - start_data_offset_in_this_segment;

            if pertial_row_size == 0 {
                 continue;
            }

            println!("[MessageWriter::add copy_loop] MsgSegment: writing to abs_slot_idx={}, phys_slot_idx={}", slot_idx_absolute, physical_slot_idx_to_write);
            println!("[MessageWriter::add copy_loop]   segment_offsets: start={}, end={}, size={}", start_data_offset_in_this_segment, end_data_offset_in_this_segment, pertial_row_size);
            println!("[MessageWriter::add copy_loop]   src_msg_offset={}", curr_message_index);

            self.shmem_service.write_slot(physical_slot_idx_to_write, |slot_struct_instance| unsafe {
                let src_p = message_ptr.add(curr_message_index);
                let dest_p = slot_struct_instance.data.as_mut_ptr().add(start_data_offset_in_this_segment);
                ptr::copy_nonoverlapping(src_p, dest_p, pertial_row_size);
            })?;

            curr_message_index += pertial_row_size;
        }
        debug_assert_eq!(curr_message_index, length, "Data copy sanity check: copied size vs message length");

        for cb in self.callback_after_add.iter() {
            cb.borrow_mut().apply(returned_shmem_idx, message_ptr, length);
        }
        Ok(returned_shmem_idx) // Return the shmem_idx where metadata was stored
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
