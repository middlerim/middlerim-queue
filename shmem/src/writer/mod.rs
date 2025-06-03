use std::cell::RefCell;
use std::ptr;
use std::sync::{Mutex, MutexGuard}; // Ensure MutexGuard is imported if needed, though static might not need it explicitly in struct
use once_cell::sync::Lazy; // For static Mutex initialization

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
    // write_lock: Mutex<()>, // Removed instance lock
}

// Global lock for all MessageWriter::add operations
static GLOBAL_WRITER_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

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

        // In a scenario where multiple writers connect to the same pre-initialized shmem,
        // they should open it, not try to re-create it.
        // writer_context() deletes and recreates. reader_context() opens existing.
        // For this constructor, assuming shmem is initialized by an external call
        // to writer_context() before this MessageWriter is created.
        let ctx = reader_context(&cfg.shmem)?; // Changed from writer_context to reader_context
        let shmem_service = ShmemService::new(ctx);

        let mut writer = MessageWriter {
            shmem_service: shmem_service,
            callback_after_add: Vec::<RefCell<Box<dyn AfterAdd>>>::with_capacity(2),
            cfg_max_rows: cfg.shmem.max_rows,
            cfg_max_slot_size: cfg.shmem.max_slot_size,
            cfg_max_row_size: cfg.shmem.max_row_size,
            cfg_shmem_max_slots: cfg.shmem.max_slots, // Initialize cfg_shmem_max_slots
            // write_lock: Mutex::new(()), // Removed instance lock initialization
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
    cfg_max_slots: usize,
) -> (usize, RowIndex) {
    debug_assert!(length > 0);

    // 1. Determine shmem_idx_for_metadata_storage (where in Index.rows this new RowIndex metadata will be stored)
    let shmem_idx_for_metadata_storage = if index.count == 0 {
        0
    } else if index.end_row_index >= cfg_max_rows - 1 {
        0 // Wrap RowIndex storage for metadata
    } else {
        index.end_row_index + 1
    };

    // 2. Determine actual_last_row_metadata to decide where physical data writing continues from
    let actual_last_row_metadata_for_slots = if index.count == 0 {
        &EMPTY_ROW_FOR_INIT
    } else {
        &index.rows[index.end_row_index]
    };

    // 3. Calculate next_slot_index and next_data_index for the new message's data (these are RAW, not wrapped by cfg_max_slots yet)
    let (raw_next_slot_index, raw_next_data_index) =
        if actual_last_row_metadata_for_slots.end_data_index < cfg_max_slot_size {
            // Continue in the current slot of the last message
            (actual_last_row_metadata_for_slots.end_slot_index, actual_last_row_metadata_for_slots.end_data_index)
        } else {
            // Last message ended exactly at slot boundary, so start new message in the next slot, at offset 0
            (actual_last_row_metadata_for_slots.end_slot_index + 1, 0)
        };

    // 4. Calculate end slot/data for the new message (these are also RAW, not wrapped by cfg_max_slots yet)
    let num_additional_slots_spanned = (raw_next_data_index + length - 1) / cfg_max_slot_size;
    let raw_end_slot_index = raw_next_slot_index + num_additional_slots_spanned;
    let mut raw_end_data_index = (raw_next_data_index + length) % cfg_max_slot_size;

    if raw_end_data_index == 0 && length > 0 { // If it perfectly fills a slot
        raw_end_data_index = cfg_max_slot_size;
    }

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
        let _guard = GLOBAL_WRITER_LOCK.lock().unwrap(); // Acquire global static lock

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
                cfg_max_slots,
            );

            index_ptr.rows[shmem_idx_for_next_meta] = new_row_meta;
            index_ptr.end_row_index = shmem_idx_for_next_meta;

            if index_ptr.count == cfg_max_rows {
                index_ptr.first_row_index = (index_ptr.first_row_index + 1) % cfg_max_rows;
            } else {
                index_ptr.count += 1;
            }

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
    use crate::core::{ShmemService, Index, RowIndex, MAX_ROWS, COMPILE_TIME_MAX_SLOT_SIZE, reader_context};
    use crate::reader::MessageReader;
    use crate::ShmemLibError;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use std::thread;
    use tempfile::{tempdir, TempDir};
    use shared_memory; // For Box<shared_memory::Shmem> from writer_context
    use crate::core::ShmemConfig; // For ShmemConfig::builder() and WriterConfig
    use crate::core::writer_context; // For initializing shmem

    // Helper function for setting up concurrent tests
    fn setup_concurrent_test_shmem(
        max_rows_config: usize,
        max_slots_config: usize,
        max_slot_size_config: usize,
    ) -> Result<(Arc<WriterConfig>, TempDir, Box<shared_memory::Shmem>), ShmemLibError> {
        static SHMEM_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);
        let temp_dir = tempdir().expect("Failed to create temp dir for test artifact tracking");

        let unique_file_name = format!(
            "test_shmem_concurrent_{}_{}",
            std::process::id(),
            SHMEM_ID_COUNTER.fetch_add(1, AtomicOrdering::SeqCst)
        );

        // max_row_size_config should be appropriate for messages that might span slots,
        // or at least be larger than max_slot_size_config if messages can be larger than one slot.
        // A common default from ShmemConfigBuilder is 524_288.
        // The original helper used max_slot_size_config * 4. Let's stick to that for consistency.
        let max_row_size_config = max_slot_size_config * 4;

        let config = ShmemConfig::builder()
            .data_dir(temp_dir.path().to_str().unwrap_or("/dev/shm").to_string()) // Use temp_dir path
            .shmem_file_name(unique_file_name)
            .max_rows(max_rows_config)
            .max_slots(max_slots_config)
            .max_slot_size(max_slot_size_config)
            .max_row_size(max_row_size_config)
            .build()?;

        // Initialize shmem and keep the mapping alive by returning it (Box<shared_memory::Shmem>)
        let initial_shmem_mapping = writer_context(&config)?;

        let writer_config = Arc::new(WriterConfig { shmem: config });
        Ok((writer_config, temp_dir, initial_shmem_mapping))
    }

    #[test]
    fn next_row_1() -> Result<(), ShmemLibError> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            count: 0, // Initialize count
            rows: [Default::default(); MAX_ROWS], // Test uses compile-time MAX_ROWS
        };
        // Use default config values for testing the logic
        let test_cfg_max_rows = MAX_ROWS; // Test with compile-time value for this test's scope
        let test_cfg_max_slot_size = COMPILE_TIME_MAX_SLOT_SIZE; // Test with compile-time value
        let test_cfg_max_slots = MAX_ROWS * 2; // Example value

        let (next_row_index, row) =
            get_next_row_logic(index, 1, test_cfg_max_rows, test_cfg_max_slot_size, test_cfg_max_slots);
        // The next_row_index for metadata is index.end_row_index + 1, which is 0+1=1 if queue not empty.
        // If queue is empty (count == 0), shmem_idx_for_metadata_storage is 0.
        // Since index.count is 0, shmem_idx_for_metadata_storage should be 0.
        let expected_row = RowIndex::new(0,0,0,1, test_cfg_max_slot_size);
        assert_eq!(next_row_index, 0); // Metadata for the first item goes into index.rows[0]
        assert_eq!(row, expected_row);
        Ok(())
    }

    #[test]
    fn next_row_max() -> Result<(), ShmemLibError> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            count: 0, // Initialize count
            rows: [Default::default(); MAX_ROWS],
        };
        let test_cfg_max_rows = MAX_ROWS;
        let test_cfg_max_slot_size = COMPILE_TIME_MAX_SLOT_SIZE;
        let test_cfg_max_slots = MAX_ROWS * 2; // Example value


        let (next_row_index, row) = get_next_row_logic(
            index,
            test_cfg_max_slot_size, // length is full slot size
            test_cfg_max_rows,
            test_cfg_max_slot_size,
            test_cfg_max_slots,
        );
        let expected_row = RowIndex::new(0,0,0,test_cfg_max_slot_size, test_cfg_max_slot_size);
        assert_eq!(next_row_index, 0); // Metadata for the first item
        assert_eq!(row, expected_row);
        Ok(())
    }

    #[test]
    fn next_row_multiple_slots() -> Result<(), ShmemLibError> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            count: 0, // Initialize count
            rows: [Default::default(); MAX_ROWS],
        };
        let test_cfg_max_rows = MAX_ROWS;
        let test_cfg_max_slot_size = COMPILE_TIME_MAX_SLOT_SIZE;
        let test_cfg_max_slots = MAX_ROWS * 2; // Example value
        let length = test_cfg_max_slot_size + 1;

        let (next_row_index, row) =
            get_next_row_logic(index, length, test_cfg_max_rows, test_cfg_max_slot_size, test_cfg_max_slots);
        // Expected: starts slot 0, index 0. Length is one full slot + 1 byte.
        // Ends in slot 1, index 1.
        let expected_row = RowIndex::new(0,0,1,1, test_cfg_max_slot_size);
        assert_eq!(next_row_index, 0); // Metadata for the first item
        assert_eq!(row, expected_row);
        Ok(())
    }

    #[test]
    fn test_concurrent_writes_no_overlap_all_read_back() -> Result<(), ShmemLibError> {
        let num_threads = 4; // Using value from prompt
        let messages_per_thread = 250; // Using value from prompt
        let total_messages = num_threads * messages_per_thread;
        let max_slot_size_config = 128; // Using value from prompt

        // max_rows must be less than or equal to MAX_ROWS (compile-time constant)
        // Ensure the test config respects this.
        let max_rows_for_test = (total_messages + 100).min(MAX_ROWS);
        // Ensure total_messages is not greater than max_rows_for_test if clamped.
        let effective_total_messages = if total_messages > max_rows_for_test {
            // This case means MAX_ROWS is too small for the desired total_messages.
            // We should adjust messages_per_thread to fit, or the test will be logically flawed.
            // For simplicity, if this happens, we might need to panic or error out,
            // as the test's intent (specific number of messages) cannot be met.
            // The prompt says "total_messages + 100", so it expects no wrapping for Index.rows
            assert!(total_messages <= max_rows_for_test, "Effective total_messages ({}) would exceed max_rows_for_test ({}) after clamping to MAX_ROWS. Test parameters are incompatible with MAX_ROWS.", total_messages, max_rows_for_test);
            total_messages // Use original if it fits
        } else {
            total_messages
        };

        // Setup shared memory, keep _initial_shmem_mapping and _temp_dir alive by assigning them.
        let (writer_config_arc, _temp_dir, _initial_shmem_mapping) = setup_concurrent_test_shmem(
            max_rows_for_test,
            effective_total_messages * 2, // max_slots (e.g., total_messages * 2)
            max_slot_size_config,
        )?;

        let mut handles = vec![];
        // Use a Mutex protected HashSet for collecting intended messages if individual threads add to it.
        // Or, collect per thread and then merge, which is simpler. The original test did the latter.
        let mut all_intended_messages_globally = HashSet::new(); // For final check

        for i in 0..num_threads {
            let writer_config_clone = Arc::clone(&writer_config_arc);
            // Need to ensure each thread generates its portion of messages for all_intended_messages_globally
            // This part of the original test was fine: collect messages per thread, then add to global set.
            let handle = thread::spawn(move || {
                // Each thread will create its own writer instance.
                // The key is that they all use the same underlying ShmemConfig (via Arc).
                let mut writer = MessageWriter::new(&writer_config_clone)
                    .expect("Failed to create MessageWriter in thread");
                let mut thread_messages_written = Vec::with_capacity(messages_per_thread);
                for j in 0..messages_per_thread {
                    let message = format!("thread_{}_msg_{}", i, j);
                    // Ensure message fits within a single slot if that's an implicit assumption,
                    // or that it's at least <= max_row_size.
                    // The original test checked against max_slot_size_config.
                    assert!(message.as_bytes().len() <= max_slot_size_config,
                            "Test message (len {}) too large for configured max_slot_size ({})",
                            message.as_bytes().len(), max_slot_size_config);

                    // writer.add() returns Result<usize, ShmemLibError>
                    match writer.add(message.as_bytes()) {
                        Ok(_) => {
                            thread_messages_written.push(message);
                        }
                        Err(e) => {
                            // Return an error string that can be processed by the main thread.
                            return Err(format!("Thread {} failed to write message {} ('{}'): {:?}", i, j, message, e));
                        }
                    }
                }
                Ok::<Vec<String>, String>(thread_messages_written)
            });
            handles.push(handle);
        }

        for handle in handles {
            match handle.join().unwrap() { // panic on join error
                Ok(thread_messages_written) => {
                    for msg in thread_messages_written {
                        all_intended_messages_globally.insert(msg);
                    }
                }
                Err(e_str) => {
                    // If any thread returned an Err, panic the test.
                    panic!("A writer thread encountered an error: {}", e_str);
                }
            }
        }
        // Ensure the number of unique messages collected matches total_messages.
        // This implicitly checks if any threads failed to write, or if messages weren't unique.
        assert_eq!(all_intended_messages_globally.len(), effective_total_messages,
                   "Mismatch in expected number of unique messages successfully written and collected. Expected {}, got {}.",
                   effective_total_messages, all_intended_messages_globally.len());

        // Verification (Reading Back)
        // Create a new ShmemService for reading the Index, using the same config.
        let reader_shmem_config = writer_config_arc.shmem.clone();
        // Need a ShmemContext to create ShmemService for reading index.
        // reader_context opens an existing mapping.
        let reader_shmem_ctx_for_index = reader_context(&reader_shmem_config)?;
        let shmem_service_for_index = ShmemService::new(reader_shmem_ctx_for_index);

        let index_state = shmem_service_for_index.read_index(|index_ptr: &Index| {
            // Perform a deep copy of the Index struct's relevant fields.
            // rows array is large, only copy necessary parts or what's needed for verification.
            // Here, we copy all fields as the original test did.
            Index {
                first_row_index: index_ptr.first_row_index,
                end_row_index: index_ptr.end_row_index,
                count: index_ptr.count,
                rows: index_ptr.rows, // This is a copy [RowIndex; MAX_ROWS]
            }
        })?;
        assert_eq!(index_state.count, effective_total_messages, "Index.count ({}) does not match total_messages written ({}).", index_state.count, effective_total_messages);

        // Create a MessageReader instance for the same shared memory.
        let reader_shmem_cfg_for_reader = writer_config_arc.shmem.clone();
        let reader_instance_config = crate::reader::ReaderConfig { shmem: reader_shmem_cfg_for_reader };
        let reader = MessageReader::new(&reader_instance_config)?;

        let mut retrieved_messages_set = HashSet::new();
        // Temporary buffer for reader.read() callback, must be large enough for any single message.
        let mut temp_read_buffer = Vec::with_capacity(max_slot_size_config);

        // Loop from 0 to Index.count - 1.
        for i in 0..(index_state.count as usize) {
            // Calculate the current_row_in_index_array = (Index.first_row_index + i) % shmem_config.max_rows
            let current_row_in_index_array = (index_state.first_row_index as usize + i) % writer_config_arc.shmem.max_rows;

            // Define a context struct for the reader.read callback to use the temp_read_buffer.
            struct ReadCaptureContext<'a> {
                buffer: &'a mut Vec<u8>,
            }
            temp_read_buffer.clear(); // Clear buffer for each new message
            let mut capture_context = ReadCaptureContext { buffer: &mut temp_read_buffer };

            // Use reader.read() to read the message data at current_row_in_index_array.
            // The callback for reader.read() should copy the data into the temporary buffer.
            let read_result = reader.read(
                current_row_in_index_array,
                |data_ptr, len, ctx_val: &mut ReadCaptureContext| {
                    unsafe {
                        // Ensure buffer is clean before extending.
                        // (already cleared outside, but good practice if callback was more complex)
                        // ctx_val.buffer.clear();
                        let message_slice = std::slice::from_raw_parts(data_ptr, len);
                        ctx_val.buffer.extend_from_slice(message_slice);
                    }
                    Ok::<(), ShmemLibError>(()) // Callback must return Result<(), ShmemLibError>
                },
                &mut capture_context, // Pass mutable reference to the context
            );

            // Check if reader.read() or its callback returned an error.
            if let Err(e) = read_result {
                panic!("Error during reader.read() for row_index {}: {:?}", current_row_in_index_array, e);
            }

            // Convert the buffer to a string.
            let retrieved_string = String::from_utf8(temp_read_buffer.clone())
                .expect(&format!("Failed to convert retrieved bytes to String for message at shmem_row_idx {}", current_row_in_index_array));

            // Insert the string into the retrieved_messages HashSet.
            retrieved_messages_set.insert(retrieved_string);
        }

        // Assert that retrieved_messages.len() equals total_messages.
        assert_eq!(retrieved_messages_set.len(), effective_total_messages,
                   "Number of unique retrieved messages ({}) does not match total_messages ({}).",
                   retrieved_messages_set.len(), effective_total_messages);

        // Assert that the retrieved_messages HashSet is equal to the HashSet of originally written messages.
        assert_eq!(retrieved_messages_set, all_intended_messages_globally,
                   "The set of retrieved messages does not match the set of originally written messages.");

        Ok(())
    }
}
