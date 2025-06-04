use std::cell::RefCell;
use std::ptr;
use std::sync::Mutex; // Removed MutexGuard
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
    #[allow(unused)] // Silencing potential false positive warning; this field IS used.
    cfg_shmem_max_slots: usize,
    // write_lock: Mutex<()>, // Removed instance lock
}

// Global lock for all MessageWriter::add operations
static GLOBAL_WRITER_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

const EMPTY_ROW_FOR_INIT: RowIndex = RowIndex {
    start_slot_index: 0,
    start_data_index: 0,
    end_slot_index: 0,
    end_data_index: 0,
    row_size: 0,
};

impl MessageWriter {
    pub fn new(cfg: &WriterConfig) -> Result<MessageWriter, ShmemLibError> {
        let ctx = reader_context(&cfg.shmem)?;
        let shmem_service = ShmemService::new(ctx);

        let mut writer = MessageWriter {
            shmem_service: shmem_service,
            callback_after_add: Vec::<RefCell<Box<dyn AfterAdd>>>::with_capacity(2),
            cfg_max_rows: cfg.shmem.max_rows,
            cfg_max_slot_size: cfg.shmem.max_slot_size,
            cfg_max_row_size: cfg.shmem.max_row_size,
            cfg_shmem_max_slots: cfg.shmem.max_slots,
        };
        replica::setup(&mut writer);
        writer.callback_after_add.shrink_to_fit();
        Ok(writer)
    }

    pub fn close(&self) -> () {
        self.shmem_service.close()
    }
}

#[inline]
fn get_next_row_logic(
    index: &Index,
    length: usize,
    cfg_max_rows: usize,
    cfg_max_slot_size: usize,
    // cfg_max_slots: usize, // Removed unused parameter
) -> (usize, RowIndex) {
    debug_assert!(length > 0);

    let shmem_idx_for_metadata_storage = if index.count == 0 {
        0
    } else if index.end_row_index >= cfg_max_rows - 1 {
        0
    } else {
        index.end_row_index + 1
    };

    let actual_last_row_metadata_for_slots = if index.count == 0 {
        &EMPTY_ROW_FOR_INIT
    } else {
        &index.rows[index.end_row_index]
    };

    let (raw_next_slot_index, raw_next_data_index) =
        if actual_last_row_metadata_for_slots.end_data_index < cfg_max_slot_size {
            (actual_last_row_metadata_for_slots.end_slot_index, actual_last_row_metadata_for_slots.end_data_index)
        } else {
            (actual_last_row_metadata_for_slots.end_slot_index + 1, 0)
        };

    let num_additional_slots_spanned = (raw_next_data_index + length - 1) / cfg_max_slot_size;
    let raw_end_slot_index = raw_next_slot_index + num_additional_slots_spanned;
    let mut raw_end_data_index = (raw_next_data_index + length) % cfg_max_slot_size;

    if raw_end_data_index == 0 && length > 0 {
        raw_end_data_index = cfg_max_slot_size;
    }

    // If cfg_max_slots parameter in get_next_row_logic is unused, this RowIndex::new call
    // and its subsequent usage must be independent of it for physical slot calculation,
    // or the physical slot calculation must happen elsewhere (e.g. in MessageWriter::add).
    // For now, assuming this function's logic is sound and cfg_max_slots is used if needed.
    // The RowIndex itself stores logical/raw slot indices, not physical/wrapped ones.
    let new_row_metadata = RowIndex::new(
        raw_next_slot_index,
        raw_next_data_index,
        raw_end_slot_index,
        raw_end_data_index,
        cfg_max_slot_size,
    );

    (
        shmem_idx_for_metadata_storage,
        new_row_metadata
    )
}


impl MessageWriter {
    pub fn add(&mut self, message: &[u8]) -> Result<usize, ShmemLibError> {
        let _guard = GLOBAL_WRITER_LOCK.lock().unwrap();

        let length = message.len();
        let message_ptr = message.as_ptr();

        if length == 0 {
            return Err(ShmemLibError::Logic("Message length cannot be zero".to_string()));
        }
        if length > self.cfg_max_row_size {
            return Err(ShmemLibError::Logic(format!(
                "Message length ({}) exceeds configured max_row_size ({})",
                length, self.cfg_max_row_size
            )));
        }

        let cfg_max_rows = self.cfg_max_rows;
        let cfg_max_slot_size = self.cfg_max_slot_size;
        let cfg_max_slots = self.cfg_shmem_max_slots; // This reads the struct field

        let (returned_shmem_idx, row_metadata_for_msg) = self.shmem_service.write_index(move |index_ptr| {
            let (shmem_idx_for_next_meta, new_row_meta) = get_next_row_logic(
                index_ptr,
                length,
                cfg_max_rows,
                cfg_max_slot_size,
                // cfg_max_slots, // Argument removed
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
            // The physical slot index calculation *requires* cfg_max_slots.
            let physical_slot_idx_to_write = slot_idx_absolute % cfg_max_slots;

            let start_data_offset_in_this_segment = if slot_idx_absolute == row_metadata_for_msg.start_slot_index {
                row_metadata_for_msg.start_data_index
            } else {
                0
            };
            let end_data_offset_in_this_segment = if slot_idx_absolute == row_metadata_for_msg.end_slot_index {
                row_metadata_for_msg.end_data_index
            } else {
                self.cfg_max_slot_size // Uses self.cfg_max_slot_size
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
        Ok(returned_shmem_idx)
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
    use shared_memory;
    use crate::core::ShmemConfig;
    use crate::core::writer_context;

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
        let max_row_size_config = max_slot_size_config * 4;

        let config = ShmemConfig::builder()
            .data_dir(temp_dir.path().to_str().unwrap_or("/dev/shm").to_string())
            .shmem_file_name(unique_file_name)
            .max_rows(max_rows_config)
            .max_slots(max_slots_config)
            .max_slot_size(max_slot_size_config)
            .max_row_size(max_row_size_config)
            .build()?;

        let initial_shmem_mapping = writer_context(&config)?;

        let writer_config = Arc::new(WriterConfig { shmem: config });
        Ok((writer_config, temp_dir, initial_shmem_mapping))
    }

    #[test]
    fn next_row_1() -> Result<(), ShmemLibError> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            count: 0,
            rows: [Default::default(); MAX_ROWS],
        };
        let test_cfg_max_rows = MAX_ROWS;
        let test_cfg_max_slot_size = COMPILE_TIME_MAX_SLOT_SIZE;
        // let test_cfg_max_slots = MAX_ROWS * 2; // Removed unused variable

        let (next_row_index, row) =
            get_next_row_logic(index, 1, test_cfg_max_rows, test_cfg_max_slot_size);
        let expected_row = RowIndex::new(0,0,0,1, test_cfg_max_slot_size);
        assert_eq!(next_row_index, 0);
        assert_eq!(row, expected_row);
        Ok(())
    }

    #[test]
    fn next_row_max() -> Result<(), ShmemLibError> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            count: 0,
            rows: [Default::default(); MAX_ROWS],
        };
        let test_cfg_max_rows = MAX_ROWS;
        let test_cfg_max_slot_size = COMPILE_TIME_MAX_SLOT_SIZE;
        // let test_cfg_max_slots = MAX_ROWS * 2; // Removed unused variable


        let (next_row_index, row) = get_next_row_logic(
            index,
            test_cfg_max_slot_size,
            test_cfg_max_rows,
            test_cfg_max_slot_size
        );
        let expected_row = RowIndex::new(0,0,0,test_cfg_max_slot_size, test_cfg_max_slot_size);
        assert_eq!(next_row_index, 0);
        assert_eq!(row, expected_row);
        Ok(())
    }

    #[test]
    fn next_row_multiple_slots() -> Result<(), ShmemLibError> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            count: 0,
            rows: [Default::default(); MAX_ROWS],
        };
        let test_cfg_max_rows = MAX_ROWS;
        let test_cfg_max_slot_size = COMPILE_TIME_MAX_SLOT_SIZE;
        // let test_cfg_max_slots = MAX_ROWS * 2; // Removed unused variable
        let length = test_cfg_max_slot_size + 1;

        let (next_row_index, row) =
            get_next_row_logic(index, length, test_cfg_max_rows, test_cfg_max_slot_size);
        let expected_row = RowIndex::new(0,0,1,1, test_cfg_max_slot_size);
        assert_eq!(next_row_index, 0);
        assert_eq!(row, expected_row);
        Ok(())
    }

    #[test]
    fn test_concurrent_writes_no_overlap_all_read_back() -> Result<(), ShmemLibError> {
        let num_threads = 4;
        let messages_per_thread = 250;
        let total_messages = num_threads * messages_per_thread;
        let max_slot_size_config = 128;

        let max_rows_for_test = (total_messages + 100).min(MAX_ROWS);
        let effective_total_messages = if total_messages > max_rows_for_test {
            assert!(total_messages <= max_rows_for_test, "Effective total_messages ({}) would exceed max_rows_for_test ({}) after clamping to MAX_ROWS. Test parameters are incompatible with MAX_ROWS.", total_messages, max_rows_for_test);
            total_messages
        } else {
            total_messages
        };

        let (writer_config_arc, _temp_dir, _initial_shmem_mapping) = setup_concurrent_test_shmem(
            max_rows_for_test,
            effective_total_messages * 2,
            max_slot_size_config,
        )?;

        let mut handles = vec![];
        let mut all_intended_messages_globally = HashSet::new();

        for i in 0..num_threads {
            let writer_config_clone = Arc::clone(&writer_config_arc);
            let handle = thread::spawn(move || {
                let mut writer = MessageWriter::new(&writer_config_clone)
                    .expect("Failed to create MessageWriter in thread");
                let mut thread_messages_written = Vec::with_capacity(messages_per_thread);
                for j in 0..messages_per_thread {
                    let message = format!("thread_{}_msg_{}", i, j);
                    assert!(message.as_bytes().len() <= max_slot_size_config,
                            "Test message (len {}) too large for configured max_slot_size ({})",
                            message.as_bytes().len(), max_slot_size_config);

                    match writer.add(message.as_bytes()) {
                        Ok(_) => {
                            thread_messages_written.push(message);
                        }
                        Err(e) => {
                            return Err(format!("Thread {} failed to write message {} ('{}'): {:?}", i, j, message, e));
                        }
                    }
                }
                Ok::<Vec<String>, String>(thread_messages_written)
            });
            handles.push(handle);
        }

        for handle in handles {
            match handle.join().unwrap() {
                Ok(thread_messages_written) => {
                    for msg in thread_messages_written {
                        all_intended_messages_globally.insert(msg);
                    }
                }
                Err(e_str) => {
                    panic!("A writer thread encountered an error: {}", e_str);
                }
            }
        }
        assert_eq!(all_intended_messages_globally.len(), effective_total_messages,
                   "Mismatch in expected number of unique messages successfully written and collected. Expected {}, got {}.",
                   effective_total_messages, all_intended_messages_globally.len());

        let reader_shmem_config = writer_config_arc.shmem.clone();
        let reader_shmem_ctx_for_index = reader_context(&reader_shmem_config)?;
        let shmem_service_for_index = ShmemService::new(reader_shmem_ctx_for_index);

        let index_state = shmem_service_for_index.read_index(|index_ptr: &Index| {
            Index {
                first_row_index: index_ptr.first_row_index,
                end_row_index: index_ptr.end_row_index,
                count: index_ptr.count,
                rows: index_ptr.rows,
            }
        })?;
        assert_eq!(index_state.count, effective_total_messages, "Index.count ({}) does not match total_messages written ({}).", index_state.count, effective_total_messages);

        let reader_shmem_cfg_for_reader = writer_config_arc.shmem.clone();
        let reader_instance_config = crate::reader::ReaderConfig { shmem: reader_shmem_cfg_for_reader };
        let reader = MessageReader::new(&reader_instance_config)?;

        let mut retrieved_messages_set = HashSet::new();
        let mut temp_read_buffer = Vec::with_capacity(max_slot_size_config);

        for i in 0..(index_state.count as usize) {
            let current_row_in_index_array = (index_state.first_row_index as usize + i) % writer_config_arc.shmem.max_rows;

            struct ReadCaptureContext<'a> {
                buffer: &'a mut Vec<u8>,
            }
            temp_read_buffer.clear();
            let mut capture_context = ReadCaptureContext { buffer: &mut temp_read_buffer };

            let read_result = reader.read(
                current_row_in_index_array,
                |data_ptr, len, ctx_val: &mut ReadCaptureContext| {
                    unsafe {
                        let message_slice = std::slice::from_raw_parts(data_ptr, len);
                        ctx_val.buffer.extend_from_slice(message_slice);
                    }
                    Ok::<(), ShmemLibError>(())
                },
                &mut capture_context,
            );

            if let Err(e) = read_result {
                panic!("Error during reader.read() for row_index {}: {:?}", current_row_in_index_array, e);
            }

            let retrieved_string = String::from_utf8(temp_read_buffer.clone())
                .expect(&format!("Failed to convert retrieved bytes to String for message at shmem_row_idx {}", current_row_in_index_array));

            retrieved_messages_set.insert(retrieved_string);
        }

        assert_eq!(retrieved_messages_set.len(), effective_total_messages,
                   "Number of unique retrieved messages ({}) does not match total_messages ({}).",
                   retrieved_messages_set.len(), effective_total_messages);

        assert_eq!(retrieved_messages_set, all_intended_messages_globally,
                   "The set of retrieved messages does not match the set of originally written messages.");

        Ok(())
    }
}
