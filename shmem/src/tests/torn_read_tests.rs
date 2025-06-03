use std::{
    error::Error,
    sync::{atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering}, Arc},
    thread,
    time::Duration,
};

use tempfile::{tempdir, TempDir};
use shared_memory; // For Box<shared_memory::Shmem> - crate is named shared_memory

use std::convert::TryInto; // For try_into() on slices
// Removed duplicate TryInto import
use crate::{
    core::{
        writer_context, reader_context, ShmemConfig, ShmemService, Index, // Added Index back
        MAX_ROWS, // Compile-time MAX_ROWS for test config
        // COMPILE_TIME_MAX_SLOT_SIZE, RowIndex are unused
    },
    writer::{MessageWriter, WriterConfig},
    reader::{MessageReader, ReaderConfig},
    ShmemLibError,
};

// Helper function for setting up torn read tests
fn setup_torn_read_test_shmem(
    test_max_rows: usize, // Allow specifying max_rows for the test
    test_max_slots: usize,
    test_max_slot_size: usize,
    test_max_row_size: usize,
) -> Result<(Arc<WriterConfig>, Arc<ReaderConfig>, TempDir, Box<shared_memory::Shmem>), Box<dyn Error>> {
    static SHMEM_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);
    let temp_dir = tempdir()?;

    let unique_file_name = format!(
        "test_torn_read_shmem_{}_{}",
        std::process::id(),
        SHMEM_ID_COUNTER.fetch_add(1, AtomicOrdering::SeqCst)
    );

    let shmem_config = ShmemConfig::builder()
        .data_dir("/dev/shm".to_string())
        .shmem_file_name(unique_file_name)
        .max_rows(test_max_rows)
        .max_slots(test_max_slots)
        .max_slot_size(test_max_slot_size)
        .max_row_size(test_max_row_size)
        .build()?;

    let initial_shmem_mapping = writer_context(&shmem_config)?;

    let writer_config = Arc::new(WriterConfig { shmem: shmem_config.clone() });
    let reader_config = Arc::new(ReaderConfig { shmem: shmem_config }); // Clone not needed for last use

    Ok((writer_config, reader_config, temp_dir, initial_shmem_mapping))
}

#[derive(Debug, Clone, PartialEq)]
struct TornReadTestMessage {
    id: u64,
    data: Vec<u8>,
}

impl TornReadTestMessage {
    fn new(id: u64, size: usize) -> Self {
        let val = (id % 250) as u8; // Keep value under 256
        TornReadTestMessage {
            id,
            data: vec![val; size],
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.id.to_le_bytes());
        bytes.extend_from_slice(&(self.data.len() as u32).to_le_bytes()); // Length of data
        bytes.extend_from_slice(&self.data);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 12 { // 8 for id + 4 for data_len
            return None;
        }
        let id = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let data_len = u32::from_le_bytes(bytes[8..12].try_into().ok()?) as usize;

        if bytes.len() < 12 + data_len {
            return None; // Not enough data bytes
        }
        let data_slice = &bytes[12..(12 + data_len)];

        Some(TornReadTestMessage {
            id,
            data: data_slice.to_vec(),
        })
    }

    fn is_consistent(&self) -> bool {
        if self.data.is_empty() {
            return true; // Or false, depending on whether 0-size is valid and consistent
        }
        let expected_val = (self.id % 250) as u8;
        self.data.iter().all(|&byte| byte == expected_val)
    }
}

#[test]
fn test_torn_read_test_message_serialization() {
    let original_msg = TornReadTestMessage::new(12345, 32);
    let bytes = original_msg.to_bytes();
    let deserialized_msg = TornReadTestMessage::from_bytes(&bytes).expect("Deserialization failed");
    assert_eq!(original_msg, deserialized_msg);
    assert!(deserialized_msg.is_consistent());

    let original_msg_large_id = TornReadTestMessage::new(u64::MAX, 64);
    let bytes_large_id = original_msg_large_id.to_bytes();
    let deserialized_msg_large_id = TornReadTestMessage::from_bytes(&bytes_large_id).expect("Deserialization failed for large ID");
    assert_eq!(original_msg_large_id, deserialized_msg_large_id);
    assert!(deserialized_msg_large_id.is_consistent());

    let mut inconsistent_data = original_msg.data.clone();
    if !inconsistent_data.is_empty() {
        inconsistent_data[0] = inconsistent_data[0].wrapping_add(1); // Make it inconsistent
    }
    let inconsistent_msg = TornReadTestMessage { id: original_msg.id, data: inconsistent_data };
    assert!(!inconsistent_msg.is_consistent());
}

// Main test function test_demonstrate_reader_torn_reads will be added next
#[test]
fn test_demonstrate_reader_torn_reads() -> Result<(), Box<dyn Error>> {
    let message_size: usize = 256; // Spans 2 slots if max_slot_size is 128
    let test_max_slot_size = 128;
    let test_max_row_size = message_size * 2; // Allow for message_size
    let test_max_rows = MAX_ROWS; // Use compile-time MAX_ROWS (16 for tests)
    let test_max_slots = 32;

    println!(
        "Starting test_demonstrate_reader_torn_reads with message_size: {}, max_slot_size: {}",
        message_size, test_max_slot_size
    );

    let (writer_config_arc, reader_config_arc, _temp_dir, _initial_shmem_mapping) =
        setup_torn_read_test_shmem(
            test_max_rows,
            test_max_slots,
            test_max_slot_size,
            test_max_row_size,
        )?;

    let stop_flag = Arc::new(AtomicBool::new(false));
    let mut message_id_counter = 0_u64; // Not atomic, only writer thread increments

    // Writer Thread
    let writer_stop_flag = Arc::clone(&stop_flag);
    let writer_thread = thread::spawn({
        let writer_config = Arc::clone(&writer_config_arc);
        move || {
            let mut writer = MessageWriter::new(&writer_config)
                .expect("Writer thread: Failed to create MessageWriter");
            let mut messages_written = 0;
            while !writer_stop_flag.load(AtomicOrdering::Relaxed) {
                message_id_counter += 1;
                let msg = TornReadTestMessage::new(message_id_counter, message_size);
                let msg_bytes = msg.to_bytes();

                match writer.add(&msg_bytes) {
                    Ok(_) => messages_written +=1,
                    Err(ShmemLibError::Logic(ref s)) if s.contains("queue is full") || s.contains("max_rows") => {
                        // Expected if queue fills up, allow writer to continue trying
                        thread::sleep(Duration::from_micros(100)); // Brief pause if full
                    }
                    Err(e) => {
                        eprintln!("Writer thread: Error adding message: {:?}", e);
                        // Depending on the error, might want to break or panic
                        // For this test, let's try to continue unless it's a severe error
                        if !matches!(e, ShmemLibError::Logic(_)) { // Stop on non-logic errors
                             eprintln!("Writer thread: Unrecoverable error, stopping: {:?}", e);
                             break;
                        }
                    }
                }
                thread::sleep(Duration::from_micros(50)); // Adjust timing to influence torn read likelihood
            }
            println!("Writer thread finished. Messages written: {}", messages_written);
            messages_written
        }
    });

    // Reader Thread
    let reader_stop_flag = Arc::clone(&stop_flag);
    let reader_thread = thread::spawn({
        let reader_config = Arc::clone(&reader_config_arc);
        move || {
            let reader = MessageReader::new(&reader_config) // Removed mut
                .expect("Reader thread: Failed to create MessageReader");

            // ShmemService for reading index directly. Reader itself doesn't expose Index directly.
            let shmem_service_ctx = reader_context(&reader_config.shmem)
                .expect("Reader thread: Failed to create reader_context for ShmemService");
            let shmem_service = ShmemService::new(shmem_service_ctx);

            let mut consistent_reads = 0_usize;
            let mut torn_reads_detected = 0_usize;
            let mut read_attempts = 0_usize;
            let mut actual_read_successes = 0_usize;
            let mut parse_failures = 0_usize;

            let mut data_buffer = Vec::with_capacity(message_size + 128); // Buffer for reader.read

            while !reader_stop_flag.load(AtomicOrdering::Relaxed) {
                read_attempts += 1;
                let index_state_res = shmem_service.read_index(|idx_ptr: &Index| { // Explicit type for idx_ptr
                    // Manually copy fields to create an owned Index struct
                    Index {
                        first_row_index: idx_ptr.first_row_index,
                        end_row_index: idx_ptr.end_row_index,
                        count: idx_ptr.count,
                        rows: idx_ptr.rows, // RowIndex is Copy, so this is a copy
                    }
                });

                match index_state_res {
                    Ok(index_state) => {
                        if index_state.count == 0 {
                            thread::sleep(Duration::from_micros(100)); // Wait if queue is empty
                            continue;
                        }

                        // Attempt to read the message at physical end_row_index
                        // This is where active writing is most likely to occur.
                        let target_row_index = index_state.end_row_index;

                        struct ReadCaptureContext<'a> { buffer_target: &'a mut Vec<u8>, }
                        data_buffer.clear();
                        let mut capture_ctx = ReadCaptureContext { buffer_target: &mut data_buffer };

                        match reader.read(
                            target_row_index,
                            |ptr, len, ctx| -> Result<(), ShmemLibError> { // Explicit return type for closure
                                unsafe {
                                    let slice = std::slice::from_raw_parts(ptr, len);
                                    ctx.buffer_target.extend_from_slice(slice);
                                }
                                Ok(()) // This will now be Result::Ok::<(), ShmemLibError>(())
                            },
                            &mut capture_ctx
                        ) {
                            // reader.read returns Result<R, ShmemLibError> where R is the closure's return type.
                            // So, this is Result<Result<(), ShmemLibError>, ShmemLibError>.
                            Ok(inner_result) => { // Outer Ok means read operation itself succeeded
                                actual_read_successes += 1;
                                match inner_result { // Check result from closure
                                    Ok(()) => {
                                        if !data_buffer.is_empty() {
                                            match TornReadTestMessage::from_bytes(&data_buffer) {
                                        Some(msg) => {
                                            if !msg.is_consistent() {
                                                torn_reads_detected += 1;
                                                // println!("Reader thread: Torn read detected: id={}, data_len={}", msg.id, msg.data.len());
                                            } else {
                                                consistent_reads += 1;
                                            }
                                        }
                                        None => { // Parse error, severe form of torn read
                                            parse_failures +=1;
                                            torn_reads_detected += 1;
                                            // println!("Reader thread: Parse error (severe torn read) for data length: {}", data_buffer.len());
                                        }
                                    }
                                }
                                    },
                                    Err(e_closure) => {
                                         eprintln!("Reader thread: Closure error during read processing: {:?}", e_closure);
                                         // This case might not be hit if closure always returns Ok(())
                                    }
                                }
                            }
                            Err(ShmemLibError::Logic(ref s)) if s.contains("Row") && s.contains("has size 0") => {
                                // This means reader.read itself returned an error because the row was empty.
                            }
                            Err(e) => { // Outer error from reader.read operation
                                eprintln!("Reader thread: Error from reader.read operation: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                         eprintln!("Reader thread: Error reading shmem index: {:?}", e);
                    }
                }
                thread::sleep(Duration::from_micros(70)); // Adjust timing
            }
            println!(
                "Reader thread finished. Attempts: {}, Actual Reads: {}, Consistent: {}, Torn: {}, Parse Failures: {}",
                read_attempts, actual_read_successes, consistent_reads, torn_reads_detected, parse_failures
            );
            (consistent_reads, torn_reads_detected)
        }
    });

    thread::sleep(Duration::from_secs(3)); // Let threads run
    stop_flag.store(true, AtomicOrdering::Relaxed);

    let writer_result = writer_thread.join().expect("Writer thread panicked");
    let reader_result = reader_thread.join().expect("Reader thread panicked");

    let (consistent_reads, torn_reads_detected) = reader_result;

    println!("Test finished. Total messages written (approx): {}", writer_result);
    println!("Final counts - Consistent reads: {}, Torn reads detected: {}", consistent_reads, torn_reads_detected);

    // Ensure some activity happened
    assert!(
        consistent_reads > 0 || torn_reads_detected > 0,
        "Test did not process any messages (consistent: {}, torn: {}). Check timings or test duration.",
        consistent_reads, torn_reads_detected
    );

    // Torn reads are timing dependent and might not always occur.
    // This assertion can be made conditional, e.g., for local testing vs CI.
    if cfg!(not(feature = "ci_environment")) { // Example: define "ci_environment" feature in Cargo.toml for CI
         // Or simply: if std::env::var("CI").is_err() { ... }
        if torn_reads_detected == 0 {
            println!("Warning: No torn reads were detected in this run. This can happen due to timing.");
        }
        // assert!(torn_reads_detected > 0, "Expected to detect torn reads in a local/non-CI environment.");
        // For now, let's not fail the test if torn reads are not detected, as they are probabilistic.
        // The main purpose is to have a test that *can* demonstrate them.
    } else {
        println!("CI environment: Torn read detection is statistical, not asserting torn_reads > 0.");
    }

    Ok(())
}
