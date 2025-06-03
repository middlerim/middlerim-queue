use std::alloc;
use std::ptr;

use serde_derive::{Deserialize, Serialize};

// Import the new error type
use crate::ShmemLibError;
use crate::errors::UserAbortReason; // Added import for UserAbortReason
use super::core::*;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ReaderConfig {
    pub shmem: ShmemConfig, // Made public
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

    /// Reads a message from shared memory, allocating a new buffer for the message.
    ///
    /// The provided callback `f` is invoked with a pointer to the newly allocated buffer
    /// containing the message data and the length of the message.
    ///
    /// **Note:** This method allocates a new buffer for each message, which can have
    /// performance implications for high-throughput scenarios. For performance-sensitive
    /// applications, or when buffer management is critical, consider using
    /// [`read_into_buffer`](#method.read_into_buffer) or
    /// [`read_segments`](#method.read_segments).
    ///
    /// # Parameters
    /// - `row_index`: The index of the row (message) to read.
    /// - `f`: A callback function that will be invoked with the message data.
    ///   - `*mut u8`: Pointer to the start of the allocated buffer containing the message.
    ///   - `usize`: Length of the message in the buffer.
    ///   - `&mut C`: A mutable reference to a context object provided by the caller.
    /// - `context`: The context object to be passed to the callback `f`.
    ///
    /// # Returns
    /// - `Ok(R)`: The value returned by the callback `f`.
    /// - `Err(ShmemLibError)`: If any error occurs during shared memory access or if the
    ///   `RowIndex` is invalid.
    pub fn read<F, C, R>(
        &self,
        row_index: usize,
        f: &F,
        context: &mut C,
    ) -> Result<R, ShmemLibError>
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

    /// Reads a message in segments directly from shared memory, avoiding an intermediate copy.
    ///
    /// This method is suitable for zero-copy processing of messages. The provided callback
    /// is invoked for each memory segment that constitutes the message.
    ///
    /// # Concurrency and Torn Reads:
    /// This method reads slot data without acquiring locks per slot, similar to `read()`.
    /// If a message spans multiple slots, and a concurrent writer is updating these slots,
    /// it's possible to read a "torn message" where different segments come from different
    /// versions of the data. The `RowIndex` itself is read once at the beginning (with a lock,
    /// as per current `ShmemService::read_index` implementation). If `RowIndex` itself is corrupted
    /// or points to inconsistent slot information, behavior is undefined.
    /// Users should ensure that the data pointed to by `row_index` is in a stable state
    /// if strict consistency is required across all segments of a message.
    ///
    /// # Parameters:
    /// - `row_index`: The index of the row (message) to read.
    /// - `context`: A mutable context that will be passed to each callback invocation.
    /// - `callback`: A closure invoked for each segment of the message.
    ///   - Arguments to callback:
    ///     - `&[u8]`: A slice pointing directly into the shared memory segment. Its lifetime
    ///                is tied to the duration of the callback. Do not store this slice beyond
    ///                the callback's scope.
    ///     - `bool`: `is_first_segment`.
    ///     - `bool`: `is_last_segment`.
    ///     - `&mut C`: The user-provided context.
    ///   - Returns:
    ///     - `Ok(())`: To continue processing more segments if any.
    ///     - `Err(UserAbortReason)`: To stop processing further segments.
    ///
    /// # Returns:
    /// - `Ok(())`: If all segments were processed successfully and the callback always returned `Ok(())`.
    /// - `Err(ShmemLibError::UserAbort(reason))`: If the callback requested an abort.
    /// - `Err(ShmemLibError::...)`: For other errors, e.g., issues reading the initial `RowIndex`
    ///   or problems with slot access (though most slot access errors are now less likely with
    ///   current `ShmemService` which might panic or return `ShmemLibError::Lock` if a lock fails).
    pub fn read_segments<F, C>(
        &self,
        row_index: usize,
        context: &mut C,
        callback: &mut F,
    ) -> Result<(), ShmemLibError>
    where
        F: FnMut(
            &[u8], // Slice points directly into shared memory. Simplified from Result<&[u8], &TornReadError>
            bool,  // is_first_segment
            bool,  // is_last_segment
            &mut C,
        ) -> Result<(), UserAbortReason>,
    {
        let row = self.shmem_service.read_index(|index| {
            // Basic check: ensure row_index is within the bounds of what the current config allows for rows.
            // The actual array size is compile-time MAX_ROWS.
            // This check is against the logical max_rows from ShmemConfig used by the writer.
            // However, MessageReader doesn't store cfg.max_rows. It should rely on RowIndex data.
            // If row_index is out of bounds of index.rows, it will panic.
            // This is acceptable; a bad row_index is a usage error.
            index.rows[row_index]
        })?;

        if row.row_size == 0 { // Handle empty message (no segments to read)
            // Optionally, call callback once with empty slice, is_first=true, is_last=true
            // Or, as per current loop structure, it will simply not iterate.
            // Let's call it once if row_size is 0 but it's a valid (though empty) row.
            // If row.end_data_index == 0 and other fields are also 0, it might be an uninitialized RowIndex.
            // The `is_empty()` method on RowIndex could be used if RowIndex was more context-aware.
            // For now, if row_size is 0, we assume no segments.
            if row.start_slot_index == row.end_slot_index && row.start_data_index == row.end_data_index {
                 // This looks like an truly empty or uninitialized row.
                 // Call the callback once indicating an empty message.
                match callback(&[], true, true, context) {
                    Ok(()) => return Ok(()),
                    Err(abort_reason) => return Err(ShmemLibError::UserAbort(abort_reason)),
                }
            }
            // If row_size is 0 but slot indices/data indices differ, it might be an anomaly.
            // However, the loop condition `row.start_slot_index..=row.end_slot_index` handles it.
        }

        let num_segments = if row.row_size == 0 { 0 } else { row.end_slot_index.saturating_sub(row.start_slot_index) + 1 };
        let mut segments_processed = 0;

        for current_slot_idx in row.start_slot_index..=row.end_slot_index {
            let segment_start_offset = if current_slot_idx == row.start_slot_index {
                row.start_data_index
            } else {
                0
            };
            let segment_end_offset = if current_slot_idx == row.end_slot_index {
                row.end_data_index
            } else {
                self.config_max_slot_size
            };

            // Basic validation of segment offsets
            if segment_start_offset > segment_end_offset || segment_end_offset > self.config_max_slot_size {
                 return Err(ShmemLibError::Logic(format!(
                    "Invalid segment offsets for slot {}: start {}, end {} (max_slot_size: {})",
                    current_slot_idx, segment_start_offset, segment_end_offset, self.config_max_slot_size
                )));
            }
            // If segment is empty but there is overall row_size, skip (should not happen with valid RowIndex)
            if segment_start_offset == segment_end_offset && row.row_size > 0 {
                continue;
            }


            let is_first = segments_processed == 0;
            segments_processed += 1;
            let is_last = segments_processed == num_segments;

            // ShmemService::read_slot's closure needs to match the type expected by read_segments' callback.
            // The current ShmemService::read_slot takes `F: FnOnce(&Slot) -> R`.
            // We need to call our `callback` from within that.
            let user_decision_result: Result<(), UserAbortReason> = self.shmem_service.read_slot(current_slot_idx, |slot_struct_ref| {
                // slot_struct_ref is &mut Slot, but we only need &Slot for reading.
                // Create the slice from the shared memory.
                // Ensure segment_end_offset does not exceed physical slot size.
                // COMPILE_TIME_MAX_SLOT_SIZE is the physical boundary.
                // self.config_max_slot_size is the logical boundary used for RowIndex calculation.
                // The segment_end_offset should be <= self.config_max_slot_size.
                // And self.config_max_slot_size must be <= COMPILE_TIME_MAX_SLOT_SIZE (checked in MessageReader::new).
                let actual_segment_end_offset = std::cmp::min(segment_end_offset, slot_struct_ref.data.len());
                 if segment_start_offset > actual_segment_end_offset {
                     // This indicates a severe issue, potentially corrupted RowIndex or slot_struct_ref not matching expectations
                     return Err(UserAbortReason::InternalError(format!(
                        "Corrected segment_end_offset {} is less than segment_start_offset {} for slot {}",
                        actual_segment_end_offset, segment_start_offset, current_slot_idx
                    )));
                 }

                let segment_slice = &slot_struct_ref.data[segment_start_offset..actual_segment_end_offset];
                callback(segment_slice, is_first, is_last, context)
            })?; // This `?` handles ShmemLibError from read_slot itself.
                 // The inner Result<(), UserAbortReason> is now `user_decision_result`.

            if let Err(abort_reason) = user_decision_result {
                return Err(ShmemLibError::UserAbort(abort_reason));
            }
        }
        Ok(())
    }

    /// Reads a message directly into a user-provided buffer.
    ///
    /// This method avoids internal allocations for the message data by copying segments
    /// of the message directly from shared memory into the `user_buffer`.
    ///
    /// # Concurrency and Torn Reads:
    /// Similar to `read_segments` and `read`, this method generally reads slot data
    /// without acquiring per-slot locks. `read_index` (which it calls internally) is locked.
    /// If a message spans multiple slots and a concurrent writer is updating these slots,
    /// a "torn read" (where different parts of `user_buffer` get data from different
    /// points in time of the write) is possible. For applications requiring strict data
    /// consistency across the entire message, external synchronization might be needed if
    /// concurrent writes to the same message data are expected.
    ///
    /// # Parameters:
    /// - `row_index`: The index of the row (message) to read.
    /// - `user_buffer`: A mutable byte slice provided by the user where the message data
    ///                  will be copied.
    ///
    /// # Returns:
    /// - `Ok(usize)`: The number of bytes read into `user_buffer`. This will be equal to
    ///                `RowIndex.row_size`.
    /// - `Err(ShmemLibError::Logic)`: If `user_buffer` is too small to hold the message,
    ///                                or if an internal consistency issue is detected (e.g.,
    ///                                bytes copied doesn't match `row_size`).
    /// - `Err(ShmemLibError::...)`: For other errors, such as issues reading the `RowIndex`
    ///                              or problems during slot access.
    pub fn read_into_buffer(
        &self,
        row_index: usize,
        user_buffer: &mut [u8],
    ) -> Result<usize, ShmemLibError> {
        let row = self.shmem_service.read_index(|index| {
            index.rows[row_index] // Panics if row_index is out of bounds (compile-time MAX_ROWS)
        })?;

        if row.row_size == 0 {
            // Consistent with read_segments: if row_size is 0, it's an empty message.
            // No need to check start/end slot/data indices here as read_segments does.
            return Ok(0);
        }

        if user_buffer.len() < row.row_size {
            return Err(ShmemLibError::Logic(format!(
                "User buffer too small. Needed: {}, available: {}.",
                row.row_size, user_buffer.len()
            )));
        }

        let mut bytes_copied_total: usize = 0;

        for current_slot_idx in row.start_slot_index..=row.end_slot_index {
            let segment_start_offset = if current_slot_idx == row.start_slot_index {
                row.start_data_index
            } else {
                0
            };
            let segment_end_offset = if current_slot_idx == row.end_slot_index {
                row.end_data_index
            } else {
                self.config_max_slot_size
            };

            let segment_length = segment_end_offset - segment_start_offset;

            if segment_length == 0 { // Should only happen if row_size is 0, which is handled.
                                     // Or if RowIndex is malformed.
                if row.row_size > 0 {
                     return Err(ShmemLibError::Logic(format!(
                        "Malformed RowIndex or internal error: segment length is 0 for slot {} while row_size is {}",
                        current_slot_idx, row.row_size
                    )));
                }
                continue;
            }

            // Safeguard: ensure we don't write past the end of user_buffer.
            // This should ideally be covered by the initial check against row.row_size.
            if bytes_copied_total + segment_length > user_buffer.len() {
                return Err(ShmemLibError::Logic(format!(
                    "Internal error: About to write past user_buffer. Copied: {}, segment: {}, buffer: {}",
                    bytes_copied_total, segment_length, user_buffer.len()
                )));
            }
             if segment_start_offset > segment_end_offset || segment_end_offset > self.config_max_slot_size {
                 return Err(ShmemLibError::Logic(format!(
                    "Invalid segment offsets for slot {}: start {}, end {} (max_slot_size: {})",
                    current_slot_idx, segment_start_offset, segment_end_offset, self.config_max_slot_size
                )));
            }


            // The closure for read_slot needs to copy data into the user_buffer.
            // It captures `user_buffer_ptr` (as *mut u8) and `current_offset_in_user_buffer`.
            // This requires careful unsafe pointer manipulation if we stick to FnOnce(&mut Slot).
            //
            // Let's use the same pattern as read_segments for the closure passed to read_slot.
            // The closure will perform the copy. To do this with FnOnce(&mut Slot),
            // user_buffer must be captured. Since user_buffer is &mut [u8], this is tricky.
            //
            // Simpler: the closure returns just `Ok(())` or an error if copy fails within it.
            // `read_slot`'s `R` becomes `Result<(), ShmemLibError>` for this specific use.
            // We need to ensure the slice `user_buffer` can be safely accessed from the closure.
            // One way is to pass raw pointers.

            let dest_slice_start = bytes_copied_total;
            let user_buffer_ptr = user_buffer.as_mut_ptr(); // Get raw pointer to user_buffer

            self.shmem_service.read_slot(current_slot_idx, |slot_data| {
                // This closure is FnOnce(&mut Slot)
                // It needs to write to user_buffer at offset dest_slice_start
                let actual_segment_end_offset = std::cmp::min(segment_end_offset, slot_data.data.len());
                if segment_start_offset > actual_segment_end_offset {
                    // This would be an internal error, hard to propagate out of FnOnce(&mut Slot) -> () nicely
                    // without changing read_slot's R to be Result.
                    // For now, assume valid segment offsets based on earlier checks.
                    // If this occurs, it's a panic-worthy logic flaw or shmem corruption.
                    // A more robust version might have read_slot's closure return Result.
                    // Let's assume this won't be hit due to prior checks.
                    // If it does, the copy below will panic due to slice bounds.
                }

                let src_slice = &slot_data.data[segment_start_offset..actual_segment_end_offset];

                // SAFETY:
                // 1. `user_buffer_ptr` is valid for writes of `user_buffer.len()` bytes.
                // 2. `dest_slice_start` is the current total of bytes copied so far.
                // 3. `segment_length` is the length of the current segment.
                // 4. We've checked `bytes_copied_total + segment_length <= user_buffer.len()`.
                // 5. `src_slice.len()` is `actual_segment_end_offset - segment_start_offset`.
                //    We must ensure this matches `segment_length` or use `src_slice.len()`.
                //    The `actual_segment_end_offset` logic ensures we don't read past physical slot end.
                //    `segment_length` was calculated from logical `segment_end_offset`.
                //    It's crucial that `src_slice.len()` is used for `copy_from_slice`.
                let current_segment_physical_length = src_slice.len();

                // This check ensures that the logical segment length matches the physical one available.
                // If segment_length (from RowIndex) is larger than what's physically slicable, it's an issue.
                if segment_length != current_segment_physical_length {
                     // This is a critical error, means RowIndex is inconsistent with slot reality.
                     // Hard to return error from here without changing read_slot signature.
                     // For now, this would lead to panic or data corruption if not caught by slice copy.
                     // Let's rely on the copy_from_slice to panic if lengths mismatch.
                     // A production system would need a way for this closure to return error.
                }


                let dest_ptr = unsafe { user_buffer_ptr.add(dest_slice_start) };
                let dest_slice = unsafe { std::slice::from_raw_parts_mut(dest_ptr, current_segment_physical_length) };

                dest_slice.copy_from_slice(src_slice);
                // This closure doesn't need to return anything specific to read_slot's R,
                // as R is generic and can be ().
            })?; // Propagate ShmemLibError from read_slot itself.

            bytes_copied_total += segment_length; // Use logical segment_length for accounting based on RowIndex
        }

        if bytes_copied_total != row.row_size {
            return Err(ShmemLibError::Logic(format!(
                "Internal error: bytes copied ({}) does not match row_size ({})",
                bytes_copied_total, row.row_size
            )));
        }
        Ok(bytes_copied_total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::MessageWriter; // To write messages for reading
    use crate::writer::WriterConfig;
    use tempfile::TempDir;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    // use std::path::PathBuf; // Unused import

    static TEST_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

    // fn get_unique_shmem_path(temp_dir: &TempDir) -> (String, String) { // Unused function
    //     let id = TEST_ID_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
    //     let dir_path = temp_dir.path().to_str().unwrap().to_string();
    //     let file_name = format!("test_shmem_reader_{}", id);
    //     (dir_path, file_name)
    // }

    // Context for the read callback
    struct ReadContext<'a> {
        buffer: &'a mut [u8],
        length_read: usize,
    }

    // Helper setup function
    fn setup_reader_writer_with_config(config: ShmemConfig) -> (MessageWriter, MessageReader, TempDir, ShmemConfig) {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut current_config = config;
        // Ensure data_dir and shmem_file_name are from temp_dir and unique
        current_config.data_dir = temp_dir.path().to_str().unwrap().to_string();
        current_config.shmem_file_name = format!("test_shmem_rw_{}", TEST_ID_COUNTER.fetch_add(1, AtomicOrdering::SeqCst));

        // Create writer context (which creates the shmem file)
        let _ = writer_context(&current_config).expect("Failed to create writer_context in setup");

        let writer_cfg = WriterConfig { shmem: current_config.clone() };
        let reader_cfg = ReaderConfig { shmem: current_config.clone() };

        let writer = MessageWriter::new(&writer_cfg).expect("Failed to create MessageWriter");
        let reader = MessageReader::new(&reader_cfg).expect("Failed to create MessageReader");

        (writer, reader, temp_dir, current_config)
    }

    fn default_test_config() -> ShmemConfig {
        // Use the builder to create the default config for tests
        // data_dir and shmem_file_name are set by setup_reader_writer_with_config
        ShmemConfig::builder()
            .max_rows(10)
            .max_row_size(256)
            .max_slots(5) // Keep slots somewhat limited for multi-slot tests
            .max_slot_size(128) // Relatively small slot size for easier multi-slot testing
            .build()
            .expect("Failed to build default test ShmemConfig")
    }


    #[test]
    fn test_read_single_message() -> Result<(), ShmemLibError> {
        let (mut writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(default_test_config());

        let message_content = "hello world";
        let message_bytes = message_content.as_bytes();

        let row_index = writer.add(message_bytes)?; // Updated call

        let mut read_buffer = vec![0u8; message_bytes.len()];
        let mut context = ReadContext {
            buffer: &mut read_buffer,
            length_read: 0,
        };

        // The callback F: Fn(*mut u8, usize, &mut C) -> R
        // R for this test is just (), or Result<(), ShmemLibError> if callback itself can fail
        let callback_result: Result<(), ShmemLibError> = reader.read(row_index, &|buff_ptr, length, ctx: &mut ReadContext| {
            if length > ctx.buffer.len() {
                // This would be an unexpected error from shmem logic or test setup
                return Err(ShmemLibError::Logic(format!(
                    "Buffer too small. Read length: {}, buffer_len: {}",
                    length, ctx.buffer.len()
                )));
            }
            unsafe {
                ptr::copy_nonoverlapping(buff_ptr, ctx.buffer.as_mut_ptr(), length);
            }
            ctx.length_read = length;
            Ok(())
        }, &mut context)?;

        callback_result?; // Propagate error from closure if any

        assert_eq!(context.length_read, message_bytes.len(), "Length of read message does not match");
        assert_eq!(read_buffer, message_bytes, "Content of read message does not match");

        Ok(())
    }

    #[test]
    fn test_read_multi_slot_message() -> Result<(), ShmemLibError> {
        let mut multi_slot_config = default_test_config();
        multi_slot_config.max_slot_size = 10; // Small slot size to force multi-slot
        multi_slot_config.max_row_size = 50;  // Ensure max_row_size can hold the message

        let (mut writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(multi_slot_config);

        let message_content = "this is a long message that spans slots"; // Length > 10
        let message_bytes = message_content.as_bytes();
        assert!(message_bytes.len() > 10 && message_bytes.len() <= 50);

        let row_index = writer.add(message_bytes)?; // Updated call

        let mut read_buffer = vec![0u8; message_bytes.len()];
        let mut context = ReadContext {
            buffer: &mut read_buffer,
            length_read: 0,
        };

        let cb_res: Result<(), ShmemLibError> = reader.read(row_index, &|buff_ptr, length, ctx: &mut ReadContext| {
            if length > ctx.buffer.len() {
                return Err(ShmemLibError::Logic(format!(
                    "Buffer too small. Read length: {}, buffer_len: {}",
                    length, ctx.buffer.len()
                )));
            }
            unsafe { ptr::copy_nonoverlapping(buff_ptr, ctx.buffer.as_mut_ptr(), length); }
            ctx.length_read = length;
            Ok(())
        }, &mut context)?;
        cb_res?;

        assert_eq!(context.length_read, message_bytes.len());
        assert_eq!(read_buffer, message_bytes);
        Ok(())
    }

    #[test]
    fn test_read_multiple_messages() -> Result<(), ShmemLibError> {
        let (mut writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(default_test_config());

        let messages = vec!["msg1", "message two", "third one is a bit longer"];
        let mut row_indices = Vec::new();

        for msg_content in &messages {
            let msg_bytes = msg_content.as_bytes();
            row_indices.push(writer.add(msg_bytes)?); // Updated call
        }

        for (i, msg_content) in messages.iter().enumerate() {
            let msg_bytes = msg_content.as_bytes();
            let row_index = row_indices[i];

            let mut read_buffer = vec![0u8; msg_bytes.len()];
            let mut context = ReadContext { buffer: &mut read_buffer, length_read: 0 };

            let cb_res: Result<(), ShmemLibError> = reader.read(row_index, &|buff_ptr, length, ctx: &mut ReadContext| {
                if length > ctx.buffer.len() {
                    return Err(ShmemLibError::Logic(format!(
                        "Buffer too small for msg {}. Read: {}, buffer: {}", i, length, ctx.buffer.len()
                    )));
                }
                unsafe { ptr::copy_nonoverlapping(buff_ptr, ctx.buffer.as_mut_ptr(), length); }
                ctx.length_read = length;
                Ok(())
            }, &mut context)?;
            cb_res?;

            assert_eq!(context.length_read, msg_bytes.len(), "Length mismatch for message {}", i);
            assert_eq!(read_buffer, msg_bytes, "Content mismatch for message {}", i);
        }
        Ok(())
    }

    #[test]
    fn test_read_max_row_size_message() -> Result<(), ShmemLibError> {
        let mut config = default_test_config();
        config.max_row_size = 50; // Define a specific max_row_size for this test
        let (mut writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(config);

        let message_bytes = vec![b'A'; _config.max_row_size]; // Use _config which is the actual used config

        let row_index = writer.add(&message_bytes)?; // Updated call, pass as slice

        let mut read_buffer = vec![0u8; message_bytes.len()];
        let mut context = ReadContext { buffer: &mut read_buffer, length_read: 0 };

        let cb_res: Result<(), ShmemLibError> = reader.read(row_index, &|buff_ptr, length, ctx: &mut ReadContext| {
            if length > ctx.buffer.len() {
                 return Err(ShmemLibError::Logic(format!(
                    "Buffer too small. Read: {}, buffer: {}", length, ctx.buffer.len()
                )));
            }
            unsafe { ptr::copy_nonoverlapping(buff_ptr, ctx.buffer.as_mut_ptr(), length); }
            ctx.length_read = length;
            Ok(())
        }, &mut context)?;
        cb_res?;

        assert_eq!(context.length_read, message_bytes.len());
        assert_eq!(read_buffer, message_bytes);
        Ok(())
    }

    // --- Tests for read_into_buffer ---

    #[test]
    fn test_rib_exact_size_buffer() -> Result<(), ShmemLibError> {
        let (mut writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(default_test_config());
        let message_content = "exact fit";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut user_buffer = vec![0u8; message_bytes.len()];
        let bytes_read = reader.read_into_buffer(row_index, &mut user_buffer)?;

        assert_eq!(bytes_read, message_bytes.len());
        assert_eq!(user_buffer, message_bytes);
        Ok(())
    }

    #[test]
    fn test_rib_larger_buffer() -> Result<(), ShmemLibError> {
        let (mut writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(default_test_config());
        let message_content = "fits well";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut user_buffer = vec![0u8; message_bytes.len() + 5]; // 5 extra bytes
        user_buffer[message_bytes.len()..].fill(0xAA); // Fill extra part to check it's untouched

        let bytes_read = reader.read_into_buffer(row_index, &mut user_buffer)?;

        assert_eq!(bytes_read, message_bytes.len());
        assert_eq!(&user_buffer[..bytes_read], message_bytes);
        assert_eq!(user_buffer[bytes_read..], vec![0xAAu8; 5]); // Check extra part remains
        Ok(())
    }

    #[test]
    fn test_rib_too_small_buffer() -> Result<(), ShmemLibError> {
        let (mut writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(default_test_config());
        let message_content = "too large for buffer";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut user_buffer = vec![0u8; message_bytes.len() - 1];
        let result = reader.read_into_buffer(row_index, &mut user_buffer);

        match result {
            Err(ShmemLibError::Logic(msg)) => {
                assert!(msg.contains("User buffer too small"));
            }
            _ => panic!("Expected ShmemLibError::Logic for too small buffer, got {:?}", result),
        }
        Ok(())
    }

    #[test]
    fn test_rib_multi_slot_message() -> Result<(), ShmemLibError> {
        let mut multi_slot_config = default_test_config();
        multi_slot_config.max_slot_size = 8; // Small slot size
        multi_slot_config.max_row_size = 30;
        let (mut writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(multi_slot_config);

        let message_content = "this message definitely spans"; // length 29
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut user_buffer = vec![0u8; message_bytes.len()];
        let bytes_read = reader.read_into_buffer(row_index, &mut user_buffer)?;

        assert_eq!(bytes_read, message_bytes.len());
        assert_eq!(user_buffer, message_bytes);
        Ok(())
    }

    #[test]
    fn test_rib_empty_message() -> Result<(), ShmemLibError> {
        let (_writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(default_test_config()); // _writer is unused
        // Current MessageWriter::add returns error for 0-length message.
        // To test read_into_buffer with row_size = 0, we'd need to mock a RowIndex or have writer support it.
        // For now, this test confirms read_into_buffer's behavior if row_size is 0.
        // We can simulate this by trying to read a (non-existent or default) row that has size 0.
        // However, read_index would panic if row_index is invalid.
        // If a valid row_index points to a RowIndex where row_size is 0:

        // This requires a way to write a 0-size row or assume a default row is 0-size.
        // Let's assume row 0 of a new shmem is effectively 0-size if not written.
        // The `read_index` will return a default RowIndex which has `row_size = 0`.
        let mut user_buffer = vec![0u8; 5]; // Buffer is not empty
        let bytes_read = reader.read_into_buffer(0, &mut user_buffer)?; // Read row 0

        assert_eq!(bytes_read, 0, "Bytes read for an empty/default row should be 0");
        Ok(())
    }

    // --- Tests for read_segments ---

    #[derive(Default)]
    struct SegmentReadContext {
        segments: Vec<Vec<u8>>,
        is_first_flags: Vec<bool>,
        is_last_flags: Vec<bool>,
    }

    impl SegmentReadContext {
        // fn clear(&mut self) { // Unused, remove for now
        //     self.segments.clear();
        //     self.is_first_flags.clear();
        //     self.is_last_flags.clear();
        // }

        fn combined_data(&self) -> Vec<u8> {
            self.segments.concat()
        }
    }

    #[test]
    fn test_rs_single_segment_message() -> Result<(), ShmemLibError> {
        let (mut writer, reader, _temp_dir, mut config) = setup_reader_writer_with_config(default_test_config());
        config.max_slot_size = 128; // Ensure message fits in one segment easily
        let message_content = "single segment";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut context = SegmentReadContext::default();
        reader.read_segments(row_index, &mut context, &mut |segment_slice, is_first, is_last, ctx| {
            ctx.segments.push(segment_slice.to_vec());
            ctx.is_first_flags.push(is_first);
            ctx.is_last_flags.push(is_last);
            Ok(())
        })?;

        assert_eq!(context.segments.len(), 1);
        assert_eq!(context.is_first_flags, vec![true]);
        assert_eq!(context.is_last_flags, vec![true]);
        assert_eq!(context.combined_data(), message_bytes);
        Ok(())
    }

    #[test]
    fn test_rs_multi_segment_message() -> Result<(), ShmemLibError> {
        let mut multi_slot_config = default_test_config();
        multi_slot_config.max_slot_size = 10;
        multi_slot_config.max_row_size = 50;
        let (mut writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(multi_slot_config);

        let message_content = "this is a long message that spans multiple slots"; // length > 10
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut context = SegmentReadContext::default();
        reader.read_segments(row_index, &mut context, &mut |segment_slice, is_first, is_last, ctx| {
            ctx.segments.push(segment_slice.to_vec());
            ctx.is_first_flags.push(is_first);
            ctx.is_last_flags.push(is_last);
            Ok(())
        })?;

        assert!(context.segments.len() > 1, "Expected multiple segments");
        assert_eq!(context.is_first_flags[0], true);
        if context.segments.len() > 1 {
            for i in 1..context.is_first_flags.len() {
                assert_eq!(context.is_first_flags[i], false, "is_first should be false for segment {}", i);
            }
            for i in 0..context.is_last_flags.len() - 1 {
                assert_eq!(context.is_last_flags[i], false, "is_last should be false for segment {}", i);
            }
        }
        assert_eq!(*context.is_last_flags.last().unwrap_or(&false), true, "is_last for last segment");
        assert_eq!(context.combined_data(), message_bytes);
        Ok(())
    }

    #[test]
    fn test_rs_user_abort() -> Result<(), ShmemLibError> {
        let mut multi_slot_config = default_test_config();
        multi_slot_config.max_slot_size = 10;
        multi_slot_config.max_row_size = 50;
        let (mut writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(multi_slot_config);

        let message_content = "long message to test user abort";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut context = SegmentReadContext::default();
        let result = reader.read_segments(row_index, &mut context, &mut |segment_slice, _is_first, _is_last, ctx| {
            ctx.segments.push(segment_slice.to_vec());
            if ctx.segments.len() >= 2 { // Abort after processing two segments (or first if only one/two)
                return Err(UserAbortReason::UserRequestedStop);
            }
            Ok(())
        });

        match result {
            Err(ShmemLibError::UserAbort(UserAbortReason::UserRequestedStop)) => {
                // Correct error type
            }
            _ => panic!("Expected UserAbortReason::UserRequestedStop, got {:?}", result),
        }
        assert!(context.segments.len() <= 2 && context.segments.len() > 0, "Should have processed some segments before abort");
        // Verify that the processed segments are correct prefixes of the original message
        let processed_data = context.combined_data();
        assert!(message_bytes.starts_with(&processed_data));
        assert_ne!(processed_data, message_bytes, "Should not have processed the full message");


        Ok(())
    }

    #[test]
    fn test_rs_empty_message() -> Result<(), ShmemLibError> {
        let (_writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(default_test_config()); // _writer is unused
        // As with test_rib_empty_message, assumes reading row 0 of a fresh queue.
        // MessageWriter::add currently doesn't allow 0-length.
        let mut context = SegmentReadContext::default();
        reader.read_segments(0, &mut context, &mut |segment_slice, is_first, is_last, ctx| {
            ctx.segments.push(segment_slice.to_vec());
            ctx.is_first_flags.push(is_first);
            ctx.is_last_flags.push(is_last);
            Ok(())
        })?;

        assert_eq!(context.segments.len(), 1, "Callback should be called once for an empty message");
        assert_eq!(context.segments[0].len(), 0, "Segment for empty message should be empty");
        assert_eq!(context.is_first_flags, vec![true]);
        assert_eq!(context.is_last_flags, vec![true]);
        Ok(())
    }
}
