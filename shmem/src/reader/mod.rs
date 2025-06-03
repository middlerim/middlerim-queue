use std::alloc;
use std::ptr;

use serde_derive::{Deserialize, Serialize};

// Import the new error type
use crate::ShmemLibError;
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
        ShmemConfig {
            max_rows: 10,
            max_row_size: 256,
            max_slots: 5, // Keep slots somewhat limited for multi-slot tests
            max_slot_size: 128, // Relatively small slot size for easier multi-slot testing
            ..Default::default() // data_dir and shmem_file_name will be set by setup_reader_writer_with_config
        }
    }


    #[test]
    fn test_read_single_message() -> Result<(), ShmemLibError> {
        let (mut writer, reader, _temp_dir, _config) = setup_reader_writer_with_config(default_test_config()); // Mark config as unused

        let message_content = "hello world";
        let message_bytes = message_content.as_bytes();

        let row_index = writer.add(message_bytes.as_ptr(), message_bytes.len())?;

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

        let row_index = writer.add(message_bytes.as_ptr(), message_bytes.len())?;

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
            row_indices.push(writer.add(msg_bytes.as_ptr(), msg_bytes.len())?);
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

        let row_index = writer.add(message_bytes.as_ptr(), message_bytes.len())?;

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
}
