use std::env;
use std::error::Error;
use std::string::String;
use std::time::Instant;

use clap::{self, Parser};

use shmem::writer;
use shmem::reader::{ReaderConfig, MessageReader}; // Added reader imports
use shmem::ShmemLibError; // Added ShmemLibError import

use std::io::{self, BufRead};

#[derive(clap::Parser)]
#[clap()]
struct Opts {
    #[clap(short = 'c', long = "config", default_value = "middlerim-writer.toml")]
    config: String,
    #[clap(long = "pause-after-write")]
    pause_after_write: bool,
    #[clap(long)] // New flag: read_after_write
    read_after_write: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts: Opts = Opts::parse();
    let writer_cfg: writer::WriterConfig = confy::load_path(&opts.config)?; // Renamed for clarity
    println!("current dir: {:#?}", env::current_dir()?);
    let writer = &mut writer::MessageWriter::new(&writer_cfg)?;
    run(writer, &opts)?; // Pass full opts to run
    Ok(())
}

#[inline]
fn copy(message: &str, buff: &mut [u8]) -> usize {
    buff[0..message.len()].clone_from_slice(message.as_bytes());
    message.len()
}

// Helper struct for reader callback context if needed for more complex data handling
struct BenchReadContext {
    data: Vec<u8>,
    message_description: String,
}

fn run(writer: &mut writer::MessageWriter, opts: &Opts) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let mut called = 0usize;

    let mut actual_written_messages_in_order: Vec<String> = Vec::new();

    let buff = &mut [0u8; shmem::core::COMPILE_TIME_MAX_SLOT_SIZE];

    // Determine number of messages to write based on flags
    // If --read-after-write is set, write a smaller number of messages (e.g., 10, or 2*max_rows from config)
    // to robustly test wrap-around. Otherwise, write the full benchmark amount.
    let num_messages_to_write = if opts.read_after_write {
        // To test wrap-around, we need max_rows from config.
        // This is tricky as writer_cfg is not directly available here without parsing again or passing ShmemConfig.
        // For now, let's use a fixed number for this test, assuming max_rows=7 from orchestrator.
        10 // Write 10 messages if max_rows is 7 to ensure wrap-around
    } else {
        10_000_000 // Full benchmark count
    };
    let write_end_marker = !opts.read_after_write; // Only write end marker for full benchmark

    println!("[Write Phase] Will write {} messages for this run.", num_messages_to_write);

    while called < num_messages_to_write {
        let current_message_str = called.to_string();
        if opts.read_after_write { // Store all messages if we are going to verify them
            actual_written_messages_in_order.push(current_message_str.clone());
        }
        let length = copy(&current_message_str, buff);
        let row_index_written = writer.add(&buff[0..length])?;

        if opts.read_after_write {
             println!("[Write Phase] Message '{}' written to row_index: {}", current_message_str, row_index_written);
        } else {
            if called % 1_000_000 == 0 {
                 eprint!("\rTotal {} ops (written to row_index: {})", called, row_index_written);
            }
        }
        called = called + 1;
    }

    if write_end_marker && called == 10_000_000 { // Original benchmark end marker logic
        let current_message_str = " 🐓 🏰 🥕 ".to_string();
        if opts.read_after_write { // If also reading, store it
             actual_written_messages_in_order.push(current_message_str.clone());
        }
        let length = copy(&current_message_str, buff);
        let row_index_written = writer.add(&buff[0..length])?;
        eprint!("\rTotal {} ops (written to row_index: {})", called, row_index_written);
        called += 1;
    }

    let duration = start.elapsed();
    let iops = ((called as f64) / (duration.as_millis() as f64)) * 1_000f64;
    println!(
        "\n{:#?}K messages write/s. Total time: {:#?}",
        (iops / 1000f64) as u64,
        duration
    );

    writer.close(); // Close writer before attempting to read
    println!("[Writer] Writer closed.");

    if opts.read_after_write {
        println!("\n--- Read After Write Phase ---");

        // Re-parse the config to get ShmemConfig for its max_rows value.
        // This is a bit redundant but ensures we use the same config source.
        // Alternatively, pass writer_cfg.shmem down or specific fields like max_rows.
        let writer_cfg_for_shmem: writer::WriterConfig = confy::load_path(&opts.config)?;
        let shmem_cfg_for_index = writer_cfg_for_shmem.shmem; // Assuming WriterConfig has pub shmem field
        let cfg_max_rows = shmem_cfg_for_index.max_rows;

        // Create a ShmemService to read the index directly
        let shmem_service_for_index = match shmem::core::reader_context(&shmem_cfg_for_index) {
            Ok(service_val) => shmem::core::ShmemService::new(service_val),
            Err(e) => {
                eprintln!("[Read Phase] Error creating ShmemService for reading index: {:?}", e);
                return Err(Box::new(e));
            }
        };

        let (first_row_idx, current_count, end_row_idx_from_shmem) = shmem_service_for_index.read_index(|index_ptr| {
             (index_ptr.first_row_index, index_ptr.count, index_ptr.end_row_index)
        })?;

        shmem_service_for_index.close();

        println!("[Read Phase] Index state from shared memory: first_row_index={}, end_row_index={}, count={}",
                 first_row_idx, end_row_idx_from_shmem, current_count);

        let reader_cfg_actual: ReaderConfig = confy::load_path(&opts.config)?;
        let reader = MessageReader::new(&reader_cfg_actual)?;
        println!("[Read Phase] MessageReader initialized for reading messages.");

        let mut messages_ok = true;
        if current_count == 0 {
            println!("[Read Phase] No messages in queue to verify (count is 0 according to Index).");
        }

        for k in 0..current_count {
            let current_shmem_idx_to_read = (first_row_idx + k) % cfg_max_rows;

            // Determine what the original message string should be.
            // `actual_written_messages_in_order` stores "0", "1", "2", ...
            // If buffer wrapped, first_row_idx points to the start of current valid messages.
            // Example: wrote 0..9 (10 msgs), max_rows=7. count=7.
            // first_idx should point to where "3" was written. end_idx where "9" was.
            // Messages in queue: "3", "4", "5", "6", "7", "8", "9".
            // The message at `first_row_idx` is `actual_written_messages_in_order[actual_written_messages_in_order.len() - count]`.
            // So, the message at `(first_row_idx + k)` is `actual_written_messages_in_order[ (actual_written_messages_in_order.len() - count) + k ]`.
            let expected_message_original_idx = (actual_written_messages_in_order.len() - current_count) + k;
            let expected_content = actual_written_messages_in_order.get(expected_message_original_idx)
                .cloned()
                .unwrap_or_else(|| format!("BUG_NO_EXPECTED_MSG_AT_IDX_{}", expected_message_original_idx));

            println!("[Read Phase] Reading shmem_idx: {} (expected original message: '{}')",
                     current_shmem_idx_to_read, expected_content);

            let mut context = BenchReadContext { data: Vec::new(), message_description: String::new() };
            match reader.read(current_shmem_idx_to_read, |ptr, len, ctx: &mut BenchReadContext| {
                unsafe { ctx.data.extend_from_slice(std::slice::from_raw_parts(ptr, len)); }
            }, &mut context) {
                Ok(_) => {
                    let received_str = String::from_utf8_lossy(&context.data);
                    println!("[Read Phase] Read shmem_idx {}: '{}'", current_shmem_idx_to_read, received_str);
                    if received_str != expected_content {
                        eprintln!("[Read Phase] MISMATCH at shmem_idx {}: GOT '{}', EXPECTED '{}'",
                                 current_shmem_idx_to_read, received_str, expected_content);
                        messages_ok = false;
                    } else {
                        println!("[Read Phase] Content for shmem_idx {} matches.", current_shmem_idx_to_read);
                    }
                }
                Err(e) => {
                    eprintln!("[Read Phase] Error reading shmem_idx {}: {:?}", current_shmem_idx_to_read, e);
                    messages_ok = false;
                }
            }
        }
        if messages_ok && current_count > 0 {
            println!("[Read Phase] All read messages verified successfully!");
        } else if current_count == 0 && actual_written_messages_in_order.len() > 0 {
             eprintln!("[Read Phase] Read 0 messages, but writer wrote {}. Index count issue.", actual_written_messages_in_order.len());
        } else if !messages_ok {
            eprintln!("[Read Phase] Some messages did not match or errors occurred during read.");
        }


        reader.close();
        println!("[Read Phase] Reader closed.");
    }

    if opts.pause_after_write {
        println!("[Writer] Operations finished. Pausing. Press Enter to exit fully.");
        let mut stdin = io::stdin();
        let _ = stdin.read_line(&mut String::new())?;
        println!("[Writer] Exiting.");
    }
    Ok(())
}
