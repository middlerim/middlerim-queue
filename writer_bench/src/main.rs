use std::env;
use std::error::Error;
use std::string::String;
use std::time::Instant;

use clap::{self, Parser};

use shmem::writer;
use shmem::reader::{ReaderConfig, MessageReader};
// use shmem::ShmemLibError; // Removed unused import

use std::io::{self}; // Removed unused BufRead

#[derive(clap::Parser)]
#[clap()]
struct Opts {
    #[clap(short = 'c', long = "config", default_value = "middlerim-writer.toml")]
    config: String,
    #[clap(long = "pause-after-write")]
    pause_after_write: bool,
    #[clap(long)]
    read_after_write: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts: Opts = Opts::parse();
    let writer_cfg: writer::WriterConfig = confy::load_path(&opts.config)?;
    println!("current dir: {:#?}", env::current_dir()?);
    let writer = &mut writer::MessageWriter::new(&writer_cfg)?;
    run(writer, &opts)?;
    Ok(())
}

#[inline]
fn copy(message: &str, buff: &mut [u8]) -> usize {
    buff[0..message.len()].clone_from_slice(message.as_bytes());
    message.len()
}

struct BenchReadContext {
    data: Vec<u8>,
    // message_description: String, // Removed unused field
}

fn run(writer: &mut writer::MessageWriter, opts: &Opts) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let mut called = 0usize;

    let mut actual_written_messages_in_order: Vec<String> = Vec::new();

    let buff = &mut [0u8; shmem::core::COMPILE_TIME_MAX_SLOT_SIZE];

    let num_messages_to_write = if opts.read_after_write {
        10
    } else {
        10_000_000
    };
    let write_end_marker = !opts.read_after_write;

    println!("[Write Phase] Will write {} messages for this run.", num_messages_to_write);

    while called < num_messages_to_write {
        let current_message_str = called.to_string();
        if opts.read_after_write {
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

    if write_end_marker && called == 10_000_000 {
        let current_message_str = " 🐓 🏰 🥕 ".to_string();
        if opts.read_after_write {
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

    writer.close();
    println!("[Writer] Writer closed.");

    if opts.read_after_write {
        println!("\n--- Read After Write Phase ---");

        let writer_cfg_for_shmem: writer::WriterConfig = confy::load_path(&opts.config)?;
        let shmem_cfg_for_index = writer_cfg_for_shmem.shmem;
        let cfg_max_rows = shmem_cfg_for_index.max_rows;

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
            let expected_message_original_idx = (actual_written_messages_in_order.len() - current_count) + k;
            let expected_content = actual_written_messages_in_order.get(expected_message_original_idx)
                .cloned()
                .unwrap_or_else(|| format!("BUG_NO_EXPECTED_MSG_AT_IDX_{}", expected_message_original_idx));

            println!("[Read Phase] Reading shmem_idx: {} (expected original message: '{}')",
                     current_shmem_idx_to_read, expected_content);

            let mut context = BenchReadContext { data: Vec::new() }; // message_description removed
            match reader.read(current_shmem_idx_to_read, |ptr, len, ctx: &mut BenchReadContext| {
                unsafe { ctx.data.extend_from_slice(std::slice::from_raw_parts(ptr, len)); }
                Ok::<(), shmem::ShmemLibError>(()) // Explicit Ok type
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
        let stdin = io::stdin(); // Removed mut
        let _ = stdin.read_line(&mut String::new())?; // read_line needs &mut self, but stdin is not used elsewhere.
                                                      // This might need stdin to be mut if read_line itself needs mut on Stdin object.
                                                      // Stdin::read_line takes &mut self. So, `stdin` needs to be mutable.
                                                      // The warning was "variable does not need to be mutable".
                                                      // This implies `stdin.read_line` was perhaps the only use.
                                                      // If stdin is `let stdin = io::stdin();`, then `stdin.read_line()` won't compile.
                                                      // The original code `let mut stdin = io::stdin();` and `stdin.read_line(...)` is correct.
                                                      // The warning might be if `read_line` was NOT called.
                                                      // The provided code DOES call read_line.
                                                      // I will keep `let mut stdin` as it is necessary.
                                                      // The warning fix should be to ensure `stdin` is used if declared mut. It is.
                                                      // Perhaps the warning was from a state where read_line was commented out.
                                                      // For now, I will assume the original `let mut stdin` is correct and the warning was context-dependent.
                                                      // To be safe and follow the instruction to fix the warning: if the warning says `stdin`
                                                      // does not need to be mutable, it means its state is not modified in a way that requires `mut`.
                                                      // However, `read_line` *does* modify `String::new()`, not `stdin` itself in terms of bindings.
                                                      // `std::io::Stdin`'s `read_line` method takes `&self`, not `&mut self`.
                                                      // So, `let stdin = io::stdin();` should be correct. The `mut` is not needed for `stdin`.
    }
    Ok(())
}
