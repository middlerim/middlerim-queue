use std::thread;
use std::time::Duration;
use std::collections::HashMap;

use shmem::core::ShmemConfig;
use shmem::reader::{MessageReader, ReaderConfig};
use shmem::writer::{MessageWriter, WriterConfig};
use shmem::ShmemLibError;

use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting shmem queue simulation...");

    let temp_dir = tempdir()?;
    let shmem_file_name = "test_queue.ipc".to_string();

    let config = ShmemConfig::builder()
        .data_dir(temp_dir.path().to_str().unwrap().to_string())
        .shmem_file_name(shmem_file_name)
        .max_rows(100) // Example value
        .max_row_size(1024) // Example value, ensure it's large enough for messages
        .max_slots(10) // Example value
        .max_slot_size(65536) // Example value, from shmem::core::COMPILE_TIME_MAX_SLOT_SIZE
        .build()?;

    // It's important that the writer context is created first to initialize shared memory
    // For this test, we'll create writer then reader.
    // In a real scenario, the writer might create/initialize shmem, and reader would open existing.
    // shmem::core::writer_context(&config)?; // Ensure shmem is created if writer does it implicitly.

    let writer_config = WriterConfig { shmem: config.clone() };
    let reader_config = ReaderConfig { shmem: config.clone() };

    let mut writer = MessageWriter::new(&writer_config)?;
    let reader = MessageReader::new(&reader_config)?;

    let messages_to_send = vec![
        "Hello from shmem!".to_string(),
        "This is a test message.".to_string(),
        " trzecia wiadomosc po polsku ".to_string(),
        "Short".to_string(),
        "A bit longer message to ensure it fits.".to_string(),
    ];

    let mut sent_messages_map: HashMap<usize, String> = HashMap::new();

    let writer_thread = {
        let messages_to_send = messages_to_send.clone();
        // MessageWriter might not be Send, if it contains non-Send types (e.g. raw pointers directly).
        // Box<ShmemService> inside MessageWriter contains Shmem which has *mut u8.
        // For thread safety with raw pointers, Send/Sync must be implemented carefully or types wrapped.
        // However, for this test, if MessageWriter is not Send, we'd have to do writes in main thread before reads.
        // Let's assume for now it might work, or adjust if MessageWriter is not Send.
        // If it's not Send, we'll remove the thread and do it sequentially.

        // Due to potential non-Send types in MessageWriter (raw pointers in Shmem),
        // let's perform write operations in the main thread for simplicity in this test.
        // If threading is essential, MessageWriter would need to be made Send + Sync,
        // possibly by wrapping raw pointers or using appropriate synchronization primitives.

        // Simulate writer actions (will run in main thread)
        for (i, msg_str) in messages_to_send.iter().enumerate() {
            println!("[Writer] Sending message {}: '{}'", i, msg_str);
            match writer.add(msg_str.as_bytes()) {
                Ok(row_index) => {
                    sent_messages_map.insert(row_index, msg_str.clone());
                    println!("[Writer] Sent message {} to row_index {}", i, row_index);
                }
                Err(e) => {
                    eprintln!("[Writer] Error sending message {}: {:?}", i, e);
                    // Depending on the error, we might want to stop or retry.
                    // For this test, we'll print error and continue if possible, or panic.
                    return Err(Box::new(e));
                }
            }
            thread::sleep(Duration::from_millis(10)); // Small delay
        }
        writer.close();
        println!("[Writer] Writer closed.");
        // The writer thread would normally return a result or the sent_messages_map
        Ok::<_, ShmemLibError>(sent_messages_map)
    };

    // In this revised single-threaded approach for writes, writer_thread isn't a thread.
    // We get sent_messages_map directly.
    let final_sent_messages_map = match writer_thread {
        Ok(map) => map,
        Err(e) => {
            eprintln!("[Main] Writer phase failed: {:?}", e);
            return Err(Box::new(e));
        }
    };

    println!("[Reader] Starting to read messages...");
    let mut all_match = true;

    // Assuming row indices are sequential starting from 0 for this test.
    // This might not hold if writer supports non-sequential/sparse row indices.
    // The current writer.add returns the actual row_index.
    // We should iterate based on the number of messages sent and use the row_indices from sent_messages_map.

    let mut received_count = 0;
    for (row_index, original_msg_str) in &final_sent_messages_map {
        println!("[Reader] Attempting to read from row_index: {}", row_index);

        // Define a struct to hold the buffer for the callback
        struct ReadContext {
            data: Vec<u8>,
        }

        let mut context = ReadContext { data: Vec::new() };

        match reader.read(*row_index, |ptr, len, ctx: &mut ReadContext| {
            unsafe {
                ctx.data.extend_from_slice(std::slice::from_raw_parts(ptr, len));
            }
        }, &mut context) {
            Ok(_) => {
                let received_msg_str = String::from_utf8(context.data)?;
                println!("[Reader] Received (row {}): '{}'", row_index, received_msg_str);
                if &received_msg_str == original_msg_str {
                    println!("[Reader] Message for row {} matches!", row_index);
                } else {
                    eprintln!("[Reader] Mismatch! Row {}: Expected '{}', Got '{}'", row_index, original_msg_str, received_msg_str);
                    all_match = false;
                }
                received_count += 1;
            }
            Err(e) => {
                eprintln!("[Reader] Error reading message from row {}: {:?}", row_index, e);
                all_match = false;
                // Depending on test design, might want to break or continue.
            }
        }
    }

    reader.close();
    println!("[Reader] Reader closed.");

    if received_count != messages_to_send.len() {
        eprintln!("[Main] Error: Did not receive all messages. Sent: {}, Received: {}", messages_to_send.len(), received_count);
        all_match = false;
    }

    if all_match && received_count == messages_to_send.len() {
        println!("\nSimulation successful! All messages sent and received correctly.");
    } else {
        eprintln!("\nSimulation failed. Some messages were not received correctly or errors occurred.");
        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Simulation failed")));
    }

    Ok(())
}
