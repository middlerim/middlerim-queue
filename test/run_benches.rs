use std::fs::File;
use std::io::Write; // BufRead, BufReader, Stdio no longer needed for this run
use std::process::Command;
use std::path::PathBuf;
use tempfile::Builder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting single writer_bench run with --read-after-write...");

    let temp_dir = Builder::new().prefix("shmem_single_bench").tempdir()?;
    let config_file_path: PathBuf = temp_dir.path().join("single_bench_config.toml");
    let shmem_file_name = "single_bench_queue.ipc";

    let config_content = format!(
        "[shmem]\n\
        data_dir = \"{}\"\n\
        shmem_file_name = \"{}\"\n\
        max_rows = 7\n\
        max_row_size = 1024\n\
        max_slots = 10\n\
        max_slot_size = 65536\n", // Keep max_slot_size reasonable
        temp_dir.path().to_str().unwrap().replace("\\", "\\\\"),
        shmem_file_name
    );

    let mut file = File::create(&config_file_path)?;
    file.write_all(config_content.as_bytes())?;
    println!("Temporary benchmark config created at: {}", config_file_path.display());
    // println!("Config content:\n{}", config_content); // Not needed for this focused run

    let cargo_executable = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());

    // --- Running writer_bench with --read-after-write ---
    println!("\n--- Running writer_bench with --read-after-write ---");
    let writer_output = Command::new(&cargo_executable)
        .arg("run")
        .arg("--release")
        .arg("--package")
        .arg("writer_bench")
        .arg("--")
        .arg("--config")
        .arg(&config_file_path)
        .arg("--read-after-write") // Added new flag
        .output()?;

    println!("writer_bench stdout:\n{}", String::from_utf8_lossy(&writer_output.stdout));
    println!("writer_bench stderr:\n{}", String::from_utf8_lossy(&writer_output.stderr));

    if !writer_output.status.success() {
        eprintln!("writer_bench failed with status: {}", writer_output.status);
         return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "writer_bench failed")));
    } else {
        println!("writer_bench completed successfully.");
    }

    println!("\nSingle writer_bench run finished.");
    Ok(())
}
