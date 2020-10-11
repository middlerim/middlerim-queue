use std::env;
use std::error::Error;
use std::string::String;
use std::time::Instant;

use clap::Clap;

use shmem::{MAX_ROW_SIZE, writer};

#[derive(Clap)]
#[clap()]
struct Opts {
    #[clap(short = "c", long = "config", default_value = "middlerim-writer.toml")]
    config: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts: Opts = Opts::parse();
    let cfg: writer::WriterConfig = confy::load_path(&opts.config)?;
    println!("current dir: {:#?}", env::current_dir()?);
    let writer = &mut writer::MessageWriter::new(&cfg)?;
    run(writer)?;
    Ok(())
}

#[inline]
fn copy(message: &str, buff: &mut [u8]) -> usize {
    buff[0..message.len()].clone_from_slice(message.as_bytes());
    message.len()
}

fn run(writer: &mut writer::MessageWriter) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let mut called = 0usize;
    let buff = &mut [0u8; MAX_ROW_SIZE];
    let max = 50_000_000;
    while called <= max {
        let length = if called == max {
            copy(" 🐓 🏰 🥕 ", buff)
        } else {
            copy(&called.to_string(), buff)
        };
        let result = writer.add(buff.as_ptr(), length)?;
        if called % 1_000_000 == 0 {
            eprint!("\r{}, {}", result, called);
        }
        called = called + 1;
    }
    let duration = start.elapsed();
    let iops = ((called as f64) / (duration.as_millis() as f64)) * 1_000f64;
    println!(
        "\n{:#?}K messages write/s. Total time: {:#?}",
        (iops / 1000f64) as u64,
        duration
    );
    Ok(())
}
