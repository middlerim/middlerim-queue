use std::error::Error;
use std::time::Instant;

use clap::Parser;

use shmem::reader;

#[derive(clap::Parser)]
#[clap()]
struct Opts {
    #[clap(short = 'c', long = "config", default_value = "middlerim-reader.toml")]
    config: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts: Opts = Opts::parse();
    let cfg: reader::ReaderConfig = confy::load_path(&opts.config)?;
    let reader = &mut reader::MessageReader::new(&cfg)?;
    run(reader)?;
    Ok(())
}

struct ReaderContext {
    called: usize,
}

fn run(reader: &reader::MessageReader) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let f = &|buff: *mut u8, length: usize, ctx: &mut ReaderContext| {
        let message = unsafe {
            let slice = std::slice::from_raw_parts(buff, length);
            String::from_utf8_lossy(slice)
        };
        if ctx.called % 500_000 == 0 {
            eprint!(
                "\rTotal {} ops (current value: {})",
                ctx.called, message
            );
        }
        ctx.called = ctx.called + 1;
        message.eq(" 🐓 🏰 🥕 ")
    };

    let mut row_index = 0;
    let ctx = &mut ReaderContext {
        called: 0,
    };
    loop {
        if row_index >= shmem::MAX_ROWS {
            row_index = 0;
        }
        let is_end = reader.read(row_index, f, ctx)?;
        if is_end {
            println!("\nFinished at row index {}", row_index);
            break;
        }
        row_index = row_index + 1;
    }
    let duration = start.elapsed();
    let iops = ((ctx.called as f64) / (duration.as_millis() as f64)) * 1_000f64;
    println!(
        "\n{:#?}K messages read/s. Total time: {:#?}",
        (iops / 1000f64) as u64,
        duration
    );
    Ok(())
}
