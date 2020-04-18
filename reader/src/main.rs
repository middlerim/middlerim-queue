use serde_derive::{Serialize, Deserialize};
use clap::Clap;

use std::error::Error;
use std::thread;
use std::time::Instant;
use shmem;

#[derive(Clap)]
#[clap()]
struct Opts {
    #[clap(short = "c", long = "config", default_value = "middlerim-reader.toml")]
    config: String,
}

#[derive(Default, Debug, Serialize, Deserialize)]
struct ReaderConfig {
    shmem: shmem::ShmemConfig,
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts: Opts = Opts::parse();
    let cfg: ReaderConfig = confy::load_path(&opts.config)?;
    let ctx = &mut shmem::reader_context(&cfg.shmem)?;
    let index = &mut shmem::IndexService::new(ctx);
    run(index);
    Ok(())
}

fn run(index: &mut shmem::IndexService) {
    let start = Instant::now();
    
    let mut count = 0u64;
    let mut result = 0;
    while result <= 20 {
        result = index.read(|mem| {
            count += 1;
            mem.first_row_index
        }).unwrap();
    }
    let duration = start.elapsed();
    let iops = ((count as f64) / (duration.as_millis() as f64)) * 1_000f64;
    println!("IOPS : {:?}, time: {:?}", iops as u64, duration);
}
