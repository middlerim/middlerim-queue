use clap::Clap;
use serde_derive::{Deserialize, Serialize};

use shmem;
use std::env;
use std::error::Error;
use std::thread;
use std::time::Duration;

#[derive(Clap)]
#[clap()]
struct Opts {
    #[clap(short = "c", long = "config", default_value = "middlerim-writer.toml")]
    config: String,
}

#[derive(Default, Debug, Serialize, Deserialize)]
struct WriterConfig {
    shmem: shmem::ShmemConfig,
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts: Opts = Opts::parse();
    let cfg: WriterConfig = confy::load_path(&opts.config)?;
    println!("current dir: {:#?}", env::current_dir()?);
    println!("{:?}", &cfg.shmem);
    let ctx = &mut shmem::writer_context(&cfg.shmem)?;
    let shmem_service = &mut shmem::ShmemService::new(ctx);
    run(shmem_service);
    Ok(())
}

fn run(shmem_service: &mut shmem::ShmemService) {
    let max = 30000;
    for x in 0..=max as usize {
        let result = shmem_service
            .write_index(|index| {
                index.first_row_index = x as u16;
                index.end_row_index = (x + 1) as u16;
                let first_slot_index = (index.first_row_index % 200) as u8;
                index.rows[x % 1024] = shmem::RowIndex::new(
                    first_slot_index,
                    100,
                    first_slot_index + (if x % 2 == 0 { 1 } else { 0 }),
                    50000,
                );
                index.first_row_index
            })
            .unwrap();
        if x % 5000 == 0 {
            println!("{}", result);
        }
        thread::sleep(Duration::from_nanos(500_000));
    }
}
