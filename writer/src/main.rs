use serde_derive::{Serialize, Deserialize};
use clap::Clap;

use std::error::Error;
use std::thread;
use std::time::Duration;
use shmem;

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
    println!("{:?}", &cfg.shmem);
    let ctx = &mut shmem::writer_context(&cfg.shmem)?;
    let index = &mut shmem::IndexService::new(ctx);
    run(index);
    Ok(())
}

fn run(index: &mut shmem::IndexService) {
    for x in 0..=10000 {
        let result = index.write(|mut mem| {
            mem.first_row_index = mem.first_row_index + 1;
            mem.first_row_index
        }).unwrap();
        if x % 1000 == 0 {
            println!("{}", result);
        }
        thread::sleep(Duration::from_millis(2));
    }
}
