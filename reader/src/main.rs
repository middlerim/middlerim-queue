use clap::Clap;
use serde_derive::{Deserialize, Serialize};

use shmem;
use std::cmp;
use std::env;
use std::error::Error;
use std::thread;
use std::time::Instant;

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
    let shmem_service = &mut shmem::ShmemService::new(ctx);
    run(shmem_service)?;
    Ok(())
}

fn run(shmem_service: &mut shmem::ShmemService) -> Result<(), Box<dyn Error>> {
    let start = Instant::now();

    let mut count = 0u64;
    let mut first_row_index = 0u16;
    let mut data_buff = [0u8; shmem::MAX_ROW_SIZE];

    while first_row_index < 30000 {
        count += 1;
        let result = shmem_service
            .read_index(|index| {
                (
                    index.first_row_index,
                    index.end_row_index,
                    index.rows[(first_row_index % 1024) as usize].clone(),
                )
            })
            .unwrap();
        first_row_index = result.0;
        let end_row_index = result.1;

        let row_index = (first_row_index % 1024) as usize;
        let row = &result.2;

        let mut curr_buff_index = 0;
        for slot_index in row.start_slot_index..row.end_slot_index {
            let is_end_slot = slot_index == row.end_slot_index;
            let start_data_index = if slot_index == row.start_slot_index {
                row.start_data_index
            } else {
                0
            };
            let end_data_index = if slot_index == row.end_slot_index {
                row.end_data_index
            } else {
                shmem::MAX_SLOT_SIZE - 1
            };
            let slot_size = end_data_index - start_data_index;
            shmem_service.read_slot(slot_index, |slot| {
                data_buff[curr_buff_index..(curr_buff_index + slot_size)]
                    .copy_from_slice(&slot.data[start_data_index..end_data_index]);
                curr_buff_index += slot_size;
            })?;
        }

        if first_row_index % 20000 == 0 {
            println!(
                "{}, {}, {:?}, {}",
                first_row_index, end_row_index, result.2, data_buff[0]
            );
        }
    }
    let duration = start.elapsed();
    let iops = ((count as f64) / (duration.as_millis() as f64)) * 1_000f64;
    println!(
        "IOPS : {:#?}K, time: {:#?}",
        (iops / 1000f64) as u64,
        duration
    );
    Ok(())
}
