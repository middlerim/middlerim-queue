use std::time::Instant;
use shmem;


fn main() -> Result<(), shmem::SharedMemError> {
    let ctx = &mut shmem::reader_context!();
    let index = &mut shmem::IndexService::new(ctx);
    run(index)
}

fn run(index: &mut shmem::IndexService) -> Result<(), shmem::SharedMemError> {
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

    Ok(())
}
