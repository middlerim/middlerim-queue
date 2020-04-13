use std::thread;
use std::time::Duration;
use shmem;


fn main() -> Result<(), shmem::SharedMemError> {
    let ctx = &mut shmem::writer_context!();
    let index = &mut shmem::IndexService::new(ctx);
    run(index)
}

fn run(index: &mut shmem::IndexService) -> Result<(), shmem::SharedMemError> {
    for _x in 0..=20 {
        let result = index.write(|mut mem| {
            mem.first_row_index = mem.first_row_index + 1;
            mem.first_row_index
        }).unwrap();
        println!("{}", result);
        thread::sleep(Duration::from_millis(500));
    }

    Ok(())
}
