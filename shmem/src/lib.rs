pub mod errors;
mod core;
pub mod reader;
pub mod writer;
mod replica;

pub use errors::ShmemLibError;

pub const MAX_ROWS: usize = core::MAX_ROWS; // This is the compile-time array limit
// MAX_ROW_SIZE is now configurable via ShmemConfig, so remove this const export.
// Users should get max_row_size from their ShmemConfig instance.
// pub const MAX_ROW_SIZE: usize = core::MAX_ROW_SIZE;
