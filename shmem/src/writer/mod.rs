use std::cell::RefCell;
use std::error::Error;
use std::ptr;

use serde_derive::{Deserialize, Serialize};

use super::core::*;
use super::replica;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct WriterConfig {
    shmem: ShmemConfig,
}

pub trait AfterAdd {
    fn apply(&mut self, row_index: usize, message: *const u8, length: usize) -> ();
}

pub struct MessageWriter {
    pub shmem_service: Box<ShmemService>,
    pub callback_after_add: Vec<RefCell<Box<dyn AfterAdd>>>,
}

static FIRST_ROW: RowIndex = RowIndex {
    start_slot_index: 0,
    start_data_index: 0,
    end_slot_index: 0,
    end_data_index: 0,
    row_size: 0,
};

impl MessageWriter {
    pub fn new(cfg: &WriterConfig) -> Result<MessageWriter, Box<dyn Error>> {
        let ctx = writer_context(&cfg.shmem)?;
        let shmem_service = ShmemService::new(ctx);

        let mut writer = MessageWriter {
            shmem_service: shmem_service,
            callback_after_add: Vec::<RefCell<Box<dyn AfterAdd>>>::with_capacity(2),
        };
        replica::setup(&mut writer);

        writer.callback_after_add.shrink_to_fit();
        Ok(writer)
    }

    pub fn close(&self) -> () {
        self.shmem_service.close()
    }

    #[inline]
    fn get_next_row(index: &Index, length: usize) -> (usize, RowIndex) {
        let (last_row, next_row_index) = if index.end_row_index >= MAX_ROWS - 1 {
            (&FIRST_ROW, 0)
        } else {
            (&index.rows[index.end_row_index], index.end_row_index + 1)
        };
        let (next_slot_index, next_data_index) = if last_row.end_data_index < MAX_SLOT_SIZE {
            (last_row.end_slot_index, last_row.end_data_index)
        } else {
            (last_row.end_slot_index, 0)
        };
        let mut end_slot_index = next_slot_index + ((next_data_index + length) / MAX_SLOT_SIZE);
        let mut end_data_index = (next_data_index + length) % MAX_SLOT_SIZE;
        if (end_data_index == 0) {
            end_data_index = MAX_SLOT_SIZE;
            end_slot_index = end_slot_index - 1;
        }
        (next_row_index, RowIndex::new(
            next_slot_index,
            next_data_index,
            end_slot_index,
            end_data_index,
        ))
    }

    pub fn add(&mut self, message: *const u8, length: usize) -> Result<usize, Box<dyn Error>> {
        let (next_row_index, row) = self.shmem_service.write_index(|index| {
            let (next_row_index, row) = MessageWriter::get_next_row(index, length);
            index.end_row_index = next_row_index;
            index.rows[next_row_index] = row;
            (next_row_index, row)
        })?;
        let mut curr_message_index = 0;
        for slot_index in row.start_slot_index..=row.end_slot_index {
            let start_data_index = if slot_index == row.start_slot_index {
                row.start_data_index
            } else {
                0
            };
            let end_data_index = if slot_index == row.end_slot_index {
                row.end_data_index
            } else {
                MAX_SLOT_SIZE
            };
            let pertial_row_size = end_data_index - start_data_index;
            self.shmem_service.write_slot(slot_index, |slot| {
                unsafe {
                    let src_p = message.add(curr_message_index);
                    let dest_p = slot.data.as_mut_ptr().add(start_data_index);
                    ptr::copy(src_p, dest_p, pertial_row_size);
                }
            })?;

            curr_message_index += pertial_row_size;
        }

        for cb in self.callback_after_add.iter() {
            cb.borrow_mut().apply(next_row_index, message, length);
        }
        Ok(next_row_index)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_row_1() -> Result<(), Box<dyn Error>> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            rows: [Default::default(); MAX_ROWS],
        };
        let (next_row_index, row) = MessageWriter::get_next_row(index, 1);
        let expected_row = RowIndex {
            start_slot_index: 0,
            start_data_index: 0,
            end_slot_index: 0,
            end_data_index: 1,
            row_size: 1,
        };
        assert_eq!(next_row_index, 1);
        assert_eq!(row, expected_row);
        Ok(())
    }

    #[test]
    fn next_row_max() -> Result<(), Box<dyn Error>> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            rows: [Default::default(); MAX_ROWS],
        };
        let (next_row_index, row) = MessageWriter::get_next_row(index, MAX_SLOT_SIZE);
        let expected_row = RowIndex {
            start_slot_index: 0,
            start_data_index: 0,
            end_slot_index: 0,
            end_data_index: MAX_SLOT_SIZE,
            row_size: MAX_SLOT_SIZE,
        };
        assert_eq!(next_row_index, 1);
        assert_eq!(row, expected_row);
        Ok(())
    }

    #[test]
    fn next_row_multiple_slots() -> Result<(), Box<dyn Error>> {
        let index = &mut Index {
            first_row_index: 0,
            end_row_index: 0,
            rows: [Default::default(); MAX_ROWS],
        };
        let length = MAX_SLOT_SIZE + 1;
        let (next_row_index, row) = MessageWriter::get_next_row(index, length);
        let expected_row = RowIndex {
            start_slot_index: 0,
            start_data_index: 0,
            end_slot_index: 1,
            end_data_index: 1,
            row_size: length,
        };
        assert_eq!(next_row_index, 1);
        assert_eq!(row, expected_row);
        Ok(())
    }
}
