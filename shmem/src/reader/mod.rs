use std::alloc;
use std::ptr;

use serde_derive::{Deserialize, Serialize};

// Import the new error type
use crate::ShmemLibError;
use crate::errors::UserAbortReason;
use super::core::*;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct ReaderConfig {
    pub shmem: ShmemConfig,
}

pub struct MessageReader {
    shmem_service: Box<ShmemService>,
    config_max_slot_size: usize,
    config_max_row_size: usize,
    config_max_rows: usize,
    config_max_slots: usize, // Added field
}

impl MessageReader {
    pub fn new(cfg: &ReaderConfig) -> Result<MessageReader, ShmemLibError> {
        if cfg.shmem.max_slot_size == 0 || cfg.shmem.max_slot_size > COMPILE_TIME_MAX_SLOT_SIZE {
             return Err(ShmemLibError::Logic(format!(
                "Configured max_slot_size ({}) must be > 0 and <= compile-time COMPILE_TIME_MAX_SLOT_SIZE ({})",
                cfg.shmem.max_slot_size, COMPILE_TIME_MAX_SLOT_SIZE
            )));
        }
        if cfg.shmem.max_row_size == 0 {
            return Err(ShmemLibError::Logic("Configured max_row_size must be > 0".to_string()));
        }
        // cfg.shmem.max_rows and cfg.shmem.max_slots are validated in ShmemConfig::build()

        let ctx = reader_context(&cfg.shmem)?;
        let shmem_service = ShmemService::new(ctx);
        Ok(MessageReader {
            shmem_service: shmem_service,
            config_max_slot_size: cfg.shmem.max_slot_size,
            config_max_row_size: cfg.shmem.max_row_size,
            config_max_rows: cfg.shmem.max_rows,
            config_max_slots: cfg.shmem.max_slots, // Initialize new field
        })
    }

    pub fn close(&self) -> () {
        self.shmem_service.close()
    }

    pub fn read<F, C, R>(
        &self,
        row_index: usize,
        mut f: F,
        context: &mut C,
    ) -> Result<R, ShmemLibError>
    where
        F: FnMut(*mut u8, usize, &mut C) -> R,
    {
        if row_index >= self.config_max_rows {
            return Err(ShmemLibError::Logic(format!(
                "Provided row_index ({}) is out of bounds for configured max_rows ({}). Potential torn Index.end_row_index read by caller.",
                row_index, self.config_max_rows
            )));
        }

        let row = self
            .shmem_service
            .read_index(|index| index.rows[row_index])?;

        // Validate RowIndex data against stored configuration from ReaderConfig
        if row.row_size > self.config_max_row_size {
            return Err(ShmemLibError::Logic(format!(
                "RowIndex.row_size ({}) exceeds configured max_row_size ({}) for row_index {}. Potential torn Index read.",
                row.row_size, self.config_max_row_size, row_index
            )));
        }

        if row.start_data_index >= self.config_max_slot_size {
             return Err(ShmemLibError::Logic(format!(
                "RowIndex.start_data_index ({}) is out of bounds for configured max_slot_size ({}) for row_index {}. Potential torn Index read.",
                row.start_data_index, self.config_max_slot_size, row_index
            )));
        }
        if row.end_data_index > self.config_max_slot_size {
            return Err(ShmemLibError::Logic(format!(
               "RowIndex.end_data_index ({}) is out of bounds for configured max_slot_size ({}) for row_index {}. Potential torn Index read.",
               row.end_data_index, self.config_max_slot_size, row_index
           )));
        }
        if row.start_slot_index == row.end_slot_index &&
           row.end_data_index == 0 &&
           row.row_size > 0 {
            if row.start_data_index + row.row_size > self.config_max_slot_size {
                 return Err(ShmemLibError::Logic(format!(
                    "RowIndex for single-slot message has end_data_index=0 but row_size={} starting at {} for row_index {}. Inconsistent.",
                    row.row_size, row.start_data_index, row_index
                )));
            }
        }
        if row.start_slot_index == row.end_slot_index &&
           row.start_data_index >= row.end_data_index &&
           row.row_size > 0 {
            return Err(ShmemLibError::Logic(format!(
                "RowIndex for single-slot message has start_data_index ({}) >= end_data_index ({}) with row_size={} for row_index {}. Inconsistent.",
                row.start_data_index, row.end_data_index, row.row_size, row_index
            )));
        }

        let layout = unsafe { alloc::Layout::from_size_align_unchecked(row.row_size, 1) };
        let buff = unsafe { alloc::alloc(layout) };
        if buff.is_null() && row.row_size > 0 {
            return Err(ShmemLibError::Logic("Memory allocation failed in reader".to_string()));
        }

        let mut curr_buff_index: usize = 0;

        if row.row_size > 0 {
            if row.start_slot_index >= self.config_max_slots {
                return Err(ShmemLibError::Logic(format!(
                    "RowIndex.start_slot_index ({}) is out of bounds for configured max_slots ({}). RowIndex: {:?}",
                    row.start_slot_index, self.config_max_slots, row
                )));
            }
            // end_slot_index is inclusive in the loop, so it must be < config_max_slots
            if row.end_slot_index >= self.config_max_slots {
                return Err(ShmemLibError::Logic(format!(
                    "RowIndex.end_slot_index ({}) is out of bounds for configured max_slots ({}). RowIndex: {:?}",
                    row.end_slot_index, self.config_max_slots, row
                )));
            }
            if row.end_slot_index < row.start_slot_index {
                 return Err(ShmemLibError::Logic(format!(
                    "RowIndex.end_slot_index ({}) < RowIndex.start_slot_index ({}) while row_size is non-zero ({}). Potential torn Index read.",
                    row.end_slot_index, row.start_slot_index, row.row_size
                )));
            }

            for slot_index in row.start_slot_index..=row.end_slot_index {
                // This check is theoretically redundant due to the checks before the loop,
                // but kept for safety during direct iteration if the meaning of row.end_slot_index changes.
                if slot_index >= self.config_max_slots {
                    return Err(ShmemLibError::Logic(format!(
                        "Loop variable slot_index ({}) is out of bounds for configured max_slots ({}). RowIndex: {:?}",
                        slot_index, self.config_max_slots, row
                    )));
                }
                let current_segment_start_data_index = if slot_index == row.start_slot_index {
                    row.start_data_index
                } else {
                    0
                };
                let current_segment_end_data_index = if slot_index == row.end_slot_index {
                    row.end_data_index
                } else {
                    self.config_max_slot_size
                };

                let pertial_row_size = if current_segment_start_data_index > current_segment_end_data_index {
                    return Err(ShmemLibError::Logic(format!(
                        "Inconsistent segment data indices: start_data_index ({}) > end_data_index ({}) for slot {}. RowIndex: {:?}",
                        current_segment_start_data_index, current_segment_end_data_index, slot_index, row
                    )));
                } else {
                    current_segment_end_data_index - current_segment_start_data_index
                };

                if pertial_row_size > 0 {
                    if curr_buff_index + pertial_row_size > row.row_size {
                        return Err(ShmemLibError::Logic(format!(
                            "Copying segment would overflow row_size. curr_idx: {}, part_size: {}, total_row_size: {}. RowIndex: {:?}",
                            curr_buff_index, pertial_row_size, row.row_size, row
                        )));
                    }

                    self.shmem_service.read_slot(slot_index, |slot| {
                        unsafe {
                            let src_p = slot.data.as_ptr().add(current_segment_start_data_index);
                            let dest_p = buff.add(curr_buff_index);
                            ptr::copy_nonoverlapping(src_p, dest_p, pertial_row_size);
                        }
                        curr_buff_index += pertial_row_size;
                    })?;
                }
            }
            if curr_buff_index != row.row_size {
                return Err(ShmemLibError::Logic(format!(
                    "Total bytes copied ({}) does not match row.row_size ({}). RowIndex: {:?}",
                    curr_buff_index, row.row_size, row
                )));
            }
        }

        let result = f(buff, row.row_size, context);

        unsafe { alloc::dealloc(buff, layout) };

        Ok(result)
    }

    pub fn read_segments<F, C>(
        &self,
        row_index: usize,
        context: &mut C,
        callback: &mut F,
    ) -> Result<(), ShmemLibError>
    where
        F: FnMut(
            &[u8],
            bool,
            bool,
            &mut C,
        ) -> Result<(), UserAbortReason>,
    {
        if row_index >= self.config_max_rows {
            return Err(ShmemLibError::Logic(format!(
                "Provided row_index ({}) in read_segments is out of bounds for configured max_rows ({}). Potential torn Index.end_row_index read by caller.",
                row_index, self.config_max_rows
            )));
        }

        let row = self.shmem_service.read_index(|index| {
            index.rows[row_index]
        })?;

        if row.row_size > self.config_max_row_size {
             return Err(ShmemLibError::Logic(format!(
                "RowIndex.row_size ({}) in read_segments exceeds configured max_row_size ({}) for row_index {}.",
                row.row_size, self.config_max_row_size, row_index
            )));
        }
        if row.start_data_index >= self.config_max_slot_size {
             return Err(ShmemLibError::Logic(format!(
                "RowIndex.start_data_index ({}) in read_segments is out of bounds for configured max_slot_size ({}) for row_index {}.",
                row.start_data_index, self.config_max_slot_size, row_index
            )));
        }
        if row.end_data_index > self.config_max_slot_size {
            return Err(ShmemLibError::Logic(format!(
               "RowIndex.end_data_index ({}) in read_segments is out of bounds for configured max_slot_size ({}) for row_index {}.",
               row.end_data_index, self.config_max_slot_size, row_index
           )));
        }
        if row.start_slot_index == row.end_slot_index && row.end_data_index == 0 && row.row_size > 0 {
             if row.start_data_index + row.row_size > self.config_max_slot_size {
                return Err(ShmemLibError::Logic(format!(
                    "RowIndex for single-slot message in read_segments has end_data_index=0 but row_size={} starting at {} for row_index {}. Inconsistent.",
                    row.row_size, row.start_data_index, row_index
                )));
            }
        }
         if row.start_slot_index == row.end_slot_index &&
           row.start_data_index >= row.end_data_index &&
           row.row_size > 0 {
            return Err(ShmemLibError::Logic(format!(
                "RowIndex for single-slot message in read_segments has start_data_index ({}) >= end_data_index ({}) with row_size={} for row_index {}. Inconsistent.",
                row.start_data_index, row.end_data_index, row.row_size, row_index
            )));
        }

        if row.row_size == 0 {
            if row.start_slot_index == row.end_slot_index && row.start_data_index == row.end_data_index {
                match callback(&[], true, true, context) {
                    Ok(()) => return Ok(()),
                    Err(abort_reason) => return Err(ShmemLibError::UserAbort(abort_reason)),
                }
            }
        }

        if row.end_slot_index < row.start_slot_index && row.row_size > 0 {
            return Err(ShmemLibError::Logic(format!(
               "RowIndex.end_slot_index ({}) < RowIndex.start_slot_index ({}) in read_segments while row_size is non-zero ({}). Potential torn Index read.",
               row.end_slot_index, row.start_slot_index, row.row_size
           )));
       }

        let num_segments = if row.row_size == 0 { 0 } else { row.end_slot_index.saturating_sub(row.start_slot_index) + 1 };
        let mut segments_processed = 0;
        let mut total_partial_size_processed = 0;

        if row.row_size > 0 { // Only validate/iterate slots if there's data
            if row.start_slot_index >= self.config_max_slots {
                return Err(ShmemLibError::Logic(format!(
                    "RowIndex.start_slot_index ({}) in read_segments is out of bounds for configured max_slots ({}). RowIndex: {:?}",
                    row.start_slot_index, self.config_max_slots, row
                )));
            }
            if row.end_slot_index >= self.config_max_slots {
                return Err(ShmemLibError::Logic(format!(
                    "RowIndex.end_slot_index ({}) in read_segments is out of bounds for configured max_slots ({}). RowIndex: {:?}",
                    row.end_slot_index, self.config_max_slots, row
                )));
            }
            // The check `row.end_slot_index < row.start_slot_index` is already present and correct.
        }

        for current_slot_idx in row.start_slot_index..=row.end_slot_index {
            if current_slot_idx >= self.config_max_slots { // Defensive check inside loop
                return Err(ShmemLibError::Logic(format!(
                    "Loop variable current_slot_idx ({}) in read_segments is out of bounds for configured max_slots ({}). RowIndex: {:?}",
                    current_slot_idx, self.config_max_slots, row
                )));
            }
            let segment_start_offset = if current_slot_idx == row.start_slot_index {
                row.start_data_index
            } else {
                0
            };
            let segment_end_offset = if current_slot_idx == row.end_slot_index {
                row.end_data_index
            } else {
                self.config_max_slot_size
            };

            if segment_start_offset > segment_end_offset || segment_end_offset > self.config_max_slot_size {
                 return Err(ShmemLibError::Logic(format!(
                    "Invalid segment offsets for slot {}: start {}, end {} (max_slot_size: {})",
                    current_slot_idx, segment_start_offset, segment_end_offset, self.config_max_slot_size
                )));
            }

            let pertial_row_size = if segment_start_offset > segment_end_offset {
                return Err(ShmemLibError::Logic(format!(
                    "Inconsistent segment data indices in read_segments: start_offset ({}) > end_offset ({}) for slot {}. RowIndex: {:?}",
                    segment_start_offset, segment_end_offset, current_slot_idx, row
                )));
            } else {
                segment_end_offset - segment_start_offset
            };

            if pertial_row_size == 0 && row.row_size > 0 && num_segments > 1 && segments_processed < num_segments {
                if !(num_segments == 1 && row.row_size == 0) {
                     return Err(ShmemLibError::Logic(format!(
                        "Empty segment (start==end offset) encountered for slot {} while row_size ({}) > 0 for row_index {} and not all segments processed.",
                        current_slot_idx, row.row_size, row_index
                    )));
                }
            }
            if pertial_row_size == 0 && (segments_processed + 1) == num_segments && total_partial_size_processed != row.row_size && row.row_size > 0 {
                 return Err(ShmemLibError::Logic(format!(
                    "Last segment empty for slot {} but total_partial_size_processed ({}) != row_size ({}).",
                    current_slot_idx, total_partial_size_processed, row.row_size
                )));
            }

            let is_first = segments_processed == 0;
            segments_processed += 1;
            let is_last = segments_processed == num_segments;

            if pertial_row_size > 0 {
                total_partial_size_processed += pertial_row_size;
                if total_partial_size_processed > row.row_size {
                     return Err(ShmemLibError::Logic(format!(
                        "Segment processing in read_segments would exceed row.row_size. total_partial: {}, row_size: {}. RowIndex: {:?}",
                        total_partial_size_processed, row.row_size, row
                    )));
                }

                let user_decision_result: Result<(), UserAbortReason> = self.shmem_service.read_slot(current_slot_idx, |slot_struct_ref| {
                    let actual_segment_end_offset = std::cmp::min(segment_end_offset, slot_struct_ref.data.len());
                    if segment_start_offset > actual_segment_end_offset {
                        return Err(UserAbortReason::InternalError(format!(
                            "Corrected segment_end_offset {} is less than segment_start_offset {} for slot {}",
                            actual_segment_end_offset, segment_start_offset, current_slot_idx
                        )));
                    }

                    let segment_slice = &slot_struct_ref.data[segment_start_offset..actual_segment_end_offset];
                    callback(segment_slice, is_first, is_last, context)
                })?;
                if let Err(abort_reason) = user_decision_result {
                    return Err(ShmemLibError::UserAbort(abort_reason));
                }
            } else if is_last && total_partial_size_processed != row.row_size && row.row_size > 0 {
                 return Err(ShmemLibError::Logic(format!(
                    "Last segment empty but total_partial_size_processed ({}) != row_size ({}). RowIndex: {:?}",
                    total_partial_size_processed, row.row_size, row
                )));
            }
        }
         if total_partial_size_processed != row.row_size && row.row_size > 0 {
            return Err(ShmemLibError::Logic(format!(
                "Total data processed in segments ({}) does not match row.row_size ({}). RowIndex: {:?}",
                total_partial_size_processed, row.row_size, row
            )));
        }

        Ok(())
    }

    pub fn read_into_buffer(
        &self,
        row_index: usize,
        user_buffer: &mut [u8],
    ) -> Result<usize, ShmemLibError> {
        if row_index >= self.config_max_rows {
            return Err(ShmemLibError::Logic(format!(
                "Provided row_index ({}) in read_into_buffer is out of bounds for configured max_rows ({}). Potential torn Index.end_row_index read by caller.",
                row_index, self.config_max_rows
            )));
        }

        let row = self.shmem_service.read_index(|index| {
            index.rows[row_index]
        })?;

        if row.row_size > self.config_max_row_size {
             return Err(ShmemLibError::Logic(format!(
                "RowIndex.row_size ({}) in read_into_buffer exceeds configured max_row_size ({}) for row_index {}.",
                row.row_size, self.config_max_row_size, row_index
            )));
        }
        if row.start_data_index >= self.config_max_slot_size {
             return Err(ShmemLibError::Logic(format!(
                "RowIndex.start_data_index ({}) in read_into_buffer is out of bounds for configured max_slot_size ({}) for row_index {}.",
                row.start_data_index, self.config_max_slot_size, row_index
            )));
        }
        if row.end_data_index > self.config_max_slot_size {
            return Err(ShmemLibError::Logic(format!(
               "RowIndex.end_data_index ({}) in read_into_buffer is out of bounds for configured max_slot_size ({}) for row_index {}.",
               row.end_data_index, self.config_max_slot_size, row_index
           )));
        }
        if row.start_slot_index == row.end_slot_index && row.end_data_index == 0 && row.row_size > 0 {
             if row.start_data_index + row.row_size > self.config_max_slot_size {
                return Err(ShmemLibError::Logic(format!(
                    "RowIndex for single-slot message in read_into_buffer has end_data_index=0 but row_size={} starting at {} for row_index {}. Inconsistent.",
                    row.row_size, row.start_data_index, row_index
                )));
            }
        }
        if row.start_slot_index == row.end_slot_index &&
           row.start_data_index >= row.end_data_index &&
           row.row_size > 0 {
            return Err(ShmemLibError::Logic(format!(
                "RowIndex for single-slot message in read_into_buffer has start_data_index ({}) >= end_data_index ({}) with row_size={} for row_index {}. Inconsistent.",
                row.start_data_index, row.end_data_index, row.row_size, row_index
            )));
        }

        if row.row_size == 0 {
            return Ok(0);
        }

        if user_buffer.len() < row.row_size {
            return Err(ShmemLibError::Logic(format!(
                "User buffer too small. Needed: {}, available: {}.",
                row.row_size, user_buffer.len()
            )));
        }

        let mut bytes_copied_total: usize = 0;

        if row.row_size > 0 { // Only validate/iterate slots if there's data
            if row.start_slot_index >= self.config_max_slots {
                return Err(ShmemLibError::Logic(format!(
                    "RowIndex.start_slot_index ({}) in read_into_buffer is out of bounds for configured max_slots ({}). RowIndex: {:?}",
                    row.start_slot_index, self.config_max_slots, row
                )));
            }
            if row.end_slot_index >= self.config_max_slots {
                return Err(ShmemLibError::Logic(format!(
                    "RowIndex.end_slot_index ({}) in read_into_buffer is out of bounds for configured max_slots ({}). RowIndex: {:?}",
                    row.end_slot_index, self.config_max_slots, row
                )));
            }
            if row.end_slot_index < row.start_slot_index { // This check was already here and is good
                return Err(ShmemLibError::Logic(format!(
                   "RowIndex.end_slot_index ({}) < RowIndex.start_slot_index ({}) in read_into_buffer while row_size is non-zero ({}). Potential torn Index read.",
                   row.end_slot_index, row.start_slot_index, row.row_size
               )));
           }
        } else { // row.row_size == 0, already handled by `if row.row_size == 0 { return Ok(0); }`
             // No loop needed if row_size is 0.
        }


        for current_slot_idx in row.start_slot_index..=row.end_slot_index {
             if current_slot_idx >= self.config_max_slots { // Defensive check inside loop
                return Err(ShmemLibError::Logic(format!(
                    "Loop variable current_slot_idx ({}) in read_into_buffer is out of bounds for configured max_slots ({}). RowIndex: {:?}",
                    current_slot_idx, self.config_max_slots, row
                )));
            }
            let segment_start_offset = if current_slot_idx == row.start_slot_index {
                row.start_data_index
            } else {
                0
            };
            let segment_end_offset = if current_slot_idx == row.end_slot_index {
                row.end_data_index
            } else {
                self.config_max_slot_size
            };

            let segment_length = if segment_start_offset > segment_end_offset {
                 return Err(ShmemLibError::Logic(format!(
                    "Inconsistent segment data indices in read_into_buffer: start_offset ({}) > end_offset ({}) for slot {}. RowIndex: {:?}",
                    segment_start_offset, segment_end_offset, current_slot_idx, row
                )));
            } else {
                segment_end_offset - segment_start_offset
            };

            if segment_length == 0 {
                if row.row_size > 0 && bytes_copied_total < row.row_size && current_slot_idx != row.end_slot_index {
                     return Err(ShmemLibError::Logic(format!(
                        "Empty middle segment encountered in read_into_buffer for slot {} while row_size is {}.",
                        current_slot_idx, row.row_size
                    )));
                }
                if current_slot_idx == row.end_slot_index && bytes_copied_total == row.row_size && segment_length == 0 {
                } else if row.row_size > 0 && bytes_copied_total < row.row_size && segment_length == 0 {
                     return Err(ShmemLibError::Logic(format!(
                        "Empty segment encountered in read_into_buffer for slot {} before all data copied. Copied: {}, Expected: {}",
                        current_slot_idx, bytes_copied_total, row.row_size
                    )));
                }
                continue;
            }

            if bytes_copied_total + segment_length > user_buffer.len() {
                return Err(ShmemLibError::Logic(format!(
                    "Internal error or inconsistent RowIndex: About to write past user_buffer. Copied: {}, segment: {}, buffer: {}, row_size: {}",
                    bytes_copied_total, segment_length, user_buffer.len(), row.row_size
                )));
            }

            let dest_slice_start = bytes_copied_total;
            let user_buffer_ptr = user_buffer.as_mut_ptr();

            self.shmem_service.read_slot(current_slot_idx, |slot_data| {
                let actual_segment_end_offset = std::cmp::min(segment_end_offset, slot_data.data.len());
                if segment_start_offset > actual_segment_end_offset {
                    // This would be an internal error or severely torn RowIndex.
                    // This path should ideally not be reached if prior validations on segment_start_offset
                    // and segment_end_offset (derived from RowIndex data_indices) are correct
                    // against config_max_slot_size, and config_max_slot_size <= COMPILE_TIME_MAX_SLOT_SIZE.
                    // However, to be safe, make it an error instead of panic.
                    // This indicates a problem that should be reported as an error.
                    // Since this closure doesn't return Result, we can't propagate.
                    // The outer function will rely on bytes_copied_total != row.row_size.
                    // For now, we'll keep the assert! but acknowledge it could panic.
                    // A more robust solution might involve the closure returning Result.
                     assert!(segment_start_offset <= actual_segment_end_offset,
                        "Corrected segment_end_offset {} is less than segment_start_offset {} for slot {}",
                        actual_segment_end_offset, segment_start_offset, current_slot_idx);
                }

                let src_slice = &slot_data.data[segment_start_offset..actual_segment_end_offset];
                let current_segment_physical_length = src_slice.len();

                if segment_length != current_segment_physical_length {
                     eprintln!("Warning: Logical segment length {} mismatch with physical {} for slot {}. RowIndex: {:?}",
                                segment_length, current_segment_physical_length, current_slot_idx, row);
                    // This is a significant issue, potentially leading to not all data being copied
                    // or, if current_segment_physical_length is used for copy, less data than row.row_size.
                    // The final check `bytes_copied_total != row.row_size` should catch this.
                }

                let dest_ptr = unsafe { user_buffer_ptr.add(dest_slice_start) };
                let dest_slice = unsafe { std::slice::from_raw_parts_mut(dest_ptr, current_segment_physical_length) };

                dest_slice.copy_from_slice(src_slice);
            })?;

            bytes_copied_total += segment_length;
        }

        if bytes_copied_total != row.row_size {
            return Err(ShmemLibError::Logic(format!(
                "Internal error or inconsistent RowIndex: total bytes copied ({}) does not match row.row_size ({}). RowIndex: {:?}",
                bytes_copied_total, row.row_size, row
            )));
        }
        Ok(bytes_copied_total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::MessageWriter;
    use crate::writer::WriterConfig;
    use tempfile::TempDir;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use shared_memory;
    use std::ptr as test_ptr;
    // std::alloc is not directly used in tests, but MessageReader::read uses it.

    static TEST_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct ReadContext<'a> {
        buffer: &'a mut [u8],
        length_read: usize,
    }

    fn setup_reader_writer_with_config(
        base_config_params: ShmemConfig // Template for logical parameters
    ) -> (MessageWriter, MessageReader, TempDir, ShmemConfig, Box<shared_memory::Shmem>) {
        let temp_dir = tempfile::tempdir().unwrap();
        let unique_shmem_file_name_id = format!("test_shmem_rw_{}", TEST_ID_COUNTER.fetch_add(1, AtomicOrdering::SeqCst));
        let shmem_file_path_str = temp_dir.path().join(unique_shmem_file_name_id).to_str().unwrap().to_string();

        let actual_config = ShmemConfig::builder()
            .shmem_file_name(shmem_file_path_str)
            .use_flink_backing(true)
            .max_rows(base_config_params.max_rows)
            .max_row_size(base_config_params.max_row_size)
            .max_slots(base_config_params.max_slots)
            .max_slot_size(base_config_params.max_slot_size)
            .build()
            .expect("Failed to build actual_config in setup");

        let initial_shmem_mapping = writer_context(&actual_config)
            .expect("Failed to create writer_context in setup");

        let writer_cfg = WriterConfig { shmem: actual_config.clone() };
        let reader_cfg = ReaderConfig { shmem: actual_config.clone() };

        let writer = MessageWriter::new(&writer_cfg).expect("Failed to create MessageWriter");
        let reader = MessageReader::new(&reader_cfg).expect("Failed to create MessageReader");

        (writer, reader, temp_dir, actual_config, initial_shmem_mapping)
    }

    fn default_test_config() -> ShmemConfig {
        ShmemConfig::builder()
            .use_flink_backing(true) // Must be true for tests using tempdir
            .shmem_file_name("default_placeholder.ipc".to_string()) // Placeholder, will be overridden
            .max_rows(10)
            .max_row_size(256)
            .max_slots(crate::core::MAX_ROWS * 2) // Explicitly use core's MAX_ROWS
            .max_slot_size(128)
            .build()
            .expect("Failed to build default test ShmemConfig")
    }


    #[test]
    fn test_read_single_message() -> Result<(), Box<dyn std::error::Error>> {
        let (mut writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(default_test_config());

        let message_content = "hello world";
        let message_bytes = message_content.as_bytes();

        let row_index = writer.add(message_bytes)?;

        let mut read_buffer = vec![0u8; message_bytes.len()];
        let mut context = ReadContext {
            buffer: &mut read_buffer,
            length_read: 0,
        };

        let callback_result_outer: Result<Result<(), ShmemLibError>, ShmemLibError> = reader.read(
            row_index,
            |buff_ptr, length, ctx: &mut ReadContext| -> Result<(), ShmemLibError> {
                if length > ctx.buffer.len() {
                    return Err(ShmemLibError::Logic(format!(
                        "Buffer too small. Read length: {}, buffer_len: {}",
                        length, ctx.buffer.len()
                    )));
                }
                unsafe {
                    test_ptr::copy_nonoverlapping(buff_ptr, ctx.buffer.as_mut_ptr(), length);
                }
                ctx.length_read = length;
                Ok(())
        }, &mut context);

        let _ = callback_result_outer??;

        assert_eq!(context.length_read, message_bytes.len(), "Length of read message does not match");
        assert_eq!(read_buffer, message_bytes, "Content of read message does not match");

        Ok(())
    }

    #[test]
    fn test_read_multi_slot_message() -> Result<(), Box<dyn std::error::Error>> {
        let mut multi_slot_config = default_test_config();
        multi_slot_config.max_slot_size = 10;
        multi_slot_config.max_row_size = 50;

        let (mut writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(multi_slot_config);

        let message_content = "this is a long message that spans slots";
        let message_bytes = message_content.as_bytes();
        assert!(message_bytes.len() > 10 && message_bytes.len() <= 50);

        let row_index = writer.add(message_bytes)?;

        let mut read_buffer = vec![0u8; message_bytes.len()];
        let mut context = ReadContext {
            buffer: &mut read_buffer,
            length_read: 0,
        };

        let cb_res_outer: Result<Result<(), ShmemLibError>, ShmemLibError> = reader.read(row_index, |buff_ptr, length, ctx: &mut ReadContext| {
            if length > ctx.buffer.len() {
                return Err(ShmemLibError::Logic(format!(
                    "Buffer too small. Read length: {}, buffer_len: {}",
                    length, ctx.buffer.len()
                )));
            }
            unsafe { test_ptr::copy_nonoverlapping(buff_ptr, ctx.buffer.as_mut_ptr(), length); }
            ctx.length_read = length;
            Ok(())
        }, &mut context);
        cb_res_outer??;

        assert_eq!(context.length_read, message_bytes.len());
        assert_eq!(read_buffer, message_bytes);
        Ok(())
    }

    #[test]
    fn test_read_multiple_messages() -> Result<(), Box<dyn std::error::Error>> {
        let (mut writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(default_test_config());

        let messages = vec!["msg1", "message two", "third one is a bit longer"];
        let mut row_indices = Vec::new();

        for msg_content in &messages {
            let msg_bytes = msg_content.as_bytes();
            row_indices.push(writer.add(msg_bytes)?);
        }

        for (i, msg_content) in messages.iter().enumerate() {
            let msg_bytes = msg_content.as_bytes();
            let row_index = row_indices[i];

            let mut read_buffer = vec![0u8; msg_bytes.len()];
            let mut context = ReadContext { buffer: &mut read_buffer, length_read: 0 };

            let cb_res_outer: Result<Result<(), ShmemLibError>, ShmemLibError> = reader.read(row_index, |buff_ptr, length, ctx: &mut ReadContext| {
                if length > ctx.buffer.len() {
                    return Err(ShmemLibError::Logic(format!(
                        "Buffer too small for msg {}. Read: {}, buffer: {}", i, length, ctx.buffer.len()
                    )));
                }
                unsafe { test_ptr::copy_nonoverlapping(buff_ptr, ctx.buffer.as_mut_ptr(), length); }
                ctx.length_read = length;
                Ok(())
            }, &mut context);
            cb_res_outer??;

            assert_eq!(context.length_read, msg_bytes.len(), "Length mismatch for message {}", i);
            assert_eq!(read_buffer, msg_bytes, "Content mismatch for message {}", i);
        }
        Ok(())
    }

    #[test]
    fn test_read_max_row_size_message() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = default_test_config();
        config.max_row_size = 50;
        let (mut writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(config);

        let message_bytes = vec![b'A'; _config.max_row_size];

        let row_index = writer.add(&message_bytes)?;

        let mut read_buffer = vec![0u8; message_bytes.len()];
        let mut context = ReadContext { buffer: &mut read_buffer, length_read: 0 };

        let cb_res_outer: Result<Result<(), ShmemLibError>, ShmemLibError> = reader.read(row_index, |buff_ptr, length, ctx: &mut ReadContext| {
            if length > ctx.buffer.len() {
                 return Err(ShmemLibError::Logic(format!(
                    "Buffer too small. Read: {}, buffer: {}", length, ctx.buffer.len()
                )));
            }
            unsafe { test_ptr::copy_nonoverlapping(buff_ptr, ctx.buffer.as_mut_ptr(), length); }
            ctx.length_read = length;
            Ok(())
        }, &mut context);
        cb_res_outer??;

        assert_eq!(context.length_read, message_bytes.len());
        assert_eq!(read_buffer, message_bytes);
        Ok(())
    }

    #[test]
    fn test_rib_exact_size_buffer() -> Result<(), ShmemLibError> {
        let (mut writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(default_test_config());
        let message_content = "exact fit";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut user_buffer = vec![0u8; message_bytes.len()];
        let bytes_read = reader.read_into_buffer(row_index, &mut user_buffer)?;

        assert_eq!(bytes_read, message_bytes.len());
        assert_eq!(user_buffer, message_bytes);
        Ok(())
    }

    #[test]
    fn test_rib_larger_buffer() -> Result<(), ShmemLibError> {
        let (mut writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(default_test_config());
        let message_content = "fits well";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut user_buffer = vec![0u8; message_bytes.len() + 5];
        user_buffer[message_bytes.len()..].fill(0xAA);

        let bytes_read = reader.read_into_buffer(row_index, &mut user_buffer)?;

        assert_eq!(bytes_read, message_bytes.len());
        assert_eq!(&user_buffer[..bytes_read], message_bytes);
        assert_eq!(user_buffer[bytes_read..], vec![0xAAu8; 5]);
        Ok(())
    }

    #[test]
    fn test_rib_too_small_buffer() -> Result<(), ShmemLibError> {
        let (mut writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(default_test_config());
        let message_content = "too large for buffer";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut user_buffer = vec![0u8; message_bytes.len() - 1];
        let result = reader.read_into_buffer(row_index, &mut user_buffer);

        match result {
            Err(ShmemLibError::Logic(msg)) => {
                assert!(msg.contains("User buffer too small"));
            }
            _ => panic!("Expected ShmemLibError::Logic for too small buffer, got {:?}", result),
        }
        Ok(())
    }

    #[test]
    fn test_rib_multi_slot_message() -> Result<(), ShmemLibError> {
        let mut multi_slot_config = default_test_config();
        multi_slot_config.max_slot_size = 8;
        multi_slot_config.max_row_size = 30;
        let (mut writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(multi_slot_config);

        let message_content = "this message definitely spans";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut user_buffer = vec![0u8; message_bytes.len()];
        let bytes_read = reader.read_into_buffer(row_index, &mut user_buffer)?;

        assert_eq!(bytes_read, message_bytes.len());
        assert_eq!(user_buffer, message_bytes);
        Ok(())
    }

    #[test]
    fn test_rib_empty_message() -> Result<(), ShmemLibError> {
        let (_writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(default_test_config());
        let mut user_buffer = vec![0u8; 5];
        let bytes_read = reader.read_into_buffer(0, &mut user_buffer)?;

        assert_eq!(bytes_read, 0, "Bytes read for an empty/default row should be 0");
        Ok(())
    }

    #[derive(Default)]
    struct SegmentReadContext {
        segments: Vec<Vec<u8>>,
        is_first_flags: Vec<bool>,
        is_last_flags: Vec<bool>,
    }

    impl SegmentReadContext {
        fn combined_data(&self) -> Vec<u8> {
            self.segments.concat()
        }
    }

    #[test]
    fn test_rs_single_segment_message() -> Result<(), ShmemLibError> {
        let (mut writer, reader, _temp_dir, mut config, _shmem_mapping) = setup_reader_writer_with_config(default_test_config());
        config.max_slot_size = 128;
        let message_content = "single segment";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut context = SegmentReadContext::default();
        reader.read_segments(row_index, &mut context, &mut |segment_slice, is_first, is_last, ctx| {
            ctx.segments.push(segment_slice.to_vec());
            ctx.is_first_flags.push(is_first);
            ctx.is_last_flags.push(is_last);
            Ok(())
        })?;

        assert_eq!(context.segments.len(), 1);
        assert_eq!(context.is_first_flags, vec![true]);
        assert_eq!(context.is_last_flags, vec![true]);
        assert_eq!(context.combined_data(), message_bytes);
        Ok(())
    }

    #[test]
    fn test_rs_multi_segment_message() -> Result<(), ShmemLibError> {
        let mut multi_slot_config = default_test_config();
        multi_slot_config.max_slot_size = 10;
        multi_slot_config.max_row_size = 50;
        let (mut writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(multi_slot_config);

        let message_content = "this is a long message that spans multiple slots";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut context = SegmentReadContext::default();
        reader.read_segments(row_index, &mut context, &mut |segment_slice, is_first, is_last, ctx| {
            ctx.segments.push(segment_slice.to_vec());
            ctx.is_first_flags.push(is_first);
            ctx.is_last_flags.push(is_last);
            Ok(())
        })?;

        assert!(context.segments.len() > 1, "Expected multiple segments");
        assert_eq!(context.is_first_flags[0], true);
        if context.segments.len() > 1 {
            for i in 1..context.is_first_flags.len() {
                assert_eq!(context.is_first_flags[i], false, "is_first should be false for segment {}", i);
            }
            for i in 0..context.is_last_flags.len() - 1 {
                assert_eq!(context.is_last_flags[i], false, "is_last should be false for segment {}", i);
            }
        }
        assert_eq!(*context.is_last_flags.last().unwrap_or(&false), true, "is_last for last segment");
        assert_eq!(context.combined_data(), message_bytes);
        Ok(())
    }

    #[test]
    fn test_rs_user_abort() -> Result<(), ShmemLibError> {
        let mut multi_slot_config = default_test_config();
        multi_slot_config.max_slot_size = 10;
        multi_slot_config.max_row_size = 50;
        let (mut writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(multi_slot_config);

        let message_content = "long message to test user abort";
        let message_bytes = message_content.as_bytes();
        let row_index = writer.add(message_bytes)?;

        let mut context = SegmentReadContext::default();
        let result = reader.read_segments(row_index, &mut context, &mut |segment_slice, _is_first, _is_last, ctx| {
            ctx.segments.push(segment_slice.to_vec());
            if ctx.segments.len() >= 2 {
                return Err(UserAbortReason::UserRequestedStop);
            }
            Ok(())
        });

        match result {
            Err(ShmemLibError::UserAbort(UserAbortReason::UserRequestedStop)) => {}
            _ => panic!("Expected UserAbortReason::UserRequestedStop, got {:?}", result),
        }
        assert!(context.segments.len() <= 2 && context.segments.len() > 0, "Should have processed some segments before abort");
        let processed_data = context.combined_data();
        assert!(message_bytes.starts_with(&processed_data));
        assert_ne!(processed_data, message_bytes, "Should not have processed the full message");

        Ok(())
    }

    #[test]
    fn test_rs_empty_message() -> Result<(), ShmemLibError> {
        let (_writer, reader, _temp_dir, _config, _shmem_mapping) = setup_reader_writer_with_config(default_test_config());
        let mut context = SegmentReadContext::default();
        reader.read_segments(0, &mut context, &mut |segment_slice, is_first, is_last, ctx| {
            ctx.segments.push(segment_slice.to_vec());
            ctx.is_first_flags.push(is_first);
            ctx.is_last_flags.push(is_last);
            Ok(())
        })?;

        assert_eq!(context.segments.len(), 1, "Callback should be called once for an empty message");
        assert_eq!(context.segments[0].len(), 0, "Segment for empty message should be empty");
        assert_eq!(context.is_first_flags, vec![true]);
        assert_eq!(context.is_last_flags, vec![true]);
        Ok(())
    }
}
