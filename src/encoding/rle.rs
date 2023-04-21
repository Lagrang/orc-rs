use std::cmp;

use bytes::{Buf, BufMut, BytesMut};

use crate::io_utils::{self, BufRead};

/// For byte streams, ORC uses a very light weight encoding of identical values.
///     - Run: a sequence of at least 3 identical values
///     - Literals: a sequence of non-identical values
/// The first byte of each group of values is a header that determines whether it is a run (value between 0 to 127)
/// or literal list (value between -128 to -1). For runs, the control byte is the length of the run minus the length
/// of the minimal run (3) and the control byte for literal lists is the negative length of the list.
/// For example, a hundred 0’s is encoded as [0x61, 0x00] and the sequence 0x44, 0x45 would be encoded as [0xfe, 0x44, 0x45].
pub struct ByteRleDecoder<Input> {
    file_reader: Input,
    completed: bool,
    // Block of data read from file but not processed yet
    buffer: BytesMut,
    // State of current run
    current_run: RunState,
}

struct RunState {
    // Index of last element in 'plain' sequence of values
    length: usize,
    // Index of next value in 'plain' sequence to return
    consumed: usize,
    /// Indicates that current block is not RLE encoded, but contains plain values.
    is_plain: bool,
}

impl RunState {
    fn empty() -> Self {
        Self {
            length: 0,
            consumed: 0,
            is_plain: false,
        }
    }

    fn from_header(header: i8) -> Self {
        let len = if header > 0 {
            //Run length at least 3 values and this min.length is not coded in 'length' field of header.
            header as usize + 3
        } else {
            header.unsigned_abs() as usize
        };
        // Negative length means that next run doesn't contains equal values.
        Self {
            length: len,
            consumed: 0,
            is_plain: header < 0,
        }
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.length - self.consumed
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        debug_assert!(self.remaining() >= cnt);
        self.consumed += cnt;
    }

    #[inline]
    fn has_values(&self) -> bool {
        self.consumed < self.length
    }
}

impl<Input: BufRead> ByteRleDecoder<Input> {
    pub fn new(file_reader: Input, buffer_size: usize) -> Self {
        let cap = cmp::max(buffer_size, 1);
        Self {
            file_reader,
            completed: false,
            buffer: BytesMut::with_capacity(cap),
            current_run: RunState::empty(),
        }
    }

    pub fn read(&mut self, batch_size: usize) -> crate::Result<Option<arrow::buffer::Buffer>> {
        if self.completed {
            return Ok(None);
        }

        let mut builder = Vec::with_capacity(batch_size);
        let mut remaining_values = batch_size;
        while remaining_values > 0 {
            // Current RLE run completed or buffer with values exhausted.
            if (!self.current_run.has_values() || self.buffer.is_empty())
                && !self.read_next_block()?
            {
                // No more data to decode
                self.completed = true;
                break;
            }

            let count = if self.current_run.is_plain {
                // Copy values(sequence of different values) from buffer
                let count = cmp::min(
                    cmp::min(self.current_run.remaining(), remaining_values),
                    self.buffer.remaining(),
                );
                builder.extend_from_slice(&self.buffer[..count]);
                self.buffer.advance(count);
                count
            } else {
                let count = cmp::min(self.current_run.remaining(), remaining_values);
                builder.put_bytes(self.buffer[0], count);
                count
            };

            remaining_values -= count;
            self.current_run.advance(count);
            if !self.current_run.has_values() && !self.current_run.is_plain {
                // RLE run with repeated values is completed, skip byte with run value.
                self.buffer.advance(1);
            }
        }

        if !builder.is_empty() {
            Ok(Some(arrow::buffer::Buffer::from_vec(builder)))
        } else {
            Ok(None)
        }
    }

    fn read_next_block(&mut self) -> crate::Result<bool> {
        if self.buffer.is_empty() {
            let bytes_read = io_utils::BufRead::read(&mut self.file_reader, &mut self.buffer)?;
            if bytes_read == 0 {
                if self.current_run.has_values() {
                    return Err(crate::OrcError::MalformedRleBlock);
                } else {
                    return Ok(false);
                }
            }
        }

        // Start new RLE run
        if !self.current_run.has_values() {
            // First byte of block contains the RLE header.
            let header = i8::from_le_bytes([self.buffer[0]; 1]);
            self.buffer.advance(1);
            self.current_run = RunState::from_header(header);
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use googletest::matchers::eq;
    use googletest::verify_that;

    use crate::source::MemoryReader;

    use super::ByteRleDecoder;

    const BUFFER_SIZE: usize = 4 * 1024;

    #[test]
    fn byte_rle_sequence() -> googletest::Result<()> {
        let mut source_data = BytesMut::new();
        let mut expected_data = BytesMut::new();
        source_data.put_u8(15);
        source_data.put_u8(1);
        expected_data.put_bytes(1, 18);
        // Test max sized for repeated values.
        source_data.put_u8(127);
        source_data.put_u8(2);
        expected_data.put_bytes(2, 130);
        // Test min sized for repeated values.
        source_data.put_u8(1);
        source_data.put_u8(3);
        expected_data.put_bytes(3, 4);
        // Test sequence of different values.
        source_data.put_u8(-34i8 as u8);
        source_data.put_bytes(1, 34);
        expected_data.put_bytes(1, 34);
        // Test max size for a sequence of different values.
        source_data.put_u8(-128i8 as u8);
        for i in 0..128 {
            source_data.put_u8(i);
            expected_data.put_u8(i);
        }
        // Test min size for a sequence of different values.
        source_data.put_u8(-1i8 as u8);
        source_data.put_u8(5);
        expected_data.put_u8(5);

        let reader = MemoryReader::from_mut(source_data.clone());
        let mut rle = ByteRleDecoder::new(reader, BUFFER_SIZE);
        let mut actual = Vec::new();
        loop {
            let array = rle.read(3)?;
            if array.is_none() {
                break;
            }
            actual.extend_from_slice(array.unwrap().as_slice());
        }

        verify_that!(actual, eq(expected_data.to_vec()))?;
        Ok(())
    }
}
