use std::cmp;

use bytes::{Buf, BufMut, BytesMut};

use crate::io_utils::{self, BufRead};

use super::Integer;

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
            // Run length at least 3 values and this min.length is not coded in 'length' field of header.
            header as usize + 3
        } else {
            // If value is negative, then flip the sigh bit to get length of run
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
    fn increment_consumed(&mut self, cnt: usize) {
        debug_assert!(self.remaining() >= cnt);
        self.consumed += cnt;
    }

    #[inline]
    fn has_values(&self) -> bool {
        self.consumed < self.length
    }
}

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
            self.current_run.increment_consumed(count);
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

/// In Hive 0.11 ORC files used Run Length Encoding version 1 (RLEv1), which provides
/// a lightweight compression of signed or unsigned integer sequences.
///
/// RLEv1 has two sub-encodings:
///     - Run - a sequence of values that differ by a small fixed delta
///     - Literals - a sequence of varint encoded values
///
/// Runs start with an initial byte of 0x00 to 0x7f, which encodes the length of the run - 3.
/// A second byte provides the fixed delta in the range of -128 to 127. Finally, the first value
/// of the run is encoded as a base 128 varint.
///
/// For example, if the sequence is 100 instances of 7 the encoding would start with 100 - 3,
/// followed by a delta of 0, and a varint of 7 for an encoding of [0x61, 0x00, 0x07].
/// To encode the sequence of numbers running from 100 to 1, the first byte is 100 - 3, the delta is -1,
/// and the varint is 100 for an encoding of [0x61, 0xff, 0x64].
///
/// Literals start with an initial byte of 0x80 to 0xff, which corresponds to the negative of number
/// of literals in the sequence. Following the header byte, the list of N varints is encoded.
/// Thus, if there are no runs, the overhead is 1 byte for each 128 integers.
/// Numbers [2, 3, 6, 7, 11] would be encoded as [0xfb, 0x02, 0x03, 0x06, 0x07, 0xb].
pub(crate) struct IntRleV1Decoder<Input, IntType> {
    file_reader: Input,
    completed: bool,
    // Block of data read from file but not processed yet.
    buffer: BytesMut,
    // State of current run.
    current_run: RunState,
    // Delta value for the current RLE run. This is second byte in run, right after the header.
    delta: IntType,
    // First value in the current RLE run which is used as base for a computation of run elements.
    // This is third byte in run, in follows after the delta.
    base_value: IntType,
}

impl<Input, IntType> IntRleV1Decoder<Input, IntType>
where
    Input: BufRead,
{
    pub fn new<const N: usize, const M: usize>(file_reader: Input, buffer_size: usize) -> Self
    where
        IntType: Integer<N, M>,
    {
        let cap = cmp::max(buffer_size, 1);
        Self {
            file_reader,
            completed: false,
            buffer: BytesMut::with_capacity(cap),
            current_run: RunState::empty(),
            delta: IntType::ZERO,
            base_value: IntType::ZERO,
        }
    }

    pub fn read<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize>(
        &mut self,
        batch_size: usize,
    ) -> crate::Result<Option<arrow::buffer::ScalarBuffer<IntType>>>
    where
        IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE>,
    {
        if self.completed {
            return Ok(None);
        }

        let mut decoded_count = 0;
        let mut builder = BytesMut::with_capacity(batch_size * TYPE_SIZE);
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
                // Take sequence of different varint values from buffer.
                let count = cmp::min(
                    cmp::min(self.current_run.remaining(), remaining_values),
                    self.buffer.remaining(),
                );
                let mut bytes_read = 0;
                for _ in 0..count {
                    let (val, bytes): (IntType, usize) =
                        IntType::varint_decode(&self.buffer[bytes_read..]);
                    bytes_read += bytes;
                    builder.put_slice(&val.to_le_bytes());
                }
                self.buffer.advance(bytes_read);
                count
            } else {
                // Values are based on delta and base value
                let count = cmp::min(self.current_run.remaining(), remaining_values);
                for _ in 0..count {
                    builder.put_slice(&(self.base_value + self.delta).to_le_bytes());
                    self.base_value += self.delta;
                }
                count
            };

            remaining_values -= count;
            decoded_count += count;
            self.current_run.increment_consumed(count);
        }

        if !builder.is_empty() {
            Ok(Some(arrow::buffer::ScalarBuffer::new(
                arrow::buffer::Buffer::from_vec(builder.to_vec()),
                0,
                decoded_count,
            )))
        } else {
            Ok(None)
        }
    }

    fn read_next_block<const N: usize, const M: usize>(&mut self) -> crate::Result<bool>
    where
        IntType: Integer<N, M>,
    {
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
            let header = self.buffer[0] as i8;
            self.buffer.advance(1);
            self.current_run = RunState::from_header(header);

            // Decode delta and base value of RLE run.
            if !self.current_run.is_plain {
                // Need more data in buffer to read 'delta' value and base value of run.
                if self.buffer.is_empty() {
                    let bytes_read =
                        io_utils::BufRead::read(&mut self.file_reader, &mut self.buffer)?;
                    // We need one more value in RLE run
                    if bytes_read == 0 {
                        if self.current_run.has_values() {
                            return Err(crate::OrcError::MalformedRleBlock);
                        } else {
                            return Ok(false);
                        }
                    }
                }

                self.delta = IntType::from_byte(self.buffer[0]);
                // Skip 1 header byte and try to decode base value of run
                let (value, bytes_read) = IntType::varint_decode(&self.buffer[1..]);
                self.base_value = value;
                self.buffer.advance(bytes_read + 1);
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Neg;

    use bytes::{BufMut, Bytes, BytesMut};
    use googletest::matchers::eq;
    use googletest::verify_that;

    use crate::encoding::Integer;
    use crate::source::MemoryReader;

    use super::{ByteRleDecoder, IntRleV1Decoder};

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

    #[test]
    fn i8_rle_v1_sequence() -> googletest::Result<()> {
        let mut source_data = BytesMut::new();
        let mut expected_data = BytesMut::new();
        // Test sequence of values
        let values: Vec<i8> = vec![0, 10, i8::MAX, -1, i8::MIN];
        source_data.put_i8((values.len() as i8).neg());
        for v in values {
            let (encoded, size) = v.as_varint();
            source_data.put_slice(&encoded[..size]);
            expected_data.put_slice(&v.to_le_bytes());
        }

        validate_int_rle::<1, 2, i8>(source_data.freeze(), expected_data.freeze())
    }

    #[test]
    fn i16_rle_v1_sequence() -> googletest::Result<()> {
        let mut source_data = BytesMut::new();
        let mut expected_data = BytesMut::new();
        // Test sequence of values
        let values: Vec<i16> = vec![0, 10, i16::MAX, -1, i16::MIN];
        source_data.put_i8((values.len() as i8).neg());
        for v in values {
            let (encoded, size) = v.as_varint();
            source_data.put_slice(&encoded[..size]);
            expected_data.put_slice(&v.to_le_bytes());
        }

        validate_int_rle::<2, 3, i16>(source_data.freeze(), expected_data.freeze())
    }

    #[test]
    fn i32_rle_v1_sequence() -> googletest::Result<()> {
        let mut source_data = BytesMut::new();
        let mut expected_data = BytesMut::new();
        // Test sequence of values
        let values: Vec<i32> = vec![0, 10, i32::MAX, -1, i32::MIN];
        source_data.put_i8((values.len() as i8).neg());
        for v in values {
            let (encoded, size) = v.as_varint();
            source_data.put_slice(&encoded[..size]);
            expected_data.put_slice(&v.to_le_bytes());
        }

        validate_int_rle::<4, 5, i32>(source_data.freeze(), expected_data.freeze())
    }

    #[test]
    fn i64_rle_v1_sequence() -> googletest::Result<()> {
        let mut source_data = BytesMut::new();
        let mut expected_data = BytesMut::new();
        // Test sequence of values
        let values: Vec<i64> = vec![0, 10, i64::MAX, -1, i64::MIN];
        source_data.put_i8((values.len() as i8).neg());
        for v in values {
            let (encoded, size) = v.as_varint();
            source_data.put_slice(&encoded[..size]);
            expected_data.put_slice(&v.to_le_bytes());
        }

        validate_int_rle::<8, 10, i64>(source_data.freeze(), expected_data.freeze())
    }

    #[test]
    fn u8_rle_v1_sequence() -> googletest::Result<()> {
        let mut source_data = BytesMut::new();
        let mut expected_data = BytesMut::new();
        // Test sequence of values
        let values: Vec<u8> = vec![0, 1, 10, u8::MAX, u8::MIN];
        source_data.put_i8((values.len() as i8).neg());
        for v in values {
            let (encoded, size) = v.as_varint();
            source_data.put_slice(&encoded[..size]);
            expected_data.put_slice(&v.to_le_bytes());
        }

        validate_int_rle::<1, 2, u8>(source_data.freeze(), expected_data.freeze())
    }

    #[test]
    fn u16_rle_v1_sequence() -> googletest::Result<()> {
        let mut source_data = BytesMut::new();
        let mut expected_data = BytesMut::new();
        // Test sequence of values
        let values: Vec<u16> = vec![0, 10, u16::MAX, u16::MIN];
        source_data.put_i8((values.len() as i8).neg());
        for v in values {
            let (encoded, size) = v.as_varint();
            source_data.put_slice(&encoded[..size]);
            expected_data.put_slice(&v.to_le_bytes());
        }

        validate_int_rle::<2, 3, u16>(source_data.freeze(), expected_data.freeze())
    }

    #[test]
    fn u32_rle_v1_sequence() -> googletest::Result<()> {
        let mut source_data = BytesMut::new();
        let mut expected_data = BytesMut::new();
        // Test sequence of values
        let values: Vec<u32> = vec![0, 10, u32::MAX, u32::MIN];
        source_data.put_i8((values.len() as i8).neg());
        for v in values {
            let (encoded, size) = v.as_varint();
            source_data.put_slice(&encoded[..size]);
            expected_data.put_slice(&v.to_le_bytes());
        }

        validate_int_rle::<4, 5, u32>(source_data.freeze(), expected_data.freeze())
    }

    #[test]
    fn u64_rle_v1_sequence() -> googletest::Result<()> {
        let mut source_data = BytesMut::new();
        let mut expected_data = BytesMut::new();
        // Test sequence of values
        let values: Vec<u64> = vec![0, 10, u64::MAX, u64::MIN];
        source_data.put_i8((values.len() as i8).neg());
        for v in values {
            let (encoded, size) = v.as_varint();
            source_data.put_slice(&encoded[..size]);
            expected_data.put_slice(&v.to_le_bytes());
        }

        validate_int_rle::<8, 10, u64>(source_data.freeze(), expected_data.freeze())
    }

    fn validate_int_rle<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize, IntType>(
        encoded_data: Bytes,
        expected_data: Bytes,
    ) -> googletest::Result<()>
    where
        IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE>,
    {
        let reader = MemoryReader::from(encoded_data);
        let mut rle: IntRleV1Decoder<MemoryReader, IntType> =
            IntRleV1Decoder::new(reader, BUFFER_SIZE);

        let mut actual = Vec::new();
        loop {
            let array = rle.read(3)?;
            if array.is_none() {
                break;
            }
            actual.extend_from_slice(array.unwrap().inner().as_slice());
        }

        verify_that!(actual, eq(expected_data.to_vec()))?;
        Ok(())
    }
}
