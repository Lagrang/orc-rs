use std::cmp;
use std::io::Read;

use bytes::{BufMut, BytesMut};

use crate::OrcError;

use super::Integer;

struct RleState {
    // Index of last element in 'plain' sequence of values
    length: usize,
    // Index of next value in 'plain' sequence to return
    consumed: usize,
    /// Indicates that current block is not RLE encoded, but contains plain values.
    is_plain_sequence: bool,
}

impl RleState {
    fn empty() -> Self {
        Self {
            length: 0,
            consumed: 0,
            is_plain_sequence: false,
        }
    }

    fn for_v2(rleType: &RleV2Type) -> Self {
        match rleType {
            RleV2Type::ShortRepeat(_, repeat_count) => RleState {
                length: *repeat_count as usize,
                consumed: 0,
                is_plain_sequence: false,
            },
            RleV2Type::Direct(_, num_values, _) => RleState {
                length: *num_values as usize,
                consumed: 0,
                is_plain_sequence: true,
            },
        }
    }

    fn from_v1_header(header: i8) -> Self {
        let len = if header >= 0 {
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
            is_plain_sequence: header < 0,
        }
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.length - self.consumed
    }

    #[inline]
    fn consumed(&self) -> usize {
        self.consumed
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
    file_reader: std::io::BufReader<Input>,
    completed: bool,
    // State of current run
    current_run: RleState,
    // Used only if current run is not a sequence of literals.
    // This is single value of RLE repeated N times.
    run_value: u8,
}

impl<Input: std::io::Read> ByteRleDecoder<Input> {
    pub fn new(file_reader: Input, buffer_size: usize) -> Self {
        let cap = cmp::max(buffer_size, 1);
        Self {
            file_reader: std::io::BufReader::with_capacity(cap, file_reader),
            completed: false,
            current_run: RleState::empty(),
            run_value: 0,
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
            if !self.current_run.has_values() && !self.read_next_block()? {
                // No more data to decode
                self.completed = true;
                break;
            }

            let values_to_read = cmp::min(self.current_run.remaining(), remaining_values);
            let values_read = if self.current_run.is_plain_sequence {
                // Copy values(sequence of different values) from buffer
                builder.reserve_exact(values_to_read);
                let bytes_written = builder.len();
                #[allow(clippy::uninit_vec)]
                unsafe {
                    builder.set_len(bytes_written + values_to_read)
                };
                let count = self.file_reader.read(&mut builder[bytes_written..])?;
                unsafe { builder.set_len(bytes_written + count) };
                count
            } else {
                builder.put_bytes(self.run_value, values_to_read);
                values_to_read
            };

            remaining_values -= values_read;
            self.current_run.increment_consumed(values_read);
        }

        if !builder.is_empty() {
            Ok(Some(arrow::buffer::Buffer::from_vec(builder)))
        } else {
            Ok(None)
        }
    }

    fn read_next_block(&mut self) -> crate::Result<bool> {
        // First byte of block contains the RLE header.
        let mut byte_buf = [0u8; 1];
        let count = self.file_reader.read(&mut byte_buf)?;
        if count == 0 {
            return Ok(false);
        }
        let header = byte_buf[0] as i8;
        self.current_run = RleState::from_v1_header(header);

        // Read repeated value of run if this is not a 'plain sequence of literals'.
        if !self.current_run.is_plain_sequence {
            let count = self.file_reader.read(&mut byte_buf)?;
            if count == 0 {
                return Err(OrcError::MalformedRleBlock);
            }
            self.run_value = byte_buf[0];
        }

        Ok(true)
    }
}

pub(crate) struct BooleanRleDecoder<Input> {
    rle: ByteRleDecoder<Input>,
    completed: bool,
}

impl<Input: std::io::Read> BooleanRleDecoder<Input> {
    pub fn new(file_reader: Input, buffer_size: usize) -> Self {
        Self {
            rle: ByteRleDecoder::new(file_reader, buffer_size),
            completed: false,
        }
    }

    pub fn read(
        &mut self,
        batch_size: usize,
    ) -> crate::Result<Option<arrow::buffer::BooleanBuffer>> {
        if self.completed {
            return Ok(None);
        }

        if let Some(buf) = self.rle.read(batch_size)? {
            let len = buf.len();
            Ok(Some(arrow::buffer::BooleanBuffer::new(buf, 0, len * 8)))
        } else {
            self.completed = true;
            Ok(None)
        }
    }
}

pub(crate) struct IntRleDecoder<Input, IntType> {
    v1: Option<IntRleV1Decoder<Input, IntType>>,
    v2: Option<IntRleV1Decoder<Input, IntType>>,
}

impl<Input, IntType> IntRleDecoder<Input, IntType> {
    pub fn new_v1(v1: IntRleV1Decoder<Input, IntType>) -> Self {
        Self {
            v1: Some(v1),
            v2: None,
        }
    }

    pub fn read<const N: usize, const M: usize>(
        &mut self,
        batch_size: usize,
    ) -> crate::Result<Option<arrow::buffer::ScalarBuffer<IntType>>>
    where
        IntType: Integer<N, M> + arrow::datatypes::ArrowNativeType,
        Input: std::io::Read,
    {
        self.v1
            .as_mut()
            .or(self.v2.as_mut())
            .unwrap()
            .read(batch_size)
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
    file_reader: std::io::BufReader<Input>,
    completed: bool,
    // State of current run.
    current_run: RleState,
    // Delta value for the current RLE run. This is second byte in run, right after the header.
    delta: i8,
    // First value in the current RLE run which is used as base for a computation of run elements.
    // This is third byte in run, in follows after the delta.
    base_value: IntType,
}

impl<Input: std::io::Read, IntType> IntRleV1Decoder<Input, IntType> {
    pub fn new<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize>(
        file_reader: Input,
        buffer_size: usize,
    ) -> Self
    where
        IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE>,
    {
        let cap = cmp::max(
            buffer_size,
            1 /* header */ + 1 /* delta */ + MAX_ENCODED_SIZE,
        );
        Self {
            file_reader: std::io::BufReader::with_capacity(cap, file_reader),
            completed: false,
            current_run: RleState::empty(),
            delta: 0,
            base_value: IntType::ZERO,
        }
    }

    pub fn read<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize>(
        &mut self,
        batch_size: usize,
    ) -> crate::Result<Option<arrow::buffer::ScalarBuffer<IntType>>>
    where
        IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE> + arrow::datatypes::ArrowNativeType,
    {
        if self.completed {
            return Ok(None);
        }

        let mut result_buffer_length = 0;
        let mut builder = BytesMut::with_capacity(batch_size * TYPE_SIZE);
        let mut remaining_values = batch_size;
        while remaining_values > 0 {
            // Current RLE run completed or buffer with values exhausted.
            if !self.current_run.has_values() && !self.read_next_block()? {
                // No more data to decode
                self.completed = true;
                break;
            }

            let count = if self.current_run.is_plain_sequence {
                // Take sequence of different varint values from buffer.
                let count = cmp::min(self.current_run.remaining(), remaining_values);
                for _ in 0..count {
                    let (val, _) = IntType::varint_decode(&mut self.file_reader)?;
                    builder.put_slice(&val.to_le_bytes());
                }
                count
            } else {
                // Values are based on delta and base value
                let count = cmp::min(self.current_run.remaining(), remaining_values);
                let mut remains = count;

                let mut next_value = self.base_value;
                // If this is a start of new RLE run, write first value without adding the delta.
                // This will handle the case when requested batch size is less than RLE run length.
                // For instance, we have run: length=10, base/first value=1, delta=1.
                // User requests 2 batches, each of size 5. First call will execute next branch and will
                // write base/first value 1 and go to the usual case which will write another 4 values(2,3,4,5).
                // After this call `self.base_value=5`. Second call will skip next block and will go directly
                // to the standard case and will start from adding a delta which will produce 6,7,8,9,10.
                if self.current_run.consumed() == 0 {
                    builder.put_slice(&next_value.to_le_bytes());
                    remains -= 1;
                }
                for _ in 0..remains {
                    next_value = next_value.overflow_add_i8(self.delta);
                    builder.put_slice(&next_value.to_le_bytes());
                }
                self.base_value = next_value;
                count
            };

            remaining_values -= count;
            result_buffer_length += count;
            self.current_run.increment_consumed(count);
        }

        if !builder.is_empty() {
            Ok(Some(arrow::buffer::ScalarBuffer::new(
                arrow::buffer::Buffer::from_vec(builder.to_vec()),
                0,
                result_buffer_length,
            )))
        } else {
            Ok(None)
        }
    }

    fn read_next_block<const N: usize, const M: usize>(&mut self) -> crate::Result<bool>
    where
        IntType: Integer<N, M>,
    {
        // First byte of block contains the RLE header.
        let mut byte_buf = [0u8; 1];
        let count = self.file_reader.read(&mut byte_buf)?;
        if count == 0 {
            return Ok(false);
        }
        let header = byte_buf[0] as i8;
        self.current_run = RleState::from_v1_header(header);

        if !self.current_run.is_plain_sequence {
            // Decode delta and base value of RLE run.
            let count = self.file_reader.read(&mut byte_buf)?;
            if count == 0 {
                return Err(OrcError::MalformedRleBlock);
            }
            self.delta = byte_buf[0] as i8;
            let (value, _) = IntType::varint_decode(&mut self.file_reader)?;
            self.base_value = value;
        }

        Ok(true)
    }
}

enum RleV2Type {
    /// The short repeat encoding is used for short repeating integer sequences
    /// with the goal of minimizing the overhead of the header.
    /// All of the bits listed in the header are from the first byte to the last
    /// and from most significant bit to least significant bit.
    /// If the type is signed, the value is zigzag encoded.
    ///
    /// Wire format:
    /// - 1 byte header:
    ///     - 2 bits for encoding type (0)
    ///     - 3 bits for width (W) of repeating value (1 to 8 bytes)
    ///     - 3 bits for repeat count (3 to 10 values)
    /// - W bytes in big endian format, which is zigzag encoded if they type is signed
    ///
    /// The unsigned sequence of [10000, 10000, 10000, 10000, 10000] would be serialized
    /// with short repeat encoding (0), a width of 2 bytes (1), and repeat count
    /// of 5 (2) as [0x0a, 0x27, 0x10].
    ShortRepeat(u8, u8),
    /// The direct encoding is used for integer sequences whose values have a relatively
    /// constant bit width. It encodes the values directly using a fixed width big endian encoding.
    ///
    /// Wire format:
    ///  - 2 bytes header
    ///     - 2 bits for encoding type (1)
    ///     - 5 bits for encoded width (W) of values (1 to 64 bits) using the 5 bit width encoding table
    ///     - 9 bits for length (L) (1 to 512 values)
    ///  - W * L bits (padded to the next byte) encoded in big endian format,
    /// which is zigzag encoding if the type is signed.
    ///
    /// The unsigned sequence of [23713, 43806, 57005, 48879] would be serialized with direct encoding (1),
    /// a width of 16 bits (15), and length of 4 (3) as [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef].
    Direct(u8, u16, u16),
    /// The Delta encoding is used for monotonically increasing or decreasing sequences.
    /// The first two numbers in the sequence can not be identical, because the encoding
    /// is using the sign of the first delta to determine if the series is increasing or decreasing.
    ///
    /// Wire format:
    ///  - 2 bytes header
    ///     - 2 bits for encoding type (3)
    ///     - 5 bits for encoded width (W) of deltas (0 to 64 bits) using the 5 bit width encoding table
    ///     - 9 bits for run length (L) (1 to 512 values)
    ///  - Base value - encoded as (signed or unsigned) varint
    ///  - Delta base - encoded as signed varint
    ///  - Delta values (W * (L - 2)) bytes - encode each delta after the first one. If the delta base is positive, the sequence is increasing and if it is negative the sequence is decreasing.
    ///
    /// The unsigned sequence of [2, 3, 5, 7, 11, 13, 17, 19, 23, 29] would be serialized with delta encoding (3), a width of 4 bits (3), length of 10 (9), a base of 2 (2), and first delta of 1 (2). The resulting sequence is [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46].
    Delta(),
}

const V2_DIRECT_WIDTH_TABLE: [u8; 32] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 28,
    30, 32, 40, 48, 56, 64,
];

impl RleV2Type {
    fn parse(header_bytes: &[u8]) -> crate::Result<Self> {
        if header_bytes.is_empty() {
            return Err(OrcError::MalformedRleBlock);
        }

        match header_bytes[0] >> 6 {
            0 => {
                let width = ((header_bytes[0] & 0x3F) >> 3) + 1;
                let repeat_cnt = (header_bytes[0] & 0x7) + 3;
                Ok(RleV2Type::ShortRepeat(width, repeat_cnt))
            }
            1 => {
                if header_bytes.len() < 2 {
                    return Err(OrcError::MalformedRleBlock);
                }

                // Bits from 5 to 1 contains encoded value width.
                let width_index = ((header_bytes[0] & 0x3E) >> 1) as usize;
                let bit_width = V2_DIRECT_WIDTH_TABLE[width_index];
                // Least significant bit from first byte + 8 bits of second byte contains
                // number of values stored in this direct encoded sequence.
                let mut num_values = ((header_bytes[0] & 0x1) as u16) << 8;
                num_values |= header_bytes[1] as u16;
                num_values += 1;
                // Compute size(in bytes) of the direct encoded values.
                let bytes: u16 = ((bit_width as u16 * num_values) + 7) & (-8i16 as u16); // round up to the next byte
                Ok(RleV2Type::Direct(bit_width, num_values, bytes))
            }
        }
    }
}

pub(crate) struct IntRleV2Decoder<Input, IntType> {
    file_reader: std::io::BufReader<Input>,
    completed: bool,
    // State of current run.
    current_run: RleState,
    // Delta value for the current RLE run. This is second byte in run, right after the header.
    delta: i8,
    // First value in the current RLE run which is used as base for a computation of run elements.
    // This is third byte in run, in follows after the delta.
    base_value: IntType,
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

    // Set buffer size to min value to test how RLE will behave when minimal data is in memory.
    const BUFFER_SIZE: usize = 1;

    // TODO: add property based tests

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
        let values: Vec<i8> = vec![0, 10, i8::MAX, -1, i8::MIN];
        let (source_data, expected_data) = prepare_int_rle_seq_data(values);
        validate_int_rle::<1, 2, i8>(source_data, expected_data)
    }

    #[test]
    fn i8_rle_v1_run() -> googletest::Result<()> {
        let values: Vec<(i8, i8, i8)> = vec![
            (3, 0, 1),
            (40, 10, -1),
            (64, -64, -1),
            (64, -64, 1),
            (64, 63, 1),
            (127, 63, -1),
            (127, 127, -1),
            (127, -128, 1),
            (127, -1, -1),
            (127, 0, 1),
            (127, 0, -1),
            (127, -1, -1),
        ];
        let (source_data, expected_data) = prepare_int_rle_run_data(values);
        validate_int_rle::<1, 2, i8>(source_data, expected_data)
    }

    #[test]
    fn i16_rle_v1_sequence() -> googletest::Result<()> {
        let values: Vec<i16> = vec![0, 10, 17408, 50176u16 as i16, i16::MAX, -1, i16::MIN];
        let (source_data, expected_data) = prepare_int_rle_seq_data(values);
        validate_int_rle::<2, 3, i16>(source_data, expected_data)
    }

    #[test]
    fn i16_rle_v1_run() -> googletest::Result<()> {
        let values: Vec<(i8, i16, i8)> = vec![
            (3, 0, 1),
            (40, 10, -1),
            (33, 5, 44),
            (87, 8763, 115),
            (127, -1, -1),
            (127, 0, -1),
            (64, i16::MIN / 2, 1),
            (64, i16::MIN / 2, -1),
            (64, i16::MAX / 2, 1),
            (64, i16::MAX / 2, -1),
            (i8::MAX, i16::MAX, -1),
            (i8::MAX, i16::MIN, 1),
            (i8::MAX, i16::MAX - i8::MAX as i16, 1),
            (i8::MAX, i16::MIN + i8::MAX as i16, 1),
            (33, i16::MAX - 33 * i8::MAX as i16, i8::MAX),
            (33, i16::MIN - 33 * i8::MIN as i16, i8::MIN),
        ];
        let (source_data, expected_data) = prepare_int_rle_run_data(values);
        validate_int_rle::<2, 3, i16>(source_data, expected_data)
    }

    #[test]
    fn i32_rle_v1_sequence() -> googletest::Result<()> {
        let values: Vec<i32> = vec![
            0,
            10,
            2730491968u32 as i32,
            583008320,
            i32::MAX,
            -1,
            i32::MIN,
        ];
        let (source_data, expected_data) = prepare_int_rle_seq_data(values);
        validate_int_rle::<4, 5, i32>(source_data, expected_data)
    }

    #[test]
    fn i32_rle_v1_run() -> googletest::Result<()> {
        let values: Vec<(i8, i32, i8)> = vec![
            (3, 0, 1),
            (40, 10, -1),
            (33, 5, 44),
            (87, 8763, 115),
            (127, -1, -1),
            (127, 0, -1),
            (64, i32::MIN / 2, 1),
            (64, i32::MIN / 2, -1),
            (64, i32::MAX / 2, 1),
            (64, i32::MAX / 2, -1),
            (i8::MAX, i32::MAX, -1),
            (i8::MAX, i32::MIN, 1),
            (i8::MAX, i32::MAX - i8::MAX as i32, 1),
            (i8::MAX, i32::MIN + i8::MAX as i32, 1),
            (33, i32::MAX - 33 * i8::MAX as i32, i8::MAX),
            (33, i32::MIN - 33 * i8::MIN as i32, i8::MIN),
        ];
        let (source_data, expected_data) = prepare_int_rle_run_data(values);
        validate_int_rle::<4, 5, i32>(source_data, expected_data)
    }

    #[test]
    fn i64_rle_v1_sequence() -> googletest::Result<()> {
        let values: Vec<i64> = vec![
            0,
            10,
            0x4F010000A2C00040,
            0xCF010000A2C00040u64 as i64,
            i64::MAX,
            -1,
            i64::MIN,
        ];
        let (source_data, expected_data) = prepare_int_rle_seq_data(values);
        validate_int_rle::<8, 10, i64>(source_data, expected_data)
    }

    #[test]
    fn i64_rle_v1_run() -> googletest::Result<()> {
        let values: Vec<(i8, i64, i8)> = vec![
            (3, 0, 1),
            (40, 10, -1),
            (33, 5, 44),
            (87, 8763, 115),
            (127, -1, -1),
            (127, 0, -1),
            (64, i64::MIN / 2, 1),
            (64, i64::MIN / 2, -1),
            (64, i64::MAX / 2, 1),
            (64, i64::MAX / 2, -1),
            (i8::MAX, i64::MAX, -1),
            (i8::MAX, i64::MIN, 1),
            (i8::MAX, i64::MAX - i8::MAX as i64, 1),
            (i8::MAX, i64::MIN + i8::MAX as i64, 1),
            (33, i64::MAX - 33 * i8::MAX as i64, i8::MAX),
            (33, i64::MIN - 33 * i8::MIN as i64, i8::MIN),
        ];
        let (source_data, expected_data) = prepare_int_rle_run_data(values);
        validate_int_rle::<8, 10, i64>(source_data, expected_data)
    }

    #[test]
    fn u8_rle_v1_sequence() -> googletest::Result<()> {
        let values: Vec<u8> = vec![0, 1, 10, u8::MAX, u8::MIN];
        let (source_data, expected_data) = prepare_int_rle_seq_data(values);
        validate_int_rle::<1, 2, u8>(source_data, expected_data)
    }

    #[test]
    fn u8_rle_v1_run() -> googletest::Result<()> {
        let values: Vec<(i8, u8, i8)> = vec![
            (3, 0, 1),
            (40, 10, 1),
            (64, 63, 1),
            (127, 127, -1),
            (127, 0, 1),
        ];
        let (source_data, expected_data) = prepare_int_rle_run_data(values);
        validate_int_rle::<1, 2, u8>(source_data, expected_data)
    }

    #[test]
    fn u16_rle_v1_sequence() -> googletest::Result<()> {
        let values: Vec<u16> = vec![0, 10, 17408, 50176, u16::MAX, u16::MIN];
        let (source_data, expected_data) = prepare_int_rle_seq_data(values);
        validate_int_rle::<2, 3, u16>(source_data, expected_data)
    }

    #[test]
    fn u16_rle_v1_run() -> googletest::Result<()> {
        let values: Vec<(i8, u16, i8)> = vec![
            (3, 0, 1),
            (40, 10, 1),
            (33, 5, 44),
            (87, 8763, 115),
            (127, 0, 1),
            (64, 0, 1),
            (64, u16::MAX / 2, 1),
            (64, u16::MAX / 2, -1),
            (i8::MAX, u16::MAX, -1),
            (i8::MAX, u16::MAX - i8::MAX as u16, 1),
            (i8::MAX, u16::MIN + i8::MAX as u16, 1),
            (33, u16::MAX - 33 * i8::MAX as u16, i8::MAX),
        ];
        let (source_data, expected_data) = prepare_int_rle_run_data(values);
        validate_int_rle::<2, 3, u16>(source_data, expected_data)
    }

    #[test]
    fn u32_rle_v1_sequence() -> googletest::Result<()> {
        let values: Vec<u32> = vec![0, 10, 583008320, 2730491968, u32::MAX, u32::MIN];
        let (source_data, expected_data) = prepare_int_rle_seq_data(values);
        validate_int_rle::<4, 5, u32>(source_data, expected_data)
    }

    #[test]
    fn u32_rle_v1_run() -> googletest::Result<()> {
        let values: Vec<(i8, u32, i8)> = vec![
            (3, 0, 1),
            (40, 10, 1),
            (33, 5, 44),
            (87, 8763, 115),
            (127, 0, 1),
            (64, 0, 1),
            (64, u32::MAX / 2, 1),
            (64, u32::MAX / 2, -1),
            (i8::MAX, u32::MAX, -1),
            (i8::MAX, u32::MAX - i8::MAX as u32, 1),
            (i8::MAX, u32::MIN + i8::MAX as u32, 1),
            (33, u32::MAX - 33 * i8::MAX as u32, i8::MAX),
        ];
        let (source_data, expected_data) = prepare_int_rle_run_data(values);
        validate_int_rle::<4, 5, u32>(source_data, expected_data)
    }

    #[test]
    fn u64_rle_v1_sequence() -> googletest::Result<()> {
        let values: Vec<u64> = vec![
            0,
            10,
            0x4F010000A2C00040,
            0xCF010000A2C00040,
            u64::MAX,
            u64::MIN,
        ];
        let (source_data, expected_data) = prepare_int_rle_seq_data(values);
        validate_int_rle::<8, 10, u64>(source_data, expected_data)
    }

    #[test]
    fn u64_rle_v1_run() -> googletest::Result<()> {
        let values: Vec<(i8, u64, i8)> = vec![
            (3, 0, 1),
            (40, 10, 1),
            (33, 5, 44),
            (87, 8763, 115),
            (127, 0, 1),
            (64, 0, 1),
            (64, u64::MAX / 2, 1),
            (64, u64::MAX / 2, -1),
            (i8::MAX, u64::MAX, -1),
            (i8::MAX, u64::MAX - i8::MAX as u64, 1),
            (i8::MAX, u64::MIN + i8::MAX as u64, 1),
            (33, u64::MAX - 33 * i8::MAX as u64, i8::MAX),
        ];
        let (source_data, expected_data) = prepare_int_rle_run_data(values);
        validate_int_rle::<8, 10, u64>(source_data, expected_data)
    }

    fn validate_int_rle<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize, IntType>(
        encoded_data: Bytes,
        expected_data: Bytes,
    ) -> googletest::Result<()>
    where
        IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE> + arrow::datatypes::ArrowNativeType,
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

    /// Prepare RLE run which consist of sequence of different values.
    fn prepare_int_rle_seq_data<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize, IntType>(
        values: Vec<IntType>,
    ) -> (Bytes, Bytes)
    where
        IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE>,
    {
        let mut encoded_data = BytesMut::new();
        let mut expected_data = BytesMut::new();
        assert!(values.len() <= 128);
        encoded_data.put_i8((values.len() as i8).neg());
        for v in values {
            let (encoded, size) = v.as_varint();
            encoded_data.put_slice(&encoded[..size]);
            expected_data.put_slice(&v.to_le_bytes());
        }
        (encoded_data.freeze(), expected_data.freeze())
    }

    /// Prepare RLE run which consist of base value and delta.
    fn prepare_int_rle_run_data<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize, IntType>(
        values: Vec<(i8, IntType, i8)>,
    ) -> (Bytes, Bytes)
    where
        IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE>,
    {
        let mut encoded_data = BytesMut::new();
        let mut expected_data = BytesMut::new();
        for (length, mut base, delta) in values {
            assert!(length >= 3);

            encoded_data.put_i8(length - 3);
            encoded_data.put_slice(&delta.to_le_bytes());
            let (encoded, size) = base.as_varint();
            encoded_data.put_slice(&encoded[..size]);

            for _ in 0..length {
                expected_data.put_slice(&base.to_le_bytes());
                base = base.overflow_add_i8(delta);
            }
        }
        (encoded_data.freeze(), expected_data.freeze())
    }
}
