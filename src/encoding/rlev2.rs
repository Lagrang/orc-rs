use std::cmp;
use std::collections::VecDeque;
use std::io::Read;

use bytes::{BufMut, BytesMut};

use crate::OrcError;

use super::{Integer, SignedInteger};

#[derive(Copy, Clone)]
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
    Delta(u8, u16, u16),
}

const V2_DIRECT_WIDTH_TABLE: [u8; 32] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 28,
    30, 32, 40, 48, 56, 64,
];

const V2_DELTA_WIDTH_TABLE: [u8; 32] = [
    0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 28,
    30, 32, 40, 48, 56, 64,
];

struct TableCodedValues<'a> {
    coded_data: &'a mut dyn Read,
    bit_width: u8,
    /// Last byte read from coded stream
    byte: [u8; 1],
    /// How many bits not read from the last byte
    byte_bits_remains: u8,
}

impl<'a> TableCodedValues<'a> {
    fn new(input: &'a mut dyn Read, bit_width: u8) -> Self {
        Self {
            coded_data: input,
            bit_width,
            byte: [0u8; 1],
            byte_bits_remains: 0,
        }
    }

    fn decode<
        const TYPE_SIZE: usize,
        const MAX_ENCODED_SIZE: usize,
        IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE>,
    >(
        &mut self,
        read_count: usize,
    ) -> crate::Result<Vec<IntType>> {
        // TODO: unrolled+AVX version of table decoding(see C++ UnpackDefault::readLongs, BitUnpackAVX512::readLongs)

        let zero = IntType::from_byte(0);
        let one = IntType::from_byte(1);
        let eight = IntType::from_byte(8);
        let mut results: Vec<IntType> = Vec::with_capacity(read_count);
        for _ in 0..read_count {
            let mut result: IntType = IntType::ZERO;
            let mut total_bits_remains = IntType::from_byte(self.bit_width);
            let mut byte_bits_remains = IntType::from_byte(self.byte_bits_remains);
            while total_bits_remains > byte_bits_remains {
                result <<= byte_bits_remains;
                result |= IntType::from_byte(self.byte[0]) & ((one << byte_bits_remains) - one);
                total_bits_remains -= byte_bits_remains;
                // New byte were read from the RLE block, reset bit counters
                byte_bits_remains = eight;
                if self.coded_data.read(&mut self.byte)? == 0 {
                    byte_bits_remains = zero;
                    break;
                }
            }

            if total_bits_remains > zero {
                result <<= total_bits_remains;
                byte_bits_remains -= total_bits_remains;
                result |= (IntType::from_byte(self.byte[0]) >> byte_bits_remains)
                    & ((one << total_bits_remains) - one);
            }
            self.byte_bits_remains = byte_bits_remains.truncate_to_u8();

            results.push(result);
        }

        Ok(results)
    }
}

impl RleV2Type {
    fn parse(rle_bytes: &mut dyn Read) -> crate::Result<Option<Self>> {
        let mut first_byte: [u8; 1] = [0];
        let bytes_read = rle_bytes.read(&mut first_byte)?;
        if bytes_read == 0 {
            return Ok(None);
        }

        let first_byte = first_byte[0];

        match (first_byte >> 6) & 0x03 {
            0 => {
                let width = ((first_byte & 0x3F) >> 3) + 1;
                let repeat_cnt = (first_byte & 0x7) + 3;
                Ok(Some(RleV2Type::ShortRepeat(width, repeat_cnt)))
            }
            1 => {
                let mut second_byte: [u8; 1] = [0];
                rle_bytes.read_exact(&mut second_byte)?;
                let second_byte = second_byte[0];

                // Bits from 5 to 1 contains encoded value width.
                let width_index = ((first_byte & 0x3E) >> 1) as usize;
                let bit_width = V2_DIRECT_WIDTH_TABLE[width_index];
                // Least significant bit from first byte + 8 bits of second byte contains
                // number of values stored in this direct encoded sequence.
                let mut num_values = ((first_byte & 0x1) as u16) << 8;
                num_values |= second_byte as u16;
                num_values += 1;
                // Compute size(in bytes) of the direct encoded values.
                let bytes: u16 = ((bit_width as u16 * num_values) + 7) & (-8i16 as u16); // round up to the next byte
                Ok(Some(RleV2Type::Direct(bit_width, num_values, bytes)))
            }
            3 => {
                let mut second_byte: [u8; 1] = [0];
                rle_bytes.read_exact(&mut second_byte)?;
                let second_byte = second_byte[0];

                // Bits from 5 to 1 contains encoded value width.
                let width_index = ((first_byte & 0x3E) >> 1) as usize;
                let bit_width = V2_DELTA_WIDTH_TABLE[width_index];
                // Least significant bit from first byte + 8 bits of second byte contains
                // number of values stored in this delta encoded sequence.
                let mut num_values = ((first_byte & 0x1) as u16) << 8;
                num_values |= second_byte as u16;
                num_values += 1;
                // Compute size(in bytes) of the delta encoded values.
                let bytes: u16 = ((bit_width as u16 * (num_values - 2)) + 7) & (-8i16 as u16); // round up to the next byte
                Ok(Some(RleV2Type::Delta(bit_width, num_values, bytes)))
            }
            2 => {
                todo!()
            }
            _ => Err(crate::OrcError::MalformedRleBlock),
        }
    }
}

struct RleV2State<
    const TYPE_SIZE: usize,
    const MAX_ENCODED_SIZE: usize,
    IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE>,
> {
    // Number of values in RLE.
    length: usize,
    // How many items of RLE already consumed
    consumed: usize,
    rle_type: RleV2Type,
    // Next 3 field used for delta encoding
    base_value: IntType,
    delta_base: IntType::SignedCounterpart,
    deltas: VecDeque<IntType::SignedCounterpart>,
    // Used for direct encoding to store decoded values.
    decoded_values: VecDeque<IntType>,
}

impl<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize, IntType>
    RleV2State<TYPE_SIZE, MAX_ENCODED_SIZE, IntType>
where
    IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE>,
{
    fn empty() -> Self {
        Self {
            length: 0,
            consumed: 0,
            rle_type: RleV2Type::ShortRepeat(0, 0),
            base_value: IntType::ZERO,
            delta_base: IntType::SignedCounterpart::ZERO,
            deltas: VecDeque::new(),
            decoded_values: VecDeque::new(),
        }
    }

    fn parse(rle_bytes: &mut dyn Read) -> crate::Result<Option<Self>> {
        let rle_type = RleV2Type::parse(rle_bytes)?;
        let state = match rle_type {
            Some(v2_type @ RleV2Type::ShortRepeat(byte_width, repeat_count)) => {
                let repeated_value =
                    IntType::from_coded_big_endian(rle_bytes, byte_width as usize)?;
                Some(Self {
                    length: repeat_count as usize,
                    consumed: 0,
                    rle_type: v2_type,
                    base_value: repeated_value,
                    delta_base: IntType::SignedCounterpart::ZERO,
                    deltas: VecDeque::new(),
                    decoded_values: VecDeque::new(),
                })
            }
            Some(v2_type @ RleV2Type::Direct(bit_width, num_values, _)) => {
                let decoded_values =
                    TableCodedValues::new(rle_bytes, bit_width).decode(num_values as usize - 2)?;
                Some(Self {
                    length: num_values as usize,
                    consumed: 0,
                    rle_type: v2_type,
                    base_value: IntType::ZERO,
                    delta_base: IntType::SignedCounterpart::ZERO,
                    deltas: VecDeque::new(),
                    decoded_values: VecDeque::from(decoded_values),
                })
            }
            Some(v2_type @ RleV2Type::Delta(bit_width, num_values, _)) => {
                let base_value = IntType::from_varint(rle_bytes)?.0;
                let base_delta = IntType::SignedCounterpart::from_varint(rle_bytes)?.0;
                let mut deltas = vec![base_delta];
                if bit_width > 0 {
                    let mut next_deltas: Vec<IntType::SignedCounterpart> =
                        TableCodedValues::new(rle_bytes, bit_width)
                            .decode(num_values as usize - 2)?;
                    // If base delta is negative, when all following deltas should be negative too.
                    if base_delta < IntType::SignedCounterpart::ZERO {
                        for v in &mut next_deltas {
                            *v = v.negate();
                        }
                    }
                    deltas.append(&mut next_deltas);
                }
                Some(Self {
                    length: num_values as usize,
                    consumed: 0,
                    rle_type: v2_type,
                    base_value,
                    delta_base: base_delta,
                    deltas: VecDeque::from(deltas),
                    decoded_values: VecDeque::new(),
                })
            }
            None => None,
        };

        Ok(state)
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

pub struct IntRleV2Decoder<
    const TYPE_SIZE: usize,
    const MAX_ENCODED_SIZE: usize,
    Input,
    IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE>,
> {
    file_reader: std::io::BufReader<Input>,
    completed: bool,
    // State of current run.
    current_run: RleV2State<TYPE_SIZE, MAX_ENCODED_SIZE, IntType>,
}

impl<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize, Input: std::io::Read, IntType>
    IntRleV2Decoder<TYPE_SIZE, MAX_ENCODED_SIZE, Input, IntType>
where
    IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE>,
{
    pub fn new(file_reader: Input, buffer_size: usize) -> Self {
        let cap = cmp::max(buffer_size, 4 /* header */+ MAX_ENCODED_SIZE * 2);
        Self {
            file_reader: std::io::BufReader::with_capacity(cap, file_reader),
            completed: false,
            current_run: RleV2State::empty(),
        }
    }

    pub fn read(
        &mut self,
        batch_size: usize,
    ) -> crate::Result<Option<arrow::buffer::ScalarBuffer<IntType>>>
    where
        IntType: arrow::datatypes::ArrowNativeType,
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

            let count = match &self.current_run.rle_type {
                RleV2Type::ShortRepeat(_, _) => {
                    // Return same value N times.
                    let count = cmp::min(self.current_run.remaining(), remaining_values);
                    let value_bytes = self.current_run.base_value.to_le_bytes();
                    for _ in 0..count {
                        builder.put_slice(&value_bytes);
                    }
                    count
                }
                RleV2Type::Direct(_, _, _) => {
                    let count = cmp::min(self.current_run.remaining(), remaining_values);
                    for _ in 0..count {
                        let next_val = self
                            .current_run
                            .decoded_values
                            .pop_front()
                            .ok_or(OrcError::MalformedRleBlock)?;
                        builder.put_slice(&next_val.to_le_bytes());
                    }
                    count
                }
                RleV2Type::Delta(bit_width, _, _) => {
                    // Values are based on delta and base value
                    let count = cmp::min(self.current_run.remaining(), remaining_values);
                    let mut remains = count;
                    // If this is a start of new RLE run, write first value without adding the delta.
                    // This will handle the case when requested batch size is less than RLE run length.
                    // For instance, we have run: length=10, base/first value=1, delta=1.
                    // User requests 2 batches, each of size 5. First call will execute next branch and will
                    // write base/first value 1 and go to the usual case which will write another 4 values(2,3,4,5).
                    // After this call `self.base_value=5`. Second call will skip next block and will go directly
                    // to the standard case and will start from adding a delta which will produce 6,7,8,9,10.
                    //
                    if self.current_run.consumed() == 0 {
                        builder.put_slice(&self.current_run.base_value.to_le_bytes());
                        remains -= 1;
                    }

                    // Second value will be 'first value + delta base'. Other values will be computed
                    // same as second or will be based on delta literal values.
                    if *bit_width == 0 {
                        // We have only 1 delta value used for all values in this RLE block.
                        for _ in 0..remains {
                            self.current_run.base_value = self
                                .current_run
                                .base_value
                                .overflow_add_signed(self.current_run.delta_base);
                            builder.put_slice(&self.current_run.base_value.to_le_bytes());
                        }
                    } else {
                        // We have a sequence of delta values which should be used to compute next value.
                        for _ in 0..remains {
                            let next_delta = self
                                .current_run
                                .deltas
                                .pop_front()
                                .ok_or(OrcError::MalformedRleBlock)?;
                            self.current_run.base_value =
                                self.current_run.base_value.overflow_add_signed(next_delta);
                            builder.put_slice(&self.current_run.base_value.to_le_bytes());
                        }
                    }
                    count
                }
                _ => todo!(),
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

    fn read_next_block(&mut self) -> crate::Result<bool> {
        if let Some(next_run) = RleV2State::parse(&mut self.file_reader)? {
            self.current_run = next_run;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use googletest::matchers::eq;
    use googletest::verify_that;

    use crate::encoding::Integer;
    use crate::source::MemoryReader;

    use super::IntRleV2Decoder;

    // Set buffer size to min value to test how RLE will behave when minimal data is in memory.
    const BUFFER_SIZE: usize = 1;

    fn decode_rle_data<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize, IntegerType>(
        data: Vec<u8>,
        total_vals_to_read: usize,
        read_size: usize,
    ) -> googletest::Result<Vec<IntegerType>>
    where
        IntegerType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE> + arrow::datatypes::ArrowNativeType,
    {
        let reader = MemoryReader::from(Bytes::from(data));
        let mut rle = IntRleV2Decoder::<TYPE_SIZE, MAX_ENCODED_SIZE, _, IntegerType>::new(
            reader,
            BUFFER_SIZE,
        );

        let mut decoded_values = Vec::with_capacity(total_vals_to_read);
        for _ in (0..total_vals_to_read).step_by(read_size) {
            let array = rle.read(read_size)?;
            if let Some(buffer) = array {
                decoded_values.extend_from_slice(buffer.as_ref());
            }
        }

        Ok(decoded_values)
    }

    fn verify_decoding<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize, IntegerType>(
        data_buffer: Vec<u8>,
        expected_vals: Vec<IntegerType>,
    ) -> googletest::Result<()>
    where
        IntegerType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE> + arrow::datatypes::ArrowNativeType,
    {
        let actual: Vec<IntegerType> =
            decode_rle_data(data_buffer.clone(), expected_vals.len(), 1)?;
        verify_that!(actual, eq(expected_vals.clone()))?;
        let actual: Vec<IntegerType> =
            decode_rle_data(data_buffer.clone(), expected_vals.len(), 3)?;
        verify_that!(actual, eq(expected_vals.clone()))?;
        let actual: Vec<IntegerType> =
            decode_rle_data(data_buffer.clone(), expected_vals.len(), 7)?;
        verify_that!(actual, eq(expected_vals.clone()))?;
        let actual: Vec<IntegerType> =
            decode_rle_data(data_buffer, expected_vals.len(), expected_vals.len())?;
        verify_that!(actual, eq(expected_vals))?;
        Ok(())
    }

    #[test]
    fn orc_cpp_backported_basic_delta0() -> googletest::Result<()> {
        {
            let expected_vals: Vec<i8> = (0..20).collect();
            let data_buffer = vec![0xc0, 0x13, 0x00, 0x02];
            verify_decoding(data_buffer, expected_vals)?;
        }
        {
            let expected_vals: Vec<i16> = (0..20).collect();
            let data_buffer = vec![0xc0, 0x13, 0x00, 0x02];
            verify_decoding(data_buffer, expected_vals)?;
        }
        {
            let expected_vals: Vec<i32> = (0..20).collect();
            let data_buffer = vec![0xc0, 0x13, 0x00, 0x02];
            verify_decoding(data_buffer, expected_vals)?;
        }
        {
            let expected_vals: Vec<i64> = (0..20).collect();
            let data_buffer = vec![0xc0, 0x13, 0x00, 0x02];
            verify_decoding(data_buffer, expected_vals)?;
        }
        {
            let expected_vals: Vec<i128> = (0..20).collect();
            let data_buffer = vec![0xc0, 0x13, 0x00, 0x02];
            verify_decoding(data_buffer, expected_vals)
        }
    }

    #[test]
    fn orc_cpp_backported_basic_delta1() -> googletest::Result<()> {
        let expected_vals: Vec<i64> = vec![-500, -400, -350, -325, -310];
        let data_buffer = vec![0xce, 0x04, 0xe7, 0x07, 0xc8, 0x01, 0x32, 0x19, 0x0f];
        verify_decoding(data_buffer, expected_vals)
    }

    #[test]
    fn orc_cpp_backported_basic_delta2() -> googletest::Result<()> {
        let expected_vals: Vec<i64> = vec![-500, -600, -650, -675, -710];
        let data_buffer = vec![0xce, 0x04, 0xe7, 0x07, 0xc7, 0x01, 0x32, 0x19, 0x23];
        verify_decoding(data_buffer, expected_vals)
    }

    #[test]
    fn orc_cpp_backported_basic_delta3() -> googletest::Result<()> {
        let expected_vals: Vec<i64> = vec![500, 400, 350, 325, 310];
        let data_buffer = vec![0xce, 0x04, 0xe8, 0x07, 0xc7, 0x01, 0x32, 0x19, 0x0f];
        verify_decoding(data_buffer, expected_vals)
    }

    #[test]
    fn orc_cpp_backported_basic_delta4() -> googletest::Result<()> {
        let expected_vals: Vec<i64> = vec![500, 600, 650, 675, 710];
        let data_buffer = vec![0xce, 0x04, 0xe8, 0x07, 0xc8, 0x01, 0x32, 0x19, 0x23];
        verify_decoding(data_buffer, expected_vals)
    }

    #[test]
    fn orc_cpp_backported_basic_delta5() -> googletest::Result<()> {
        let mut expected_vals: Vec<i64> = Vec::with_capacity(65);
        for i in 0..65 {
            expected_vals.push(i - 32);
        }

        // Original values: [-32, -31, -30, ..., -1, 0, 1, 2, ..., 32]
        // 2 bytes header: 0xc0, 0x40
        //    2 bits for encoding type(3). 5 bits for bitSize which is 0 for fixed delta.
        //    9 bits for length of 65(64).
        // Base value: -32 which is 65(0x3f) after zigzag
        // Delta base: 1 which is 2(0x02) after zigzag
        let data_buffer = vec![0xc0, 0x40, 0x3f, 0x02];
        verify_decoding(data_buffer, expected_vals)
    }

    #[test]
    fn orc_cpp_backported_basic_delta0_width() -> googletest::Result<()> {
        let data_buffer = vec![0x4e, 0x2, 0x0, 0x1, 0x2, 0xc0, 0x2, 0x42, 0x0];

        let actual: Vec<i64> = decode_rle_data(data_buffer, 6, 6)?;
        verify_that!(0, eq(actual[0]))?;
        verify_that!(1, eq(actual[1]))?;
        verify_that!(2, eq(actual[2]))?;
        verify_that!(0x42, eq(actual[3]))?;
        verify_that!(0x42, eq(actual[4]))?;
        verify_that!(0x42, eq(actual[5]))?;
        Ok(())
    }

    #[test]
    fn orc_cpp_backported_short_repeats() -> googletest::Result<()> {
        signed_short_repeats_test::<1, 2, i8>()?;
        signed_short_repeats_test::<2, 3, i16>()?;
        signed_short_repeats_test::<4, 5, i32>()?;
        signed_short_repeats_test::<8, 10, i64>()?;
        signed_short_repeats_test::<16, 19, i128>()
        // signed_short_repeats_test::<1, 2, u8>()
        // signed_short_repeats_test::<2, 3, u16>()?;
        // signed_short_repeats_test::<4, 5, u32>()?;
        // signed_short_repeats_test::<8, 10, u64>()
        // signed_short_repeats_test::<16, 19, u128>()
    }

    fn signed_short_repeats_test<
        const TYPE_SIZE: usize,
        const MAX_ENCODED_SIZE: usize,
        IntegerType,
    >() -> googletest::Result<()>
    where
        IntegerType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE> + arrow::datatypes::ArrowNativeType,
    {
        let mut expected_vals: Vec<IntegerType> = Vec::with_capacity(70);
        for i in 0..10 {
            for _ in 0..7 {
                expected_vals.push(IntegerType::from_byte(i));
            }
        }

        let mut data_buffer = vec![
            0x04, 0x00, 0x04, 0x02, 0x04, 0x04, 0x04, 0x06, 0x04, 0x08, 0x04, 0x0a, 0x04, 0x0c,
            0x04, 0x0e, 0x04, 0x10, 0x04, 0x12,
        ];

        for (i, v) in data_buffer.iter_mut().enumerate() {
            if !IntegerType::IS_SIGNED && i % 2 == 0 {
                v = v.fro
            }
        }

        verify_decoding(data_buffer, expected_vals)
    }
}
