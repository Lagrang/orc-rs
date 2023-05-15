use std::cmp;
use std::collections::VecDeque;
use std::io::Read;

use bytes::{BufMut, BytesMut};

use crate::OrcError;

use super::Integer;

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

struct TableCodedValues<'a> {
    coded_data: &'a mut dyn Read,
    bit_width: u8,
}

impl<'a> TableCodedValues<'a> {
    fn new(input: &'a mut dyn Read, bit_width: u8) -> Self {
        Self {
            coded_data: input,
            bit_width,
        }
    }

    fn decode<
        const TYPE_SIZE: usize,
        const MAX_ENCODED_SIZE: usize,
        IntType: Integer<TYPE_SIZE, MAX_ENCODED_SIZE>,
    >(
        &self,
        values: usize,
    ) -> std::io::Result<Vec<IntType>> {
        Ok(vec![])
    }
}

impl RleV2Type {
    fn parse(rle_bytes: &mut dyn Read) -> crate::Result<Self> {
        let mut first_byte: [u8; 1] = [0];
        rle_bytes.read_exact(&mut first_byte)?;
        let first_byte = first_byte[0];

        match first_byte >> 6 {
            0 => {
                let width = ((first_byte & 0x3F) >> 3) + 1;
                let repeat_cnt = (first_byte & 0x7) + 3;
                Ok(RleV2Type::ShortRepeat(width, repeat_cnt))
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
                Ok(RleV2Type::Direct(bit_width, num_values, bytes))
            }
            3 => {
                let mut second_byte: [u8; 1] = [0];
                rle_bytes.read_exact(&mut second_byte)?;
                let second_byte = second_byte[0];

                // Bits from 5 to 1 contains encoded value width.
                let width_index = ((first_byte & 0x3E) >> 1) as usize;
                let bit_width = V2_DIRECT_WIDTH_TABLE[width_index];
                // Least significant bit from first byte + 8 bits of second byte contains
                // number of values stored in this delta encoded sequence.
                let mut num_values = ((first_byte & 0x1) as u16) << 8;
                num_values |= second_byte as u16;
                num_values += 1;
                // Compute size(in bytes) of the delta encoded values.
                let bytes: u16 = ((bit_width as u16 * (num_values - 2)) + 7) & (-8i16 as u16); // round up to the next byte
                Ok(RleV2Type::Delta(bit_width, num_values, bytes))
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

    fn parse(rle_bytes: &mut dyn Read) -> crate::Result<Self> {
        let rle_type = RleV2Type::parse(rle_bytes)?;
        let state = match rle_type {
            RleV2Type::ShortRepeat(byte_width, repeat_count) => {
                let repeated_value = IntType::from_coded_be_bytes(rle_bytes, byte_width as usize)?;
                Self {
                    length: repeat_count as usize,
                    consumed: 0,
                    rle_type,
                    base_value: repeated_value,
                    delta_base: IntType::SignedCounterpart::ZERO,
                    deltas: VecDeque::new(),
                    decoded_values: VecDeque::new(),
                }
            }
            RleV2Type::Direct(bit_width, num_values, _) => {
                let decoded_values =
                    TableCodedValues::new(rle_bytes, bit_width).decode(num_values as usize - 2)?;
                Self {
                    length: num_values as usize,
                    consumed: 0,
                    rle_type,
                    base_value: IntType::ZERO,
                    delta_base: IntType::SignedCounterpart::ZERO,
                    deltas: VecDeque::new(),
                    decoded_values: VecDeque::from(decoded_values),
                }
            }
            RleV2Type::Delta(bit_width, num_values, _) => {
                let base_value = IntType::from_varint(rle_bytes)?.0;
                let delta = IntType::SignedCounterpart::from_varint(rle_bytes)?.0;
                let mut deltas = Vec::new();
                if bit_width > 0 {
                    deltas.push(delta);
                    deltas.append(
                        &mut TableCodedValues::new(rle_bytes, bit_width)
                            .decode(num_values as usize - 2)?,
                    );
                }
                Self {
                    length: num_values as usize,
                    consumed: 0,
                    rle_type,
                    base_value,
                    delta_base: delta,
                    deltas: VecDeque::from(deltas),
                    decoded_values: VecDeque::new(),
                }
            }
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

pub(crate) struct IntRleV2Decoder<
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
        self.current_run = RleV2State::parse(&mut self.file_reader)?;

        match &self.current_run.rle_type {
            // RleV2Type::ShortRepeat(_, _, bytes) => {}
            RleV2Type::Delta(_, _, bytes) => {}
            _ => {}
        }

        Ok(true)
    }
}
