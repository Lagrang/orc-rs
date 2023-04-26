use std::ops::{
    AddAssign, BitAnd, BitOr, BitOrAssign, BitXor, Neg, Not, Shl, ShlAssign, Shr, ShrAssign,
};

pub mod rle;

/// Marker trait for signed integer types.
pub(crate) trait Integer<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize>:
    Copy
    + ByteRepr<TYPE_SIZE>
    + PartialEq
    + AddAssign
    + ShrAssign
    + ShlAssign
    + Shl<Output = Self>
    + Shr<Output = Self>
    + BitAnd<Output = Self>
    + BitOr<Output = Self>
    + BitOrAssign
    + BitXor<Output = Self>
    + Not<Output = Self>
{
    const ZERO: Self;

    /// Encode integer as 'base 128 varint' value.
    /// Description can be found [here](https://protobuf.dev/programming-guides/encoding/#varints).
    /// Order of result bytes is little endian.
    ///
    /// Returns an array with encoded value and actual size of encoded value(can be <= MAX_ENCODED_SIZE).
    fn as_varint(&self) -> ([u8; MAX_ENCODED_SIZE], usize);

    /// Decode 'base 128 varint' value.
    /// Description can be found [here](https://protobuf.dev/programming-guides/encoding/#varints).
    /// Expected order of varint bytes is little endian.
    fn varint_decode(encoded_stream: &mut dyn std::io::Read) -> std::io::Result<(Self, usize)>;

    fn overflow_add_i8(&self, other: i8) -> Self;
}

impl Integer<1, 2> for i8 {
    const ZERO: Self = 0;

    #[inline]
    fn as_varint(&self) -> ([u8; 2], usize) {
        self.zigzag_encode().as_varint()
    }

    #[inline]
    fn varint_decode(encoded_stream: &mut dyn std::io::Read) -> std::io::Result<(Self, usize)> {
        let (decoded, size): (u8, usize) = UnsignedInteger::varint_decode(encoded_stream)?;
        Ok((decoded.zigzag_decode(), size))
    }

    #[inline]
    fn overflow_add_i8(&self, other: i8) -> Self {
        self.overflowing_add(other).0
    }
}

impl Integer<2, 3> for i16 {
    const ZERO: Self = 0;

    #[inline]
    fn as_varint(&self) -> ([u8; 3], usize) {
        self.zigzag_encode().as_varint()
    }

    #[inline]
    fn varint_decode(encoded_stream: &mut dyn std::io::Read) -> std::io::Result<(Self, usize)> {
        let (decoded, size): (u16, usize) = UnsignedInteger::varint_decode(encoded_stream)?;
        Ok((decoded.zigzag_decode(), size))
    }

    #[inline]
    fn overflow_add_i8(&self, other: i8) -> Self {
        self.overflowing_add(other as i16).0
    }
}

impl Integer<4, 5> for i32 {
    const ZERO: Self = 0;

    #[inline]
    fn as_varint(&self) -> ([u8; 5], usize) {
        self.zigzag_encode().as_varint()
    }

    #[inline]
    fn varint_decode(encoded_stream: &mut dyn std::io::Read) -> std::io::Result<(Self, usize)> {
        let (decoded, size): (u32, usize) = UnsignedInteger::varint_decode(encoded_stream)?;
        Ok((decoded.zigzag_decode(), size))
    }

    #[inline]
    fn overflow_add_i8(&self, other: i8) -> Self {
        self.overflowing_add(other as i32).0
    }
}

impl Integer<8, 10> for i64 {
    const ZERO: Self = 0;

    #[inline]
    fn as_varint(&self) -> ([u8; 10], usize) {
        self.zigzag_encode().as_varint()
    }

    #[inline]
    fn varint_decode(encoded_stream: &mut dyn std::io::Read) -> std::io::Result<(Self, usize)> {
        let (decoded, size): (u64, usize) = UnsignedInteger::varint_decode(encoded_stream)?;
        Ok((decoded.zigzag_decode(), size))
    }

    #[inline]
    fn overflow_add_i8(&self, other: i8) -> Self {
        self.overflowing_add(other as i64).0
    }
}

pub(crate) trait SignedInteger<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize>:
    Integer<TYPE_SIZE, MAX_ENCODED_SIZE> + Neg<Output = Self>
{
    /// Type represent unsigned counterpart of this type, e.g. i8 => u8.
    type UnsignedCounterpart;

    /// Encode value using Zigzag algorithm.
    ///
    /// Description can be found [here](https://protobuf.dev/programming-guides/encoding/#signed-ints)
    fn zigzag_encode(&self) -> Self::UnsignedCounterpart;
}

impl SignedInteger<1, 2> for i8 {
    type UnsignedCounterpart = u8;

    #[inline]
    fn zigzag_encode(&self) -> u8 {
        ((*self << 1) ^ (*self >> 7)) as u8
    }
}

impl SignedInteger<2, 3> for i16 {
    type UnsignedCounterpart = u16;

    #[inline]
    fn zigzag_encode(&self) -> u16 {
        ((*self << 1) ^ (*self >> 15)) as u16
    }
}

impl SignedInteger<4, 5> for i32 {
    type UnsignedCounterpart = u32;

    #[inline]
    fn zigzag_encode(&self) -> u32 {
        ((*self << 1) ^ (*self >> 31)) as u32
    }
}

impl SignedInteger<8, 10> for i64 {
    type UnsignedCounterpart = u64;

    #[inline]
    fn zigzag_encode(&self) -> u64 {
        ((*self << 1) ^ (*self >> 63)) as u64
    }
}

pub(crate) trait UnsignedInteger<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize>:
    Integer<TYPE_SIZE, MAX_ENCODED_SIZE>
{
    /// Type represent unsigned counterpart of this type, e.g. u8 => i8.
    type SignedCounterpart;
    /// Value which contains a mask used by varint encoder/decoder.
    /// Mask should have following form: 7 least significant bits are set to 0,
    /// others, starting from 8 up to TYPE_SIZE * 8, set to 1.
    const VARINT_MASK: Self;
    /// Value used by varint algorithm to shift 7 bits of Self to left/right.
    const VARINT_SHIFT: Self;

    fn varint_encode(value: Self) -> ([u8; MAX_ENCODED_SIZE], usize) {
        let mut result = [0u8; MAX_ENCODED_SIZE];
        let mut i = 0;
        let mut v = value;
        // Scan bits of the value starting from least significant(LS):
        // Starting from 7th bit try to find at least 1 bit which is set.
        // If there is no such bit, encoded varint value formed.
        // Otherwise, write 7 LS bits to the output and append
        // continuation marker(one bit set to '1') and proceed to the next 7 bit.
        loop {
            // Optimistic case: always set sign bit to 1 before write to result.
            result[i] = (Self::VARINT_MASK | v).truncate_to_u8();
            if v & Self::VARINT_MASK == Self::ZERO {
                // Set MS bit to 0 to rollback optimistic case.
                result[i] &= !Self::VARINT_MASK.truncate_to_u8();
                break;
            }
            v >>= Self::VARINT_SHIFT;
            i += 1;
        }
        (result, i + 1)
    }

    /// Decode 'base 128 varint' value.
    /// Description can be found [here](https://protobuf.dev/programming-guides/encoding/#varints).
    /// Expected order of varint bytes is little endian.
    ///
    /// Examples:
    /// - 128 => [0x80, 0x01]
    /// - 16383 => [0xff, 0x7f]
    fn varint_decode(encoded_stream: &mut dyn std::io::Read) -> std::io::Result<(Self, usize)> {
        let mut result = Self::ZERO;
        let mut i = 0;
        let mut offset: Self = Self::ZERO;
        let mut byte_buf = [0; 1];
        loop {
            let count = encoded_stream.read(&mut byte_buf)?;
            if count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Malformed encoded varint value",
                ));
            }
            let byte: Self = Self::from_byte(byte_buf[0]);
            // Set sign bit to zero and shift to right place.
            result |= (byte & !Self::VARINT_MASK) << offset;
            // First bit is not set, stop
            if byte & Self::VARINT_MASK == Self::ZERO {
                break;
            }
            offset += Self::VARINT_SHIFT;
            i += 1;
        }

        Ok((result, i + 1))
    }

    /// Decode using Zigzag algorithm.
    ///
    /// Description can be found [here](https://protobuf.dev/programming-guides/encoding/#signed-ints)
    fn zigzag_decode(&self) -> Self::SignedCounterpart;
}

impl Integer<1, 2> for u8 {
    const ZERO: Self = 0;

    fn as_varint(&self) -> ([u8; 2], usize) {
        UnsignedInteger::varint_encode(*self)
    }

    fn varint_decode(encoded_stream: &mut dyn std::io::Read) -> std::io::Result<(Self, usize)> {
        UnsignedInteger::varint_decode(encoded_stream)
    }

    #[inline]
    fn overflow_add_i8(&self, other: i8) -> Self {
        (*self as i8).overflowing_add(other).0 as u8
    }
}

impl UnsignedInteger<1, 2> for u8 {
    type SignedCounterpart = i8;
    const VARINT_MASK: Self = !0x7f;
    const VARINT_SHIFT: Self = 7;

    #[inline]
    fn zigzag_decode(&self) -> i8 {
        (*self >> 1) as i8 ^ ((*self & 1) as i8).neg()
    }
}

impl Integer<2, 3> for u16 {
    const ZERO: Self = 0;

    fn as_varint(&self) -> ([u8; 3], usize) {
        UnsignedInteger::varint_encode(*self)
    }

    fn varint_decode(encoded_stream: &mut dyn std::io::Read) -> std::io::Result<(Self, usize)> {
        UnsignedInteger::varint_decode(encoded_stream)
    }

    #[inline]
    fn overflow_add_i8(&self, other: i8) -> Self {
        (*self as i16).overflowing_add(other as i16).0 as u16
    }
}

impl UnsignedInteger<2, 3> for u16 {
    type SignedCounterpart = i16;
    const VARINT_MASK: Self = !0x7f;
    const VARINT_SHIFT: Self = 7;

    #[inline]
    fn zigzag_decode(&self) -> i16 {
        (*self >> 1) as i16 ^ ((*self & 1) as i16).neg()
    }
}

impl Integer<4, 5> for u32 {
    const ZERO: Self = 0;

    fn as_varint(&self) -> ([u8; 5], usize) {
        UnsignedInteger::varint_encode(*self)
    }

    fn varint_decode(encoded_stream: &mut dyn std::io::Read) -> std::io::Result<(Self, usize)> {
        UnsignedInteger::varint_decode(encoded_stream)
    }

    #[inline]
    fn overflow_add_i8(&self, other: i8) -> Self {
        (*self as i32).overflowing_add(other as i32).0 as u32
    }
}

impl UnsignedInteger<4, 5> for u32 {
    type SignedCounterpart = i32;
    const VARINT_MASK: Self = !0x7f;
    const VARINT_SHIFT: Self = 7;

    #[inline]
    fn zigzag_decode(&self) -> i32 {
        (*self >> 1) as i32 ^ ((*self & 1) as i32).neg()
    }
}

impl Integer<8, 10> for u64 {
    const ZERO: Self = 0;

    fn as_varint(&self) -> ([u8; 10], usize) {
        UnsignedInteger::varint_encode(*self)
    }

    fn varint_decode(encoded_stream: &mut dyn std::io::Read) -> std::io::Result<(Self, usize)> {
        UnsignedInteger::varint_decode(encoded_stream)
    }

    #[inline]
    fn overflow_add_i8(&self, other: i8) -> Self {
        (*self as i64).overflowing_add(other as i64).0 as u64
    }
}

impl UnsignedInteger<16, 19> for u128 {
    type SignedCounterpart = i128;
    const VARINT_MASK: Self = !0x7f;
    const VARINT_SHIFT: Self = 7;

    #[inline]
    fn zigzag_decode(&self) -> i128 {
        (*self >> 1) as i128 ^ ((*self & 1) as i128).neg()
    }
}

impl Integer<16, 19> for u128 {
    const ZERO: Self = 0;

    fn as_varint(&self) -> ([u8; 19], usize) {
        UnsignedInteger::varint_encode(*self)
    }

    fn varint_decode(encoded_stream: &mut dyn std::io::Read) -> std::io::Result<(Self, usize)> {
        UnsignedInteger::varint_decode(encoded_stream)
    }

    #[inline]
    fn overflow_add_i8(&self, other: i8) -> Self {
        (*self as i128).overflowing_add(other as i128).0 as u128
    }
}

impl UnsignedInteger<8, 10> for u64 {
    type SignedCounterpart = i64;
    const VARINT_MASK: Self = !0x7f;
    const VARINT_SHIFT: Self = 7;

    #[inline]
    fn zigzag_decode(&self) -> i64 {
        (*self >> 1) as i64 ^ ((*self & 1) as i64).neg()
    }
}

pub(crate) trait ByteRepr<const N: usize> {
    fn to_le_bytes(&self) -> [u8; N];
    fn truncate_to_u8(&self) -> u8;
    fn from_byte(value: u8) -> Self;
}

impl ByteRepr<1> for i8 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; 1] {
        i8::to_le_bytes(*self)
    }

    fn truncate_to_u8(&self) -> u8 {
        *self as u8
    }

    fn from_byte(value: u8) -> Self {
        value as i8
    }
}

impl ByteRepr<2> for i16 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; 2] {
        i16::to_le_bytes(*self)
    }

    fn truncate_to_u8(&self) -> u8 {
        *self as u8
    }

    fn from_byte(value: u8) -> Self {
        value as i8 as i16
    }
}

impl ByteRepr<4> for i32 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; 4] {
        i32::to_le_bytes(*self)
    }

    fn truncate_to_u8(&self) -> u8 {
        *self as u8
    }

    fn from_byte(value: u8) -> Self {
        value as i8 as i32
    }
}

impl ByteRepr<8> for i64 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; 8] {
        i64::to_le_bytes(*self)
    }

    fn truncate_to_u8(&self) -> u8 {
        *self as u8
    }

    fn from_byte(value: u8) -> Self {
        value as i8 as i64
    }
}

impl ByteRepr<1> for u8 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; 1] {
        u8::to_le_bytes(*self)
    }

    fn truncate_to_u8(&self) -> u8 {
        *self
    }

    fn from_byte(value: u8) -> Self {
        value
    }
}

impl ByteRepr<2> for u16 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; 2] {
        u16::to_le_bytes(*self)
    }

    fn truncate_to_u8(&self) -> u8 {
        *self as u8
    }

    fn from_byte(value: u8) -> Self {
        value as u16
    }
}

impl ByteRepr<4> for u32 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; 4] {
        u32::to_le_bytes(*self)
    }

    fn truncate_to_u8(&self) -> u8 {
        *self as u8
    }

    fn from_byte(value: u8) -> Self {
        value as u32
    }
}

impl ByteRepr<8> for u64 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; 8] {
        u64::to_le_bytes(*self)
    }

    fn truncate_to_u8(&self) -> u8 {
        *self as u8
    }

    fn from_byte(value: u8) -> Self {
        value as u64
    }
}

impl ByteRepr<16> for u128 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; 16] {
        u128::to_le_bytes(*self)
    }

    fn truncate_to_u8(&self) -> u8 {
        *self as u8
    }

    fn from_byte(value: u8) -> Self {
        value as u128
    }
}

#[cfg(test)]
mod tests {
    // TODO: varint coding tests
}
