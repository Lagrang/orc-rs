use std::ops::{
    Add, AddAssign, BitAnd, BitOr, BitOrAssign, BitXor, Neg, Not, Shl, ShlAssign, Shr, ShrAssign,
};

pub mod rle;

/// Marker trait for signed integer types.
pub(crate) trait Integer<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize>:
    Copy
    + arrow::datatypes::ArrowNativeType
    + ByteRepr<TYPE_SIZE>
    + Add<Output = Self>
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
    fn varint_decode(buffer: &[u8]) -> (Self, usize);
}

impl Integer<1, 2> for i8 {
    const ZERO: Self = 0;

    #[inline]
    fn as_varint(&self) -> ([u8; 2], usize) {
        varint_encode(self.zigzag_encode())
    }

    #[inline]
    fn varint_decode(buffer: &[u8]) -> (Self, usize) {
        let (decoded, size): (u8, usize) = UnsignedInteger::varint_decode(buffer);
        (decoded.zigzag_decode(), size)
    }
}

impl Integer<2, 3> for i16 {
    const ZERO: Self = 0;

    #[inline]
    fn as_varint(&self) -> ([u8; 3], usize) {
        varint_encode(self.zigzag_encode())
    }

    #[inline]
    fn varint_decode(buffer: &[u8]) -> (Self, usize) {
        let (decoded, size): (u16, usize) = UnsignedInteger::varint_decode(buffer);
        (decoded.zigzag_decode(), size)
    }
}

impl Integer<4, 5> for i32 {
    const ZERO: Self = 0;

    #[inline]
    fn as_varint(&self) -> ([u8; 5], usize) {
        varint_encode(self.zigzag_encode())
    }

    #[inline]
    fn varint_decode(buffer: &[u8]) -> (Self, usize) {
        let (decoded, size): (u32, usize) = UnsignedInteger::varint_decode(buffer);
        (decoded.zigzag_decode(), size)
    }
}

impl Integer<8, 10> for i64 {
    const ZERO: Self = 0;

    #[inline]
    fn as_varint(&self) -> ([u8; 10], usize) {
        varint_encode(self.zigzag_encode())
    }

    #[inline]
    fn varint_decode(buffer: &[u8]) -> (Self, usize) {
        let (decoded, size): (u64, usize) = UnsignedInteger::varint_decode(buffer);
        (decoded.zigzag_decode(), size)
    }
}

pub(crate) trait SignedInteger<const TYPE_SIZE: usize, const MAX_ENCODED_SIZE: usize>:
    Integer<TYPE_SIZE, MAX_ENCODED_SIZE> + From<i8> + Neg<Output = Self>
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
    Integer<TYPE_SIZE, MAX_ENCODED_SIZE> + From<u8>
{
    /// Type represent unsigned counterpart of this type, e.g. u8 => i8.
    type SignedCounterpart;

    /// Decode 'base 128 varint' value.
    /// Description can be found [here](https://protobuf.dev/programming-guides/encoding/#varints).
    /// Expected order of varint bytes is little endian.
    ///
    /// Examples:
    /// - 128 => [0x80, 0x01]
    /// - 16383 => [0xff, 0x7f]
    fn varint_decode(buffer: &[u8]) -> (Self, usize) {
        // let first: Self = From::from(buffer[0]);
        // First bit is not set, stop
        // if first & From::from(0x80) == Self::ZERO {
        //     return (first, 1);
        // }

        let unsigned_mask: Self = From::from(0x7f);
        let signed_mask: Self = From::from(0x80);
        let shift: Self = From::from(7);

        let mut result = Self::ZERO;
        let mut i = 0;
        let mut offset: Self = Self::ZERO;
        loop {
            let byte: Self = From::from(buffer[i]);
            result |= (byte & unsigned_mask) << offset;
            // First bit is not set, stop
            if byte & signed_mask == Self::ZERO {
                break;
            }
            offset += shift;
            i += 1;
        }

        (result, i + 1)
    }

    /// Decode using Zigzag algorithm.
    ///
    /// Description can be found [here](https://protobuf.dev/programming-guides/encoding/#signed-ints)
    fn zigzag_decode(&self) -> Self::SignedCounterpart;
}

impl Integer<1, 2> for u8 {
    const ZERO: Self = 0;

    fn as_varint(&self) -> ([u8; 2], usize) {
        varint_encode(*self)
    }

    fn varint_decode(buffer: &[u8]) -> (Self, usize) {
        UnsignedInteger::varint_decode(buffer)
    }
}

impl UnsignedInteger<1, 2> for u8 {
    type SignedCounterpart = i8;

    #[inline]
    fn zigzag_decode(&self) -> i8 {
        (*self >> 1) as i8 ^ ((*self & 1) as i8).neg()
    }
}

impl Integer<2, 3> for u16 {
    const ZERO: Self = 0;

    fn as_varint(&self) -> ([u8; 3], usize) {
        varint_encode(*self)
    }

    fn varint_decode(buffer: &[u8]) -> (Self, usize) {
        UnsignedInteger::varint_decode(buffer)
    }
}

impl UnsignedInteger<2, 3> for u16 {
    type SignedCounterpart = i16;

    #[inline]
    fn zigzag_decode(&self) -> i16 {
        (*self >> 1) as i16 ^ ((*self & 1) as i16).neg()
    }
}

impl Integer<4, 5> for u32 {
    const ZERO: Self = 0;

    fn as_varint(&self) -> ([u8; 5], usize) {
        varint_encode(*self)
    }

    fn varint_decode(buffer: &[u8]) -> (Self, usize) {
        UnsignedInteger::varint_decode(buffer)
    }
}

impl UnsignedInteger<4, 5> for u32 {
    type SignedCounterpart = i32;

    #[inline]
    fn zigzag_decode(&self) -> i32 {
        (*self >> 1) as i32 ^ ((*self & 1) as i32).neg()
    }
}

impl Integer<8, 10> for u64 {
    const ZERO: Self = 0;

    fn as_varint(&self) -> ([u8; 10], usize) {
        varint_encode(*self)
    }

    fn varint_decode(buffer: &[u8]) -> (Self, usize) {
        UnsignedInteger::varint_decode(buffer)
    }
}

impl UnsignedInteger<8, 10> for u64 {
    type SignedCounterpart = i64;

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
        value as i16
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
        value as i32
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
        value as i64
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

fn varint_encode<
    const N: usize,
    const MAX_ENCODED_SIZE: usize,
    T: UnsignedInteger<N, MAX_ENCODED_SIZE>,
>(
    value: T,
) -> ([u8; MAX_ENCODED_SIZE], usize) {
    let shift: T = From::from(7u8);
    let mut result: [u8; MAX_ENCODED_SIZE] = [0; MAX_ENCODED_SIZE];
    let mut i = 0;
    let mut v = value;
    // Scan all bytes of the value starting from least significant(LS):
    // If sign bit of LS byte is not set, encoded varint value completed.
    // Otherwise, write 7 bits of LS byte to the output and append
    // continuation marker(one bit set to '1') and proceed to the next 7 bit.
    loop {
        result[i] = v.truncate_to_u8();
        if result[i] & 0x80 == 0 {
            break;
        }
        v >>= shift;
        i += 1;
    }
    (result, i + 1)
}
