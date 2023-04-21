use std::ops::{Add, AddAssign, BitAnd, BitOr, BitOrAssign, BitXor, Neg, Shl, Shr};

pub mod rle;

/// Marker trait for signed integer types.
pub(crate) trait SignedInt<const N: usize, Output = Self>:
    Copy
    + arrow::datatypes::ArrowNativeType
    + From<i8>
    + ToBytes<N>
    + Add<Output = Output>
    + AddAssign
    + Neg<Output = Output>
    + Shl<Output = Output>
    + Shr<Output = Output>
    + BitAnd<Output = Output>
    + BitOr<Output = Output>
    + BitOrAssign
    + BitXor<Output = Output>
    + Neg<Output = Output>
{
    type Output;
}

pub(crate) trait ToBytes<const N: usize> {
    fn to_le_bytes(&self) -> [u8; N];
}

impl<const N: usize> SignedInt<N> for i8 {
    type Output = i8;
}

impl<const N: usize> SignedInt<N> for i16 {
    type Output = i16;
}

impl<const N: usize> SignedInt<N> for i32 {
    type Output = i32;
}

impl<const N: usize> SignedInt<N> for i64 {
    type Output = i64;
}

impl<const N: usize> ToBytes<N> for i8 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; N] {
        self.to_le_bytes()
    }
}

impl<const N: usize> ToBytes<N> for i16 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; N] {
        self.to_le_bytes()
    }
}

impl<const N: usize> ToBytes<N> for i32 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; N] {
        self.to_le_bytes()
    }
}

impl<const N: usize> ToBytes<N> for i64 {
    #[inline]
    fn to_le_bytes(&self) -> [u8; N] {
        self.to_le_bytes()
    }
}
