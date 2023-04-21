use std::borrow::{Borrow, BorrowMut};
use std::io::*;
use std::ops::{Deref, DerefMut, RangeBounds};

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub struct WriteBuffer {
    buffer: BytesMut,
}

impl WriteBuffer {
    /// Creates mutable byte buffer with specified capacity.
    pub fn new(capacity: usize) -> Self {
        WriteBuffer {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Reserve enough bytes in the buffer to accommodate write of `capacity` bytes.
    pub fn ensure_capacity(&mut self, capacity: usize) {
        // TODO: need optimization here
        let remaining_bytes = self.buffer.remaining();
        let new_cap = remaining_bytes + capacity;

        if self.buffer.capacity() >= new_cap {
            return;
        }

        let mut new_buf = BytesMut::with_capacity(new_cap);
        new_buf.extend_from_slice(&self.buffer);
        self.buffer = new_buf;
    }

    pub fn write_from<F, E>(&mut self, read_fn: F) -> std::io::Result<usize>
    where
        F: FnOnce(&mut [u8]) -> std::result::Result<usize, E>,
        E: std::error::Error,
        std::io::Error: std::convert::From<E>,
    {
        let bytes_read = read_fn(self)?;

        #[cfg(debug_assertions)]
        {
            let rem = self.remaining_mut();
            debug_assert!(
                rem >= bytes_read,
                "Buffer has capacity only for {bytes_read} bytes, but operation read {rem} bytes."
            );
        }

        unsafe {
            self.buffer.advance_mut(bytes_read);
        }
        Ok(bytes_read)
    }
}

impl From<BytesMut> for WriteBuffer {
    fn from(value: BytesMut) -> Self {
        Self { buffer: value }
    }
}

impl From<WriteBuffer> for BytesMut {
    fn from(value: WriteBuffer) -> Self {
        value.buffer
    }
}

impl Borrow<BytesMut> for WriteBuffer {
    fn borrow(&self) -> &BytesMut {
        &self.buffer
    }
}

impl BorrowMut<BytesMut> for WriteBuffer {
    fn borrow_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }
}

impl AsRef<BytesMut> for WriteBuffer {
    fn as_ref(&self) -> &BytesMut {
        &self.buffer
    }
}

impl AsMut<BytesMut> for WriteBuffer {
    fn as_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }
}

impl DerefMut for WriteBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            std::slice::from_raw_parts_mut(self.chunk_mut().as_mut_ptr(), self.chunk_mut().len())
        }
    }
}

impl Deref for WriteBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.chunk().as_ptr(), self.chunk().len()) }
    }
}

impl Buf for WriteBuffer {
    fn remaining(&self) -> usize {
        self.buffer.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.buffer.chunk()
    }

    fn advance(&mut self, cnt: usize) {
        self.buffer.advance(cnt)
    }
}

unsafe impl BufMut for WriteBuffer {
    fn remaining_mut(&self) -> usize {
        self.buffer.remaining_mut()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.buffer.advance_mut(cnt);
    }

    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        self.buffer.chunk_mut()
    }
}

pub trait SizedStream: Read {
    fn len(&self) -> u64;
}

impl<T: SizedStream + ?Sized> SizedStream for Box<T> {
    fn len(&self) -> u64 {
        self.deref().len()
    }
}

/// Trait which provide implementation for the reading into [`bytes::BufMut`] and stream length.
pub trait BufRead: Read {
    fn read(&mut self, buffer: &mut dyn BufMut) -> std::io::Result<usize> {
        let mut byte_read = 0;
        loop {
            let slice = unsafe {
                std::slice::from_raw_parts_mut(
                    buffer.chunk_mut().as_mut_ptr(),
                    buffer.chunk_mut().len(),
                )
            };
            match Read::read(self, slice) {
                Ok(read) => {
                    if read == 0 {
                        // end of file stream reached
                        return Ok(byte_read);
                    }

                    byte_read += read;
                    unsafe {
                        buffer.advance_mut(read);
                    }
                    if !buffer.has_remaining_mut() {
                        return Ok(byte_read);
                    }
                }
                Err(e) => {
                    if e.kind() != ErrorKind::Interrupted {
                        return Err(e);
                    }
                }
            }
        }
    }
}

impl<T: BufRead + ?Sized> BufRead for Box<T> {
    fn read(&mut self, buffer: &mut dyn BufMut) -> std::io::Result<usize> {
        BufRead::read(self.as_mut(), buffer)
    }
}

/// Trait which allows to seek to the some position in the stream.
pub trait SeekableRead: BufRead + SizedStream {
    /// Seeks to the particular position inside the stream.
    ///
    /// Position should be in range [0..[`BufRead::len()`]).
    fn seek(&mut self, pos: u64) -> std::io::Result<u64>;
}

impl<T: SeekableRead + ?Sized> SeekableRead for Box<T> {
    fn seek(&mut self, pos: u64) -> std::io::Result<u64> {
        self.as_mut().seek(pos)
    }
}

/// Trait which allows to perform read operations at some offset(similar to Posix pread).
pub trait PositionalRead: SeekableRead {
    fn read_at(&mut self, pos: u64, buffer: &mut dyn BufMut) -> std::io::Result<usize> {
        self.seek(pos)?;
        BufRead::read(self, buffer)
    }

    fn read_exact_at(
        &mut self,
        offset: u64,
        bytes_to_read: usize,
        err_msg_prefix: &str,
    ) -> std::io::Result<Bytes> {
        let mut buffer = WriteBuffer::new(bytes_to_read);
        let bytes_read = self.read_at(offset, &mut buffer)?;
        if bytes_read != bytes_to_read {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "{err_msg_prefix}: expected to read {bytes_read} bytes, but actually read {bytes_to_read} bytes",
                ),
            ));
        }
        let buffer: BytesMut = buffer.into();
        Ok(buffer.freeze())
    }
}

/// Reader which wraps a base reader and allows to read the data only in a passed byte range.
///
/// For instance, base reader can read bytes in range (0..1000), i.e. reader can read 1000 bytes at max.
/// Range reader will allow to narrow this range to some subset: (0..15), (40..=877), etc.
pub(crate) struct RangeRead<T> {
    reader: T,
    limit: u64,
    read_bytes: usize,
}

impl<T: SeekableRead> RangeRead<T> {
    pub fn new(mut base_reader: T, offset_range: impl RangeBounds<u64>) -> std::io::Result<Self> {
        let start_pos = match offset_range.start_bound() {
            std::ops::Bound::Unbounded => base_reader.borrow_mut().seek(0)?,
            std::ops::Bound::Excluded(pos) => base_reader.borrow_mut().seek(*pos + 1)?,
            std::ops::Bound::Included(pos) => base_reader.borrow_mut().seek(*pos)?,
        };
        let end_pos = match offset_range.end_bound() {
            std::ops::Bound::Unbounded => u64::MAX,
            std::ops::Bound::Excluded(pos) => pos - 1,
            std::ops::Bound::Included(pos) => *pos,
        };
        Ok(Self {
            reader: base_reader,
            limit: end_pos - start_pos + 1,
            read_bytes: 0,
        })
    }
}

impl<T: SeekableRead> Read for RangeRead<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.read_bytes >= self.limit as usize {
            return Ok(0);
        }

        let len = std::cmp::min(buf.len(), self.limit as usize - self.read_bytes);
        let read_buf = &mut buf[..len];
        let bytes_read = Read::read(self, read_buf)?;

        debug_assert!(bytes_read <= read_buf.len());
        self.read_bytes += bytes_read;

        Ok(bytes_read)
    }
}
