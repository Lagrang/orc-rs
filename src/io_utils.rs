use std::io::{Result, *};
use std::ops::{Deref, DerefMut};

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub struct UninitBytesMut {
    buffer: BytesMut,
}

impl UninitBytesMut {
    /// Creates mutable byte buffer with specified capacity.
    pub fn new(capacity: usize) -> Self {
        UninitBytesMut {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    pub fn freeze(self) -> Bytes {
        self.buffer.freeze()
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

    pub fn split_to(&mut self, at: usize) -> BytesMut {
        self.buffer.split_to(at)
    }

    pub fn write_from<F, E>(&mut self, read_fn: F) -> Result<usize>
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

impl DerefMut for UninitBytesMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            std::slice::from_raw_parts_mut(self.chunk_mut().as_mut_ptr(), self.chunk_mut().len())
        }
    }
}

impl Deref for UninitBytesMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.chunk().as_ptr(), self.chunk().len()) }
    }
}

impl Buf for UninitBytesMut {
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

unsafe impl BufMut for UninitBytesMut {
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

pub trait PositionalReader {
    fn start_pos(&self) -> u64;
    fn end_pos(&self) -> u64;
    fn seek_from_start(&mut self, pos: u64) -> Result<u64>;
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    fn len(&self) -> u64 {
        self.end_pos() - self.start_pos()
    }

    fn read_at(&mut self, pos: u64, buffer: &mut dyn BufMut) -> Result<usize> {
        self.seek_from_start(pos)?;

        let mut byte_read = 0;
        loop {
            let slice = unsafe {
                std::slice::from_raw_parts_mut(
                    buffer.chunk_mut().as_mut_ptr(),
                    buffer.chunk_mut().len(),
                )
            };
            match self.read(slice) {
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
