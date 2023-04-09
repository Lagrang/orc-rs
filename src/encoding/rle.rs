use std::cmp;
use std::sync::Arc;

use bytes::{Buf, BytesMut};

use crate::io_utils::PositionalReader;

pub trait RleDecoder {
    fn read(&mut self, batch_size: usize) -> crate::Result<arrow::array::ArrayRef>;
}

/// For byte streams, ORC uses a very light weight encoding of identical values.
///     - Run: a sequence of at least 3 identical values
///     - Literals: a sequence of non-identical values
/// The first byte of each group of values is a header that determines whether it is a run (value between 0 to 127)
/// or literal list (value between -128 to -1). For runs, the control byte is the length of the run minus the length
/// of the minimal run (3) and the control byte for literal lists is the negative length of the list.
/// For example, a hundred 0’s is encoded as [0x61, 0x00] and the sequence 0x44, 0x45 would be encoded as [0xfe, 0x44, 0x45].
pub struct ByteRleDecoder {
    file_reader: Box<dyn PositionalReader>,
    // Block of data read from file but not processed yet
    buffer: BytesMut,
    // State of current run
    current_run: RunState,
}

struct RunState {
    // Index of last element in 'plain' sequence of values
    length: usize,
    // Index of next value in 'plain' sequence to return
    next_element: usize,
    /// Indicates that current block is not RLE encoded, but contains plain values.
    is_plain: bool,
}

impl RunState {
    fn empty() -> Self {
        Self {
            length: 0,
            next_element: 0,
            is_plain: false,
        }
    }

    fn from_header(header: i8) -> Self {
        let len = if header > 0 {
            //Run length at least 3 values and this min.length is not coded in 'length' field of header.
            (header + 3) as usize
        } else {
            header.abs() as usize
        };
        // Negative length means that next run doesn't contains equal values.
        Self {
            length: len,
            next_element: 0,
            is_plain: header < 0,
        }
    }

    fn remaining(&self) -> usize {
        self.length - self.next_element
    }

    fn next(&mut self) -> bool {
        self.next_element += 1;
        self.next_element <= self.length
    }

    #[inline]
    fn has_values(&self) -> bool {
        self.next_element < self.length
    }
}

impl RleDecoder for ByteRleDecoder {
    fn read(&mut self, batch_size: usize) -> crate::Result<arrow::array::ArrayRef> {
        let builder = arrow::array::UInt8Builder::with_capacity(batch_size);
        let mut remaining_values = batch_size;
        while remaining_values > 0 {
            if !self.current_run.next() && !self.read_next_block()? {
                // No more data to decode
                break;
            }

            if self.buffer.remaining() > 0 {
                let count = cmp::min(self.current_run.remaining(), self.buffer.remaining());
                builder.append_slice(&self.buffer[..count]);
                self.buffer.advance(count);
                remaining_values -= count;
            } else {
                // RLE run is not completed, but no buffered data remains => read next chunk
                let res = self.read_next_block()?;
                debug_assert!(res);
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

impl ByteRleDecoder {
    fn new(file_reader: Box<dyn PositionalReader>, buffer_size: usize) -> Self {
        let cap = cmp::max(
            buffer_size,
            4 * 1024, /* this value must be >=128 to get RLE run in one read operation */
        );
        Self {
            file_reader,
            buffer: BytesMut::with_capacity(cap),
            current_run: RunState::empty(),
        }
    }

    fn read_next_block(&mut self) -> crate::Result<bool> {
        if self.current_run.has_values() {
            // Current run has more values, but current buffer exhausted. Read more data from data stream.
            if self.buffer.is_empty() {
                let bytes_read = self.file_reader.read(&mut self.buffer)?;
                if bytes_read == 0 {
                    return Err(crate::OrcError::MalformedRleBlock);
                }
            }
        } else {
            // Current run is completed, read header of next RLE run.
            if self.buffer.is_empty() {
                let bytes_read = self.file_reader.read(&mut self.buffer)?;
                if bytes_read == 0 {
                    return Ok(false);
                }
            }

            // First byte of block contains the RLE header.
            let header = i8::from_le_bytes([self.buffer[0]; 1]);
            self.buffer.advance(1);
            self.current_run = RunState::from_header(header);
        }
        Ok(true)
    }
}
