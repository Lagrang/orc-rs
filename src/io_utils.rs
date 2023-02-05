use std::io::{Result, SeekFrom, *};

use bytes::BufMut;

pub struct PositionalReader<'a, READER: Read + Seek> {
    reader: &'a mut READER,
    start_pos: u64,
    end_pos: u64,
}

impl<'a, READER: Read + Seek> PositionalReader<'a, READER> {
    pub fn new(reader: &'a mut READER) -> Result<Self> {
        let start_pos = reader.stream_position()?;
        let end_pos = reader.seek(SeekFrom::End(0))?;
        // TODO: do we need this seek to start?
        assert_eq!(reader.seek(std::io::SeekFrom::Start(start_pos))?, start_pos);
        Ok(Self {
            reader,
            start_pos,
            end_pos,
        })
    }

    #[inline(always)]
    pub fn len(&self) -> u64 {
        self.end_pos - self.start_pos
    }

    pub fn read_at(&mut self, pos: u64, buffer: &mut dyn BufMut) -> Result<usize> {
        self.reader.seek(SeekFrom::Start(self.start_pos + pos))?;

        let mut byte_read = 0;
        loop {
            let slice = unsafe {
                std::slice::from_raw_parts_mut(
                    buffer.chunk_mut().as_mut_ptr(),
                    buffer.chunk_mut().len(),
                )
            };
            match self.reader.read(slice) {
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
