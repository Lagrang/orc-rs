use std::io::{Result, SeekFrom, *};

pub struct PositionalReader<'a, READER: Read + Seek> {
    reader: &'a READER,
    start_pos: u64,
    end_pos: u64,
}

impl<'a, READER: Read + Seek> PositionalReader<'a, READER> {
    pub fn new(reader: &READER) -> Result<Self> {
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

    pub fn read_at(&self, pos: u64, buffer: &mut [u8]) -> Result<usize> {
        self.reader.seek(SeekFrom::Start(self.start_pos + pos))?;

        let mut byte_read = 0;
        loop {
            match self.reader.read(&mut buffer[byte_read..]) {
                Ok(read) => {
                    byte_read += read;
                    if byte_read >= buffer.len() {
                        return Ok(byte_read);
                    }
                }
                res @ Err(e) => {
                    if e.kind() != ErrorKind::Interrupted {
                        return res;
                    }
                }
            }
        }
    }
}
