use std::cmp;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use bytes::{Buf, Bytes};

use crate::io_utils::PositionalReader;

/// ORC file source abstraction.
/// Each source represent ORC file located on some 'source', i.e. file, memory buffer, etc.
pub trait OrcSource {
    /// Creates a new reader for ORC file.
    fn reader(&self) -> std::io::Result<Box<dyn PositionalReader>>;
}

pub struct FileSource {
    file: std::fs::File,
    end_pos: Option<u64>,
}

/// ORC file represented as file on some filesystem.
impl FileSource {
    pub fn new(path: &std::path::Path) -> crate::Result<Box<Self>> {
        Ok(Box::new(Self {
            file: std::fs::File::open(path)?,
            end_pos: None,
        }))
    }

    pub fn with_end_pos(file: File, file_end_pos: u64) -> Box<Self> {
        Box::new(Self {
            file,
            end_pos: Some(file_end_pos),
        })
    }
}

impl From<File> for FileSource {
    fn from(file: File) -> Self {
        Self {
            file,
            end_pos: None,
        }
    }
}

impl OrcSource for FileSource {
    fn reader(&self) -> std::io::Result<Box<dyn PositionalReader>> {
        Ok(Box::new(FileReader::new(
            self.file.try_clone()?,
            self.end_pos,
        )?))
    }
}

struct FileReader {
    file: std::fs::File,
    start_pos: u64,
    end_pos: u64,
}

impl FileReader {
    pub fn new(mut file: std::fs::File, end_pos: Option<u64>) -> std::io::Result<Self> {
        let start_pos = file.stream_position()?;
        let end_pos = end_pos.map_or_else(|| file.seek(SeekFrom::End(0)), Ok)?;
        if end_pos <= start_pos {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("File start position({start_pos}) is greater than end position({end_pos})",),
            ));
        }

        Ok(Self {
            file,
            start_pos,
            end_pos,
        })
    }
}

impl PositionalReader for FileReader {
    fn start_pos(&self) -> u64 {
        self.start_pos
    }

    fn end_pos(&self) -> u64 {
        self.end_pos
    }

    fn seek(&mut self, pos: u64) -> std::io::Result<u64> {
        self.file.seek(SeekFrom::Start(self.start_pos + pos))
    }
}

impl Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

pub struct MemoryReader {
    buffer: Bytes,
    current_pos: usize,
}

impl MemoryReader {
    pub fn from(buf: bytes::Bytes) -> Self {
        Self {
            buffer: buf,
            current_pos: 0,
        }
    }

    pub fn from_mut(buf: bytes::BytesMut) -> Self {
        Self {
            buffer: buf.freeze(),
            current_pos: 0,
        }
    }
}

impl PositionalReader for MemoryReader {
    #[inline]
    fn start_pos(&self) -> u64 {
        0
    }

    #[inline]
    fn end_pos(&self) -> u64 {
        (self.buffer.len() - 1) as u64
    }

    fn seek(&mut self, pos: u64) -> std::io::Result<u64> {
        let range = self.start_pos()..=self.end_pos();
        if !range.contains(&pos) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Position {} is out of allowed range: {:?}", pos, range),
            ));
        }
        self.current_pos = pos as usize;
        Ok(pos)
    }
}

impl Read for MemoryReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.buffer.len() - self.current_pos;
        let bytes_to_read = cmp::min(buf.len(), remaining);
        if bytes_to_read == 0 || buf.is_empty() {
            return Ok(0);
        }

        buf[..bytes_to_read].copy_from_slice(
            &self.buffer.chunk()[self.current_pos..self.current_pos + bytes_to_read],
        );
        self.current_pos += bytes_to_read;
        Ok(bytes_to_read)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};

    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use googletest::matchers::{anything, eq, err};
    use googletest::verify_that;

    use crate::io_utils::PositionalReader;

    use super::{FileReader, MemoryReader};

    #[test]
    fn memory_source() -> googletest::Result<()> {
        let mut buffer = BytesMut::new();
        buffer.put_bytes(1, 5);
        buffer.put_bytes(2, 3);
        buffer.put_bytes(3, 4);

        validate(buffer.freeze(), |buffer| {
            let reader = MemoryReader::from(buffer);
            Ok(Box::new(reader))
        })
    }

    #[test]
    fn file_source() -> googletest::Result<()> {
        let mut buffer = BytesMut::new();
        buffer.put_bytes(1, 5);
        buffer.put_bytes(2, 3);
        buffer.put_bytes(3, 4);

        validate(buffer.freeze(), |buffer| {
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(buffer.as_ref())?;
            file.seek(SeekFrom::Start(0))?;
            Ok(Box::new(FileReader::new(
                file,
                Some((buffer.len() - 1) as u64),
            )?))
        })
    }

    fn validate<F>(expected_content: Bytes, reader_factory: F) -> googletest::Result<()>
    where
        F: Fn(Bytes) -> std::io::Result<Box<dyn PositionalReader>>,
    {
        let mut reader = reader_factory(expected_content.clone())?;
        let mut read_buffer = Vec::<u8>::with_capacity(expected_content.len());
        let bytes_read = reader.read_to_end(&mut read_buffer)?;
        verify_that!(bytes_read, eq(expected_content.len()))?;
        verify_that!(expected_content.chunk(), eq(read_buffer))?;
        verify_that!(reader.start_pos(), eq(0))?;
        verify_that!(reader.end_pos(), eq((expected_content.len() - 1) as u64))?;

        // Pass read buffer greater than actual data size
        let mut reader = reader_factory(expected_content.clone())?;
        let mut read_buffer = BytesMut::with_capacity(expected_content.len() * 2);
        let bytes_read = PositionalReader::read(reader.as_mut(), &mut read_buffer)?;
        verify_that!(bytes_read, eq(expected_content.len()))?;
        verify_that!(&read_buffer, eq(&expected_content))?;

        // Pass read buffer less than actual data size
        let mut reader = reader_factory(expected_content.clone())?;
        let mut read_buffer = BytesMut::with_capacity(expected_content.len() / 2);
        let mut bytes_read = PositionalReader::read(reader.as_mut(), &mut read_buffer)?;
        verify_that!(bytes_read, eq(read_buffer.len()))?;
        read_buffer.reserve(expected_content.len() - read_buffer.len());
        bytes_read += PositionalReader::read(reader.as_mut(), &mut read_buffer)?;
        verify_that!(bytes_read, eq(expected_content.len()))?;
        verify_that!(&read_buffer, eq(&expected_content))?;
        read_buffer.reserve(1);
        verify_that!(
            PositionalReader::read(reader.as_mut(), &mut read_buffer)?,
            eq(0)
        )?;

        let mut reader = reader_factory(expected_content.clone())?;
        let mut read_buffer = BytesMut::with_capacity(expected_content.len());
        let bytes_read = PositionalReader::read_at(
            reader.as_mut(),
            (expected_content.len() - 1) as u64,
            &mut read_buffer,
        )?;
        verify_that!(bytes_read, eq(1))?;
        verify_that!(
            &read_buffer,
            eq(&expected_content[expected_content.len() - 1..])
        )?;

        let mut reader = reader_factory(expected_content.clone())?;
        verify_that!(
            PositionalReader::read_exact_at(reader.as_mut(), 0, expected_content.len() + 1, ""),
            err(anything())
        )?;

        Ok(())
    }
}
