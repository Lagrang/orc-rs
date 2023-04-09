use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

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
            return Err(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "File start position({start_pos}) is greater than end position({end_pos})",
                    ),
                )
                .into(),
            );
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

    fn current_pos(&self) -> std::io::Result<u64> {
        self.file.stream_position()
    }

    fn seek(&mut self, pos: u64) -> std::io::Result<u64> {
        self.file.seek(SeekFrom::Start(self.start_pos + pos))
    }

    fn read_to_slice(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}
