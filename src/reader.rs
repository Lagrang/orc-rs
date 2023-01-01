use std::io;
use std::{
    format,
    io::{Read, Result, Seek},
};

use crate::io_utils::PositionalReader;
use crate::proto;
use bytes::Bytes;
use prost::Message;

pub struct ReaderOptions {
    pub file_end_pos: Option<u64>,
}
pub struct FileReader {}

pub trait Reader {}

struct TailReader<'a, T: Read + Seek> {
    file_reader: &'a PositionalReader<'a, T>,
    read_buffer: Bytes,
    file_end_pos: u64,
}

impl<'a, T: Read + Seek> TailReader<'a, T> {
    fn new(file_reader: &PositionalReader<'a, T>, file_end_pos: u64) -> Result<Self> {
        // Try to read 16K from ORC file tail to get footer and postscript in one call.
        let tail_buf_cap: u64 = 16 * 1024;
        let mut buffer = Vec::with_capacity(tail_buf_cap as usize);
        unsafe {
            buffer.set_len(buffer.capacity());
        };

        let tail_start_pos = if file_end_pos > tail_buf_cap {
            file_end_pos - tail_buf_cap
        } else {
            file_end_pos
        };
        let bytes_read = file_reader.read_at(tail_start_pos, &mut buffer)?;
        if bytes_read < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("ORC file is too short, only {bytes_read} bytes"),
            ));
        }

        Ok(TailReader {
            file_reader,
            read_buffer: Bytes::from(buffer).slice(..bytes_read),
            file_end_pos,
        })
    }

    fn read(&self) -> Result<()> {
        let (postscript, postscript_len) = self.read_postscript()?;
        let tail_size = postscript.footer_length() + postscript_len + 1;
        let file_size = self.file_end_pos + 1;
        if tail_size >= file_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid ORC file: tail size({} byte(s)) is greater than file size({} byte(s))",
                    file_size, tail_size
                ),
            ));
        }

        let footer = self.read_footer(&postscript, postscript_len)?;

        Ok(())
    }

    fn read_footer(
        &mut self,
        postscript: &proto::PostScript,
        postscript_len: u64,
    ) -> Result<proto::Footer> {
        let footer_len = postscript.footer_length() as usize;
        let footer_buffer = if footer_len <= self.read_buffer.len() {
            // footer already read into a buffer
            self.read_buffer
                .slice(self.read_buffer.len() - footer_len..)
        } else {
            let new_buf = Vec::with_capacity(footer_len);
            unsafe {
                new_buf.set_len(new_buf.capacity());
            }
            let file_size = self.file_end_pos + 1;
            let tail_size = postscript.footer_length() + postscript_len + 1;
            let footer_pos = file_size - tail_size;
            let footer_actual_len = self.file_reader.read_at(footer_pos, &mut new_buf)?;
            if footer_actual_len != footer_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "Invalid file footer size: expected length={}, actual footer size={}, postscript size={}, file size={}.",
                        footer_len, footer_actual_len, postscript_len, file_size
                    ),
                ));
            }
            Bytes::from(new_buf)
        };

        // all data read from existing buffer, replace it with empty one
        self.read_buffer = Bytes::new();
        proto::Footer::decode(footer_buffer).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Footer protobuf damaged: '{}'", err.to_string()),
            )
        })
    }

    fn read_postscript(&mut self) -> Result<(proto::PostScript, u64)> {
        let postscript_len = self.read_buffer[self.read_buffer.len() - 1] as usize;
        let postscript_start_pos = self.read_buffer.len() - postscript_len - 1;
        let postscript_body = &self.read_buffer[postscript_start_pos..];

        let postscript = proto::PostScript::decode(postscript_body).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Postscript protobuf damaged: '{}'", err.to_string()),
            )
        })?;
        if postscript.magic() != "ORC" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Postscript has magic invalid magic value: '{}'. Expected 'ORC'",
                    postscript.magic()
                ),
            ));
        }

        self.read_buffer = self.read_buffer.slice(..postscript_start_pos);
        Ok((postscript, postscript_len as u64))
    }
}

pub fn new_reader<T>(orc_file: &T, opts: &ReaderOptions) -> Result<impl Reader>
where
    T: Read + Seek,
{
    let reader = PositionalReader::new(orc_file)?;

    let stream_len = reader.len();
    let end_pos = opts.file_end_pos.unwrap_or(stream_len - 1);
    if end_pos >= stream_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "ORC file stream length is {}, but end position(from read options) is {}",
                stream_len, end_pos
            ),
        ));
    }

    Ok()
}
