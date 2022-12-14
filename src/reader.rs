use std::{
    fmt, format,
    io::{Error, ErrorKind, Read, Result, Seek},
};

use crate::io_utils::PositionalReader;

pub struct ReaderOptions {
    pub file_end_pos: Option<u64>,
}
pub struct FileReader {}

pub trait Reader {}

pub fn new_reader<T>(orc_file: &T, opts: &ReaderOptions) -> Result<impl Reader>
where
    T: Read + Seek,
{
    let reader = PositionalReader::new(orc_file)?;

    let stream_len = reader.len();
    let end_pos = opts.file_end_pos.unwrap_or(stream_len - 1);
    if end_pos >= stream_len {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "ORC file stream length is {}, but end position(from read options) is {}",
                stream_len, end_pos
            ),
        ));
    }

    // Try to read 16K from ORC file tail to get footer and postscript in one call.
    let tail_buf_cap: u64 = 16 * 1024;
    let mut buffer = Vec::with_capacity(tail_buf_cap as usize);
    buffer.fill(0);

    let tail_start_pos = if end_pos > tail_buf_cap {
        end_pos - tail_buf_cap
    } else {
        end_pos
    };
    let bytes_read = reader.read_at(tail_start_pos, &mut buffer)?;
    let tail_buffer = &mut buffer[..bytes_read];
    if bytes_read < 4 {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            format!("ORC file is too short, only {bytes_read} bytes"),
        ));
    }

    let postscript_len = tail_buffer[tail_buffer.len() - 1] as usize;
    tail_buffer = &mut tail_buffer[..tail_buffer.len() - 1];
    let postscript_body = &tail_buffer[tail_buffer.len() - postscript_len..];

    Ok()
}
