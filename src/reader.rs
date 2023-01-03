use std::io;
use std::ops::Deref;
use std::{
    format,
    io::{Read, Result, Seek},
};

use crate::compression::CompressionFactory;
use crate::io_utils::PositionalReader;
use crate::proto;
use bytes::{Buf, Bytes};
use prost::Message;

pub struct ReaderOptions {
    pub file_end_pos: Option<u64>,
    pub compression_opts: CompressionFactory,
    //TODO: pub allocator: &'a dyn std::alloc::Allocator,
}
pub struct FileReader {}

pub trait Reader {}

struct FileVersion(u32, u32);

struct TailReader<'a, T: Read + Seek> {
    file_reader: &'a PositionalReader<'a, T>,
    opts: &'a ReaderOptions,
    read_buffer: Bytes,
    file_end_pos: u64,
}

impl<'a, T: Read + Seek> TailReader<'a, T> {
    fn new(
        file_reader: &PositionalReader<'a, T>,
        file_end_pos: u64,
        opts: &ReaderOptions,
    ) -> Result<Self> {
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
            opts,
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

        let mut version = FileVersion(0, 0);
        if postscript.version.len() == 2 {
            version.0 = postscript.version[0];
            version.1 = postscript.version[1];
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
        // decompress the footer
        let footer_reader = self
            .opts
            .compression_opts
            .decoder(footer_buffer.reader(), postscript.compression())?;
        let decompressed_footer_buf = Vec::with_capacity(footer_buffer.len());
        footer_reader
            .read_to_end(&mut decompressed_footer_buf)
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("ORC footer decompression failed: {}", err.to_string()),
                )
            })?;
        let footer = proto::Footer::decode(decompressed_footer_buf.deref()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Footer protobuf damaged: '{}'", err.to_string()),
            )
        })?;

        // validate the schema
        if footer.types.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("ORC footer misses the schema(types vector is empty)",),
            ));
        }
        // Schema tree should be numbered in increasing order, level by level.
        // Increase of type ID happens on each new type(going deeper when nested type is found).
        //
        // For instance, we have a schema:
        //               Struct(0)
        //         /         |      \
        //  Int(1)      Struct(2)  Float(5)
        //               |      \
        //          Int(3)      String(4)
        //
        // Types should be encoded in this way:
        // Type_ID: 0
        //      SubTypes: 1,2,5
        // Type_ID: 1
        // Type_ID: 2
        //      SubTypes: 3,4
        // Type_ID: 3
        // Type_ID: 4
        // Type_ID: 5
        let max_type_id = footer.types.len();
        for type_id in 0..max_type_id {
            let field_type = footer.types[type_id];
            if field_type.kind() == proto::r#type::Kind::Struct
                && field_type.field_names.len() != field_type.subtypes.len()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Footer schema is corrupted: has {} field names and {} subtypes.",
                        field_type.field_names.len(),
                        field_type.subtypes.len(),
                    ),
                ));
            }

            for subtype_idx in 0..field_type.subtypes.len() {
                let subtype_id = field_type.subtypes[subtype_idx];
                if subtype_id <= type_id as u32 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                    "Subtype has ID >= than its holder type: subtype_id={}, outer_type_id={}",
                    subtype_id,
                    type_id,
                ),
                    ));
                }
                if subtype_id >= max_type_id as u32 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Invalid subtype ID={}(should be less than max ID={})",
                            subtype_id, max_type_id,
                        ),
                    ));
                }
                if subtype_idx > 0 && subtype_id < field_type.subtypes[subtype_idx - 1] {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Invalid schema type order: types should be numbered in increasing order. Outer type ID={}, subtype_id={}, previous subtype_id={}",
                            type_id,
                            subtype_id,
                            field_type.subtypes[subtype_idx - 1],
                        ),
                    ));
                }
            }
        }

        Ok(footer)
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

    Ok(reader)
}
