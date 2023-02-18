use std::collections::HashMap;
use std::io;
use std::ops::Deref;
use std::{
    format,
    io::{Read, Result, Seek},
};

use crate::compression::{new_decompress_stream, CompressionRegistry};
use crate::io_utils::{PositionalReader, UninitBytesMut};
use crate::proto;
use bytes::{Buf, Bytes};
use prost::Message;

#[derive(Default)]
pub struct ReaderOptions {
    pub file_end_pos: Option<u64>,
    pub compression_opts: CompressionRegistry,
    //TODO: add custom allocator support, pub allocator: &'a dyn std::alloc::Allocator,
}

pub trait Reader {}

pub struct FileReader {
    tail: FileTail,
}

impl Reader for FileReader {}

#[derive(PartialEq, Eq, Debug)]
pub struct FileVersion(u32, u32);

struct TailReader<'a, T: Read + Seek> {
    file_reader: &'a mut PositionalReader<'a, T>,
    opts: ReaderOptions,
    read_buffer: Bytes,
    file_end_pos: u64,
}

struct FileTail {
    compression: CompressionRegistry,
    version: FileVersion,
    header_size: u64,
    content_size: u64,
    row_count: u64,
    metadata: HashMap<String, Bytes>,
}

impl<'a, T: Read + Seek> TailReader<'a, T> {
    fn new(
        file_reader: &'a mut PositionalReader<'a, T>,
        file_end_pos: u64,
        opts: ReaderOptions,
    ) -> Result<Self> {
        // Try to read 16K from ORC file tail to get footer and postscript in one call.
        let tail_buf_cap: u64 = 16 * 1024;

        let tail_start_pos = if file_end_pos > tail_buf_cap {
            file_end_pos - tail_buf_cap
        } else {
            // TODO: error here!!!
            file_end_pos
        };

        let mut buffer = UninitBytesMut::new(tail_buf_cap as usize);
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
            read_buffer: buffer.freeze(),
            file_end_pos,
        })
    }

    fn read(mut self) -> Result<FileTail> {
        let (postscript, postscript_len) = self.read_postscript()?;
        let tail_size = postscript.footer_length() + postscript_len + 1;
        let file_size = self.file_end_pos + 1;
        if tail_size >= file_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid ORC file: tail size({file_size} byte(s)) is greater than file size({tail_size} byte(s))",
                ),
            ));
        }

        let mut version = FileVersion(0, 0);
        if postscript.version.len() == 2 {
            version.0 = postscript.version[0];
            version.1 = postscript.version[1];
        }

        let footer = self.read_footer(&postscript, postscript_len)?;

        Ok(FileTail {
            compression: self.opts.compression_opts,
            version,
            header_size: footer.header_length(),
            content_size: footer.content_length(),
            row_count: footer.number_of_rows(),
            metadata: footer
                .metadata
                .iter()
                .map(|kv| (kv.name().to_owned(), kv.value().to_vec().into()))
                .collect(),
        })
    }

    fn read_footer(
        &mut self,
        postscript: &proto::PostScript,
        postscript_len: u64,
    ) -> Result<proto::Footer> {
        let declared_footer_len = postscript.footer_length() as usize;
        let footer_buffer = if declared_footer_len <= self.read_buffer.len() {
            // footer already read into a buffer
            self.read_buffer
                .slice(self.read_buffer.len() - declared_footer_len..)
        } else {
            let mut new_buf = UninitBytesMut::new(declared_footer_len);
            let file_size = self.file_end_pos + 1;
            let tail_size = postscript.footer_length() + postscript_len + 1;
            let footer_pos = file_size - tail_size;
            let actual_footer_len = self.file_reader.read_at(footer_pos, &mut new_buf)?;
            if actual_footer_len != declared_footer_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "Invalid file footer size: \
                        expected length={declared_footer_len}, \
                        actual footer size={actual_footer_len}, \
                        postscript size={postscript_len}, \
                        file size={file_size}.",
                    ),
                ));
            }
            new_buf.freeze()
        };

        // all data read from existing buffer, replace it with empty one
        self.read_buffer = Bytes::new();
        // decompress the footer
        let compression_codec = self.opts.compression_opts.codec(postscript.compression())?;
        let mut decompressed_footer = Vec::with_capacity(footer_buffer.len());
        let mut footer_reader = footer_buffer.reader();
        let mut footer_reader = new_decompress_stream(
            &mut footer_reader,
            compression_codec,
            postscript.compression_block_size(),
        );
        let footer_len = footer_reader
            .read_to_end(&mut decompressed_footer)
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("ORC footer decompression failed: {err}"),
                )
            })?;
        unsafe {
            decompressed_footer.set_len(footer_len);
        }
        let footer = proto::Footer::decode(decompressed_footer.deref()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Footer protobuf damaged: '{err}'"),
            )
        })?;

        // validate the schema
        if footer.types.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ORC footer misses the schema(types vector is empty)",
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
            let field_type = &footer.types[type_id];
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
                            "Subtype has ID >= than its holder type: subtype_id={subtype_id}, outer_type_id={type_id}",
                        ),
                    ));
                }
                if subtype_id >= max_type_id as u32 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Invalid subtype ID={subtype_id}(should be less than max ID={max_type_id})",
                        ),
                    ));
                }
                if subtype_idx > 0 && subtype_id < field_type.subtypes[subtype_idx - 1] {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Invalid schema type order: types should be numbered in increasing order. \
                            Outer type ID={type_id}, subtype_id={subtype_id}, previous subtype_id={}",
                            field_type.subtypes[subtype_idx - 1],
                        ),
                    ));
                }
            }
        }

        if footer.encryption.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Encrypted files are not supported",
            ));
        }

        Ok(footer)
    }

    fn read_postscript(&mut self) -> Result<(proto::PostScript, u64)> {
        let postscript_len = self.read_buffer[self.read_buffer.len() - 1] as usize;
        let postscript_start_pos = self.read_buffer.len() - postscript_len - 1;
        let postscript_body = &self.read_buffer[postscript_start_pos..self.read_buffer.len() - 1];

        let postscript = proto::PostScript::decode(postscript_body).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Postscript protobuf damaged: '{err}'"),
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

pub fn new_reader<T>(orc_file: &mut T, opts: ReaderOptions) -> Result<impl Reader>
where
    T: Read + Seek,
{
    let mut reader = PositionalReader::new(orc_file)?;

    let stream_len = reader.len();
    let end_pos = opts.file_end_pos.unwrap_or(stream_len - 1);
    if end_pos >= stream_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "ORC file stream length is {stream_len}, but end position(from read options) is {end_pos}",
            ),
        ));
    }

    let tail_reader = TailReader::new(&mut reader, end_pos, opts)?;
    let tail = tail_reader.read()?;
    Ok(FileReader { tail })
}

#[cfg(test)]
mod tests {
    use super::TailReader;
    use crate::io_utils::PositionalReader;
    use crate::reader::FileVersion;

    #[test]
    fn snappy_footer() {
        let mut file = std::fs::File::open("src/test_files/TestOrcFile.testSnappy.orc").unwrap();
        let mut file_reader = PositionalReader::new(&mut file).unwrap();
        let end_pos = file_reader.end_position();
        let tail_reader = TailReader::new(&mut file_reader, end_pos, Default::default()).unwrap();
        let tail = tail_reader.read().unwrap();
        assert_eq!(tail.row_count, 10000);
        assert_eq!(tail.content_size, 126061);
        assert_eq!(tail.header_size, 3);
        assert_eq!(tail.version, FileVersion(0, 12));
        assert!(tail.metadata.is_empty());
    }
}
