use std::collections::HashMap;
use std::io;
use std::ops::Deref;
use std::{
    format,
    io::{Read, Result, Seek},
};

use crate::compression::{new_decompress_stream, CompressionRegistry};
use crate::io_utils::{PositionalReader, UninitBytesMut};
use crate::proto::{self};
use crate::schema::read_schema;
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

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct FileVersion(u32, u32);

struct TailReader<'a, T: Read + Seek> {
    file_reader: &'a mut PositionalReader<'a, T>,
    opts: ReaderOptions,
    read_buffer: Bytes,
    file_end_pos: u64,
}

#[derive(Debug)]
struct FileTail {
    compression: proto::CompressionKind,
    version: FileVersion,
    header_size: u64,
    content_size: u64,
    row_count: u64,
    row_index_stride: u32,
    metadata: HashMap<String, Bytes>,
    schema: arrow::datatypes::Schema,
    column_statistics: Vec<proto::ColumnStatistics>,
    stripes: Vec<proto::StripeInformation>,
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
        if footer.encryption.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Encrypted files are not supported",
            ));
        }

        let schema = read_schema(&footer.types, &footer.statistics)?;

        Ok(FileTail {
            compression: postscript.compression(),
            version,
            header_size: footer.header_length(),
            content_size: footer.content_length(),
            row_count: footer.number_of_rows(),
            row_index_stride: footer.row_index_stride(),
            metadata: footer
                .metadata
                .iter()
                .map(|kv| (kv.name().to_owned(), kv.value().to_vec().into()))
                .collect(),
            schema,
            column_statistics: footer.statistics,
            stripes: footer.stripes,
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

        // postscript and footer read from the buffer, release it
        self.read_buffer = Bytes::new();

        let decompressed_footer = if postscript.compression() != proto::CompressionKind::None {
            // decompress the footer
            let compression_codec = self.opts.compression_opts.codec(postscript.compression())?;
            let mut decompressed_footer = Vec::with_capacity(footer_buffer.len());
            let mut footer_reader = new_decompress_stream(
                footer_buffer.reader(),
                compression_codec,
                postscript.compression_block_size(),
            );
            footer_reader
                .read_to_end(&mut decompressed_footer)
                .map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("ORC footer decompression failed: {err}"),
                    )
                })?;
            decompressed_footer
        } else {
            footer_buffer.to_vec()
        };

        let footer = proto::Footer::decode(decompressed_footer.deref()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Footer protobuf damaged: '{err}'"),
            )
        })?;

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
    use std::cell::Cell;
    use std::collections::HashMap;

    use arrow::datatypes;
    use googletest::matcher::Matcher;
    use googletest::matchers::eq;
    use googletest::{matcher::MatcherResult, verify_that, Result};

    use super::{FileTail, TailReader};
    use crate::io_utils::PositionalReader;
    use crate::reader::FileVersion;
    use crate::test_utils::hashmap_eq;

    struct FileTailMatcher {
        expected: FileTail,
        err_msg: Cell<String>,
    }

    impl Matcher<FileTail> for FileTailMatcher {
        fn matches(&self, actual: &FileTail) -> MatcherResult {
            let result = verify_that!(self.expected.content_size, eq(actual.content_size))
                .and(verify_that!(
                    self.expected.header_size,
                    eq(actual.header_size)
                ))
                .and(verify_that!(
                    self.expected.compression,
                    eq(actual.compression)
                ))
                .and(verify_that!(
                    self.expected.metadata,
                    hashmap_eq(&actual.metadata)
                ))
                .and(verify_that!(self.expected.version, eq(actual.version)))
                .and(verify_that!(self.expected.row_count, eq(actual.row_count)))
                .and(verify_that!(
                    self.expected.row_index_stride,
                    eq(actual.row_index_stride)
                ))
                .and(verify_that!(&self.expected.schema, eq(&actual.schema)));
            // .and(verify_that!(&self.expected.stripes, eq(&actual.stripes)))
            // .and(verify_that!(
            //     &self.expected.column_statistics,
            //     eq(&actual.column_statistics)
            // ));

            if let Some(err) = result.as_ref().err() {
                self.err_msg.set(err.to_string());
            }

            result
                .map(|_| MatcherResult::Matches)
                .unwrap_or(MatcherResult::DoesNotMatch)
        }

        fn describe(&self, matcher_result: MatcherResult) -> String {
            match matcher_result {
                MatcherResult::Matches => format!("is same as {:?}", self.expected),
                MatcherResult::DoesNotMatch => {
                    format!(" is not the same as expected: {}", self.err_msg.take())
                }
            }
        }
    }

    fn same_tail(expected: FileTail) -> FileTailMatcher {
        FileTailMatcher {
            expected,
            err_msg: Cell::default(),
        }
    }

    #[test]
    fn test_file_tail() -> Result<()> {
        for (file_name, footer) in prepared_footers() {
            let mut file = std::fs::File::open(format!("src/test_files/{file_name}")).unwrap();
            let mut file_reader = PositionalReader::new(&mut file).unwrap();
            let end_pos = file_reader.end_position();
            let tail_reader =
                TailReader::new(&mut file_reader, end_pos, Default::default()).unwrap();
            let tail = tail_reader.read().unwrap();
            verify_that!(tail, same_tail(footer))?
        }
        Ok(())
    }

    fn prepared_footers() -> HashMap<String, FileTail> {
        let mut footers = HashMap::new();
        footers.insert(
            "TestOrcFile.testSnappy.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                compression: crate::proto::CompressionKind::Snappy,
                content_size: 126061,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 10000,
                row_index_stride: 10000,
                version: FileVersion(0, 12),
                schema: datatypes::Schema::new(vec![
                    datatypes::Field::new("int1", datatypes::DataType::Int32, false),
                    datatypes::Field::new("string1", datatypes::DataType::Utf8, false),
                ]),
            },
        );

        footers.insert(
            "nulls-at-end-snappy.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                compression: crate::proto::CompressionKind::Snappy,
                content_size: 366347,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 70000,
                row_index_stride: 10000,
                version: FileVersion(0, 12),
                schema: datatypes::Schema::new(vec![
                    datatypes::Field::new("_col0", datatypes::DataType::Int8, false),
                    datatypes::Field::new("_col1", datatypes::DataType::Int16, false),
                    datatypes::Field::new("_col2", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col3", datatypes::DataType::Int64, false),
                    datatypes::Field::new("_col4", datatypes::DataType::Float32, false),
                    datatypes::Field::new("_col5", datatypes::DataType::Float64, false),
                    datatypes::Field::new("_col6", datatypes::DataType::Boolean, false),
                ]),
            },
        );

        footers.insert(
            "TestVectorOrcFile.testLz4.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                compression: crate::proto::CompressionKind::Lz4,
                content_size: 120952,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 10000,
                row_index_stride: 10000,
                version: FileVersion(0, 12),
                schema: datatypes::Schema::new(vec![
                    datatypes::Field::new("x", datatypes::DataType::Int64, false),
                    datatypes::Field::new("y", datatypes::DataType::Int32, false),
                    datatypes::Field::new("z", datatypes::DataType::Int64, false),
                ]),
            },
        );

        // footers.insert(
        //     "TestVectorOrcFile.testLzo.orc".to_string(),
        //     FileTail {
        //         column_statistics: vec![],
        //         stripes: vec![],
        //         compression: crate::proto::CompressionKind::Lzo,
        //         content_size: 120955,
        //         header_size: 3,
        //         metadata: HashMap::new(),
        //         row_count: 10000,
        //         row_index_stride: 10000,
        //         version: FileVersion(0, 12),
        //         schema: datatypes::Schema::new(vec![
        //             datatypes::Field::new("x", datatypes::DataType::Int64, false),
        //             datatypes::Field::new("y", datatypes::DataType::Int32, false),
        //             datatypes::Field::new("z", datatypes::DataType::Int64, false),
        //         ]),
        //     },
        // );

        footers.insert(
            "TestVectorOrcFile.testZstd.0.12.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                compression: crate::proto::CompressionKind::Zstd,
                content_size: 120734,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 10000,
                row_index_stride: 10000,
                version: FileVersion(0, 12),
                schema: datatypes::Schema::new(vec![
                    datatypes::Field::new("x", datatypes::DataType::Int64, false),
                    datatypes::Field::new("y", datatypes::DataType::Int32, false),
                    datatypes::Field::new("z", datatypes::DataType::Int64, false),
                ]),
            },
        );

        footers.insert(
            "demo-11-zlib.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                compression: crate::proto::CompressionKind::Zlib,
                content_size: 396823,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 1920800,
                row_index_stride: 10000,
                version: FileVersion(0, 11),
                schema: datatypes::Schema::new(vec![
                    datatypes::Field::new("_col0", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col1", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col2", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col3", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col4", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col5", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col6", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col7", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col8", datatypes::DataType::Int32, false),
                ]),
            },
        );

        footers.insert(
            "demo-12-zlib.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                compression: crate::proto::CompressionKind::Zlib,
                content_size: 45592,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 1920800,
                row_index_stride: 10000,
                version: FileVersion(0, 12),
                schema: datatypes::Schema::new(vec![
                    datatypes::Field::new("_col0", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col1", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col2", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col3", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col4", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col5", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col6", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col7", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col8", datatypes::DataType::Int32, false),
                ]),
            },
        );

        footers.insert(
            "demo-11-none.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                compression: crate::proto::CompressionKind::None,
                content_size: 5069718,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 1920800,
                row_index_stride: 10000,
                version: FileVersion(0, 11),
                schema: datatypes::Schema::new(vec![
                    datatypes::Field::new("_col0", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col1", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col2", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col3", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col4", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col5", datatypes::DataType::Utf8, false),
                    datatypes::Field::new("_col6", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col7", datatypes::DataType::Int32, false),
                    datatypes::Field::new("_col8", datatypes::DataType::Int32, false),
                ]),
            },
        );
        footers
    }
}
