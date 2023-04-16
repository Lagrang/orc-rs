use std::collections::HashMap;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use prost::Message;

use crate::compression;
use crate::compression::CompressionRegistry;
use crate::io_utils;
use crate::proto;
use crate::schema;
use crate::OrcError;

#[derive(Debug)]
pub(crate) struct FileTail {
    pub postscript: proto::PostScript,
    pub version: FileVersion,
    pub header_size: u64,
    pub content_size: u64,
    pub row_count: u64,
    pub row_index_stride: u32,
    pub metadata: HashMap<String, Bytes>,
    pub schema: arrow::datatypes::SchemaRef,
    pub column_statistics: Vec<proto::ColumnStatistics>,
    pub stripes: Vec<proto::StripeInformation>,
}

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct FileVersion(pub u32, pub u32);

/// Struct which allows to read ORC file metadata: postscript, footer and metadata.
pub struct FileMetadataReader {
    file_reader: Box<dyn io_utils::PositionalRead>,
}

/// Try to read 16K from ORC file tail to get footer and postscript in one call.
/// Should be at least 256 in order to read entire postscript.
const TAIL_BUFFER_SIZE: usize = 16 * 1024;
/// Length of the encoded postscript size. Last byte of the ORC file contains length of the postscript body.
const POSTSCRIPT_SIZE_BYTES: u64 = 1;

impl FileMetadataReader {
    pub fn new(file_reader: Box<dyn io_utils::PositionalRead>) -> crate::Result<Self> {
        Ok(FileMetadataReader { file_reader })
    }

    pub(crate) fn read_tail(
        &mut self,
        compression_registry: CompressionRegistry,
    ) -> crate::Result<(FileTail, compression::Compression)> {
        let file_len: u64 = self.file_reader.len();
        let (read_pos, buf_size) = if file_len > TAIL_BUFFER_SIZE as u64 {
            (file_len - TAIL_BUFFER_SIZE as u64, TAIL_BUFFER_SIZE)
        } else {
            (0, file_len as usize)
        };

        let mut buffer = bytes::BytesMut::with_capacity(buf_size);
        let bytes_read = self.file_reader.read_at(read_pos, &mut buffer)?;
        if bytes_read < 4 {
            return Err(OrcError::UnexpectedEof(bytes_read));
        }

        let mut tail_buffer = buffer.freeze();

        let (postscript, postscript_len) = self.read_postscript(&mut tail_buffer)?;
        let tail_size = postscript.footer_length() + postscript_len + POSTSCRIPT_SIZE_BYTES;
        let file_size = self.file_reader.len();
        if tail_size >= file_size {
            return Err(OrcError::InvalidTail(tail_size, file_size));
        }

        let mut version = FileVersion(0, 0);
        if postscript.version.len() == 2 {
            version.0 = postscript.version[0];
            version.1 = postscript.version[1];
        }

        let compression = compression::Compression::new(
            compression_registry,
            postscript.compression(),
            postscript.compression_block_size(),
        )?;

        let footer = self.read_footer(&tail_buffer, &postscript, postscript_len, &compression)?;
        if footer.encryption.is_some() {
            return Err(OrcError::UnsupportedFeature("Encrypted files".to_string()));
        }

        let schema = schema::read_schema(&footer.types, &footer.statistics)?;

        Ok((
            FileTail {
                postscript,
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
                schema: Arc::new(schema),
                column_statistics: footer.statistics,
                stripes: footer.stripes,
            },
            compression,
        ))
    }

    pub fn read_metadata(
        &mut self,
        postscript: &proto::PostScript,
        postscript_len: usize,
    ) -> crate::Result<Option<proto::Metadata>> {
        if postscript.metadata_length() == 0 {
            return Ok(None);
        }

        let metadata_size = postscript.metadata_length() as usize;
        let tail_size = postscript.footer_length() + postscript_len as u64 + POSTSCRIPT_SIZE_BYTES;
        let metadata_offset = self.file_reader.len() - tail_size - metadata_size as u64;
        let mut metadata_buffer = self.file_reader.read_exact_at(
            metadata_offset,
            metadata_size,
            "Invalid metadata size",
        )?;

        Ok(Some(proto::Metadata::decode(&mut metadata_buffer)?))
    }

    fn read_footer(
        &mut self,
        read_buffer: &Bytes,
        postscript: &proto::PostScript,
        postscript_len: u64,
        compression: &compression::Compression,
    ) -> crate::Result<proto::Footer> {
        let declared_footer_len = postscript.footer_length() as usize;
        let footer_buffer = if declared_footer_len <= read_buffer.len() {
            // footer already read into a buffer
            read_buffer.slice(read_buffer.len() - declared_footer_len..)
        } else {
            let file_size = self.file_reader.len();
            let tail_size = postscript.footer_length() + postscript_len + POSTSCRIPT_SIZE_BYTES;
            let footer_pos = file_size - tail_size;
            self.file_reader.read_exact_at(
                footer_pos,
                declared_footer_len,
                "Invalid file footer size",
            )?
        };

        let decompressed_footer = compression
            .decompress_buffer(footer_buffer)
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("ORC footer decompression failed: {err}"),
                )
            })?;

        let footer = proto::Footer::decode(decompressed_footer).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Footer protobuf damaged: '{err}'"),
            )
        })?;

        Ok(footer)
    }

    fn read_postscript(
        &mut self,
        read_buffer: &mut Bytes,
    ) -> std::io::Result<(proto::PostScript, u64)> {
        let postscript_len = read_buffer[read_buffer.len() - 1] as usize;
        let postscript_start_pos = read_buffer.len() - postscript_len - 1;
        let postscript_body = &read_buffer[postscript_start_pos..read_buffer.len() - 1];

        let postscript = proto::PostScript::decode(postscript_body).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Postscript protobuf damaged: '{err}'"),
            )
        })?;

        // validate postscript data
        if postscript.magic() != "ORC" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid postscript magic: {}.", postscript.magic()),
            ));
        }
        let file_size = self.file_reader.len();
        let metadata_size = postscript.metadata_length();
        let footer_size = postscript.footer_length();
        if file_size < metadata_size + footer_size + postscript_len as u64 + POSTSCRIPT_SIZE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid postscript length data: \
                        metadata length={metadata_size}, \
                        footer size={footer_size}, \
                        postscript size={postscript_len}, \
                        but total file size is {file_size}.",
                ),
            ));
        }

        read_buffer.truncate(read_buffer.len() - (postscript_len + POSTSCRIPT_SIZE_BYTES as usize));
        Ok((postscript, postscript_len as u64))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use arrow::datatypes::{self};
    use googletest::{verify_that, Result};

    use super::{FileMetadataReader, FileTail, FileVersion};
    use crate::schema;
    use crate::source::{FileSource, OrcFile};
    use crate::test::matchers::same_tail;

    #[test]
    fn test_file_tail() -> Result<()> {
        for (file_name, footer) in prepared_footers() {
            let file = FileSource::new(Path::new(&format!("src/test/test_files/{file_name}")))?;
            let mut tail_reader = FileMetadataReader::new(file.positional_reader()?).unwrap();
            let (tail, _) = tail_reader.read_tail(Default::default()).unwrap();
            verify_that!(tail, same_tail(footer))?;
        }
        Ok(())
    }

    fn prepared_footers() -> HashMap<String, FileTail> {
        let mut footers = HashMap::new();
        let mut snappy_postscript = crate::proto::PostScript::default();
        snappy_postscript.set_compression(crate::proto::CompressionKind::Snappy);
        footers.insert(
            "TestOrcFile.testSnappy.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                postscript: snappy_postscript.clone(),
                content_size: 126061,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 10000,
                row_index_stride: 10000,
                version: FileVersion(0, 12),
                schema: Arc::new(datatypes::Schema::new(vec![
                    schema::set_column_id(
                        datatypes::Field::new("int1", datatypes::DataType::Int32, false),
                        1,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("string1", datatypes::DataType::Utf8, false),
                        2,
                    ),
                ])),
            },
        );

        footers.insert(
            "nulls-at-end-snappy.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                postscript: snappy_postscript,
                content_size: 366347,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 70000,
                row_index_stride: 10000,
                version: FileVersion(0, 12),
                schema: Arc::new(datatypes::Schema::new(vec![
                    schema::set_column_id(
                        datatypes::Field::new("_col0", datatypes::DataType::Int8, false),
                        1,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col1", datatypes::DataType::Int16, false),
                        2,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col2", datatypes::DataType::Int32, false),
                        3,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col3", datatypes::DataType::Int64, false),
                        4,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col4", datatypes::DataType::Float32, false),
                        5,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col5", datatypes::DataType::Float64, false),
                        6,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col6", datatypes::DataType::Boolean, false),
                        7,
                    ),
                ])),
            },
        );

        let mut lz4_postscript = crate::proto::PostScript::default();
        lz4_postscript.set_compression(crate::proto::CompressionKind::Lz4);
        footers.insert(
            "TestVectorOrcFile.testLz4.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                postscript: lz4_postscript,
                content_size: 120952,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 10000,
                row_index_stride: 10000,
                version: FileVersion(0, 12),
                schema: Arc::new(datatypes::Schema::new(vec![
                    schema::set_column_id(
                        datatypes::Field::new("x", datatypes::DataType::Int64, false),
                        1,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("y", datatypes::DataType::Int32, false),
                        2,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("z", datatypes::DataType::Int64, false),
                        3,
                    ),
                ])),
            },
        );

        let mut zstd_postscript = crate::proto::PostScript::default();
        zstd_postscript.set_compression(crate::proto::CompressionKind::Zstd);
        footers.insert(
            "TestVectorOrcFile.testZstd.0.12.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                postscript: zstd_postscript,
                content_size: 120734,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 10000,
                row_index_stride: 10000,
                version: FileVersion(0, 12),
                schema: Arc::new(datatypes::Schema::new(vec![
                    schema::set_column_id(
                        datatypes::Field::new("x", datatypes::DataType::Int64, false),
                        1,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("y", datatypes::DataType::Int32, false),
                        2,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("z", datatypes::DataType::Int64, false),
                        3,
                    ),
                ])),
            },
        );

        let mut zlib_postscript = crate::proto::PostScript::default();
        zlib_postscript.set_compression(crate::proto::CompressionKind::Zlib);
        footers.insert(
            "demo-11-zlib.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                postscript: zlib_postscript.clone(),
                content_size: 396823,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 1920800,
                row_index_stride: 10000,
                version: FileVersion(0, 11),
                schema: Arc::new(datatypes::Schema::new(vec![
                    schema::set_column_id(
                        datatypes::Field::new("_col0", datatypes::DataType::Int32, false),
                        1,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col1", datatypes::DataType::Utf8, false),
                        2,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col2", datatypes::DataType::Utf8, false),
                        3,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col3", datatypes::DataType::Utf8, false),
                        4,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col4", datatypes::DataType::Int32, false),
                        5,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col5", datatypes::DataType::Utf8, false),
                        6,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col6", datatypes::DataType::Int32, false),
                        7,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col7", datatypes::DataType::Int32, false),
                        8,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col8", datatypes::DataType::Int32, false),
                        9,
                    ),
                ])),
            },
        );

        footers.insert(
            "demo-12-zlib.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                postscript: zlib_postscript,
                content_size: 45592,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 1920800,
                row_index_stride: 10000,
                version: FileVersion(0, 12),
                schema: Arc::new(datatypes::Schema::new(vec![
                    schema::set_column_id(
                        datatypes::Field::new("_col0", datatypes::DataType::Int32, false),
                        1,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col1", datatypes::DataType::Utf8, false),
                        2,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col2", datatypes::DataType::Utf8, false),
                        3,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col3", datatypes::DataType::Utf8, false),
                        4,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col4", datatypes::DataType::Int32, false),
                        5,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col5", datatypes::DataType::Utf8, false),
                        6,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col6", datatypes::DataType::Int32, false),
                        7,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col7", datatypes::DataType::Int32, false),
                        8,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col8", datatypes::DataType::Int32, false),
                        9,
                    ),
                ])),
            },
        );

        footers.insert(
            "demo-11-none.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                postscript: crate::proto::PostScript::default(),
                content_size: 5069718,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 1920800,
                row_index_stride: 10000,
                version: FileVersion(0, 11),
                schema: Arc::new(datatypes::Schema::new(vec![
                    schema::set_column_id(
                        datatypes::Field::new("_col0", datatypes::DataType::Int32, false),
                        1,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col1", datatypes::DataType::Utf8, false),
                        2,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col2", datatypes::DataType::Utf8, false),
                        3,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col3", datatypes::DataType::Utf8, false),
                        4,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col4", datatypes::DataType::Int32, false),
                        5,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col5", datatypes::DataType::Utf8, false),
                        6,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col6", datatypes::DataType::Int32, false),
                        7,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col7", datatypes::DataType::Int32, false),
                        8,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("_col8", datatypes::DataType::Int32, false),
                        9,
                    ),
                ])),
            },
        );

        // "middle:struct< list:array<struct<int1:int,string1:string>> >,
        // list:array<struct<int1:int,string1:string>>,
        // map:map<string,struct<int1:int,string1:string>>,
        // ts:timestamp,
        // decimal1:decimal(0,0)>"

        footers.insert(
            "orc-file-11-format.orc".to_string(),
            FileTail {
                column_statistics: vec![],
                stripes: vec![],
                postscript: crate::proto::PostScript::default(),
                content_size: 372542,
                header_size: 3,
                metadata: HashMap::new(),
                row_count: 7500,
                row_index_stride: 10000,
                version: FileVersion(0, 11),
                schema: Arc::new(datatypes::Schema::new(vec![
                    schema::set_column_id(
                        datatypes::Field::new("boolean1", datatypes::DataType::Boolean, false),
                        1,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("byte1", datatypes::DataType::Int8, false),
                        2,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("short1", datatypes::DataType::Int16, false),
                        3,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("int1", datatypes::DataType::Int32, false),
                        4,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("long1", datatypes::DataType::Int64, false),
                        5,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("float1", datatypes::DataType::Float32, false),
                        6,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("double1", datatypes::DataType::Float64, false),
                        7,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("bytes1", datatypes::DataType::Binary, false),
                        8,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new("string1", datatypes::DataType::Utf8, false),
                        9,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new(
                            "middle",
                            datatypes::DataType::Struct(vec![schema::set_column_id(
                                datatypes::Field::new(
                                    "list",
                                    datatypes::DataType::List(Box::new(schema::set_column_id(
                                        datatypes::Field::new(
                                            "inner",
                                            datatypes::DataType::Struct(vec![
                                                schema::set_column_id(
                                                    datatypes::Field::new(
                                                        "int1",
                                                        datatypes::DataType::Int32,
                                                        false,
                                                    ),
                                                    13,
                                                ),
                                                schema::set_column_id(
                                                    datatypes::Field::new(
                                                        "string1",
                                                        datatypes::DataType::Utf8,
                                                        false,
                                                    ),
                                                    14,
                                                ),
                                            ]),
                                            false,
                                        ),
                                        12,
                                    ))),
                                    false,
                                ),
                                11,
                            )]),
                            false,
                        ),
                        10,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new(
                            "list",
                            datatypes::DataType::List(Box::new(schema::set_column_id(
                                datatypes::Field::new(
                                    "inner",
                                    datatypes::DataType::Struct(vec![
                                        schema::set_column_id(
                                            datatypes::Field::new(
                                                "int1",
                                                datatypes::DataType::Int32,
                                                false,
                                            ),
                                            17,
                                        ),
                                        schema::set_column_id(
                                            datatypes::Field::new(
                                                "string1",
                                                datatypes::DataType::Utf8,
                                                false,
                                            ),
                                            18,
                                        ),
                                    ]),
                                    false,
                                ),
                                16,
                            ))),
                            false,
                        ),
                        15,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new(
                            "map",
                            datatypes::DataType::Map(
                                Box::new(datatypes::Field::new(
                                    "entries",
                                    datatypes::DataType::Struct(vec![
                                        schema::set_column_id(
                                            datatypes::Field::new(
                                                "key",
                                                datatypes::DataType::Utf8,
                                                false,
                                            ),
                                            20,
                                        ),
                                        schema::set_column_id(
                                            datatypes::Field::new(
                                                "value",
                                                datatypes::DataType::Struct(vec![
                                                    schema::set_column_id(
                                                        datatypes::Field::new(
                                                            "int1",
                                                            datatypes::DataType::Int32,
                                                            false,
                                                        ),
                                                        22,
                                                    ),
                                                    schema::set_column_id(
                                                        datatypes::Field::new(
                                                            "string1",
                                                            datatypes::DataType::Utf8,
                                                            false,
                                                        ),
                                                        23,
                                                    ),
                                                ]),
                                                false,
                                            ),
                                            21,
                                        ),
                                    ]),
                                    false,
                                )),
                                false,
                            ),
                            false,
                        ),
                        19,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new(
                            "ts",
                            datatypes::DataType::Timestamp(
                                datatypes::TimeUnit::Nanosecond,
                                Some("UTC".into()),
                            ),
                            false,
                        ),
                        24,
                    ),
                    schema::set_column_id(
                        datatypes::Field::new(
                            "decimal1",
                            datatypes::DataType::Decimal128(0, 0),
                            false,
                        ),
                        25,
                    ),
                ])),
            },
        );
        footers
    }
}
