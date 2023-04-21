use std::sync::Arc;

use crate::encoding::rle::ByteRleDecoder;
use crate::io_utils::{self};
use crate::source::OrcFile;
use crate::{compression, proto, schema, OrcError, Result};

use arrow::datatypes::DataType;

pub trait ColumnReader {
    /// Read next chunk of column values.
    ///
    /// Returns `None` if no more chunk left.
    fn read(&mut self, num_values: usize) -> crate::Result<Option<arrow::array::ArrayRef>>;
}

pub(crate) fn create_reader<'a>(
    column: &arrow::datatypes::Field,
    orc_file: &'a dyn OrcFile,
    footer: &proto::StripeFooter,
    stripe_meta: &proto::StripeInformation,
    compression: &'a compression::Compression,
) -> crate::Result<impl ColumnReader + 'a> {
    let col_id = schema::get_column_id(column)?;
    let null_stream = open_stream_reader(
        col_id,
        proto::stream::Kind::Present,
        footer,
        stripe_meta,
        orc_file,
        compression,
    )?;
    let data_stream = open_stream_reader(
        col_id,
        proto::stream::Kind::Data,
        footer,
        stripe_meta,
        orc_file,
        compression,
    )?;
    match column.data_type() {
        DataType::Boolean => Ok(GenericReader::new(
            BooleanReader::new(data_stream),
            null_stream,
            4 * 1024,
        )),
        _ => panic!(""),
    }
}

fn open_stream_reader<'a>(
    col_id: u32,
    stream_kind: proto::stream::Kind,
    footer: &proto::StripeFooter,
    stripe_meta: &proto::StripeInformation,
    orc_file: &dyn OrcFile,
    compression: &'a compression::Compression,
) -> Result<impl io_utils::BufRead + 'a> {
    let mut offset = stripe_meta.offset();
    for s in &footer.streams {
        if s.column() == col_id && s.kind() == stream_kind {
            return compression
                .new_reader(orc_file, offset, s.length())
                .map_err(|e| OrcError::IoError(e.kind(), e.to_string()));
        }
        offset += s.length();
    }

    Err(OrcError::InvalidStreamKind(stream_kind, col_id))
}

trait ColumnProcessor {
    /// Read next chunk of data stream and stores it for the later use.
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()>;
    /// Signal to chunk reader to read value at index from data chunk(read by [`load_data_chunk`])
    /// and append it as next element into internal buffer which,
    /// at the end of the day, will be returned to user.
    fn append_value(&mut self, index: usize);
    /// Append NULL as next value into internal buffer.
    fn append_null(&mut self);
    /// Complete processing of current chunk and returns an Arrow array
    /// with column values(belong to the current chunk).
    fn complete(&mut self) -> arrow::array::ArrayRef;
}

/// Base column reader handles reading of present(NULL) stream and provide facilities
/// to read any column data using abstraction provided by [`ChunkProcessor`] trait.
struct GenericReader<NullStream, TypedReader> {
    /// NULLs stream which indicate where column stores actual value or NULL.
    /// If bit in byte(decoded from RLE encoded data) is set to 1, then
    /// column has value for this row. Otherwise, value is NULL.
    null_stream: ByteRleDecoder<NullStream>,
    /// NULL bitmap. Bit set to 1 if column has non NULL value at this position.
    /// Buffer contain data read from RLE streams which is not consumed yet.
    nulls_chunk: Option<arrow::buffer::BooleanBuffer>,
    /// Number of remaining values in current chunk.
    remaining: usize,
    /// Index of next row(in column data) to read into resulting Arrow array.
    data_index: usize,
    type_reader: TypedReader,
}

impl<NullStream: io_utils::BufRead, TypedReader: ColumnProcessor>
    GenericReader<NullStream, TypedReader>
{
    fn new(type_reader: TypedReader, null_stream: NullStream, buffer_size: usize) -> Self {
        GenericReader {
            null_stream: ByteRleDecoder::new(null_stream, buffer_size),
            nulls_chunk: None,
            remaining: 0,
            data_index: 0,
            type_reader,
        }
    }
}

impl<NullStream: io_utils::BufRead, Processor: ColumnProcessor> ColumnReader
    for GenericReader<NullStream, Processor>
{
    fn read(&mut self, num_values: usize) -> crate::Result<Option<arrow::array::ArrayRef>> {
        if self.remaining == 0 {
            // Decode more data for NULL and data stream
            if let Some(buffer) = self.null_stream.read(num_values)? {
                let len = buffer.len();
                self.nulls_chunk = Some(arrow::buffer::BooleanBuffer::new(buffer, 0, len));
                self.remaining = len;
                self.data_index = 0;
                // Data buffer read can return empty buffer if only NULLs values remain in a column
                self.type_reader.load_chunk(num_values)?;
            } else {
                return Ok(None);
            }
        }

        // Read NULL bitmap and process next data chunk(through chunk processor) using it.
        let null_bitmap = self
            .nulls_chunk
            .as_ref()
            .expect("NULL chunk must exist at this point");
        for _ in 0..std::cmp::min(num_values, self.remaining) {
            // Check if the next value NULL or not.
            if null_bitmap.value(null_bitmap.len() - self.remaining) {
                self.type_reader.append_value(self.data_index);
                self.data_index += 1;
            } else {
                self.type_reader.append_null();
            }
            self.remaining -= 1;
        }

        let array = self.type_reader.complete();
        Ok(Some(array))
    }
}

struct BooleanReader<DataStream> {
    /// Decoded data of boolean column. Boolean column values encoded using byte RLE
    /// where each N'th bit is a boolean value of N'th row in column.
    /// Boolean values packed into bytes instead of using 1 byte per boolean value.
    data_stream: ByteRleDecoder<DataStream>,
    /// Data chunk buffer. Contain values which are not a NULL.
    data_chunk: Option<arrow::buffer::BooleanBuffer>,
    /// Builder for a data array which will be returned to the user.
    array_builder: arrow::array::BooleanBuilder,
}

impl<'a, DataStream: io_utils::BufRead + 'a> BooleanReader<DataStream> {
    fn new(data_stream: DataStream) -> BooleanReader<DataStream> {
        Self {
            data_stream: ByteRleDecoder::new(data_stream, 4 * 1024),
            data_chunk: None,
            array_builder: arrow::array::BooleanBuilder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for BooleanReader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        if let Some(buffer) = self.data_stream.read(num_values)? {
            let len = buffer.len();
            self.data_chunk = Some(arrow::buffer::BooleanBuffer::new(buffer, 0, len));
        } else {
            self.data_chunk = None;
        }
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data.value(index);
        self.array_builder.append_value(col_val);
    }

    fn append_null(&mut self) {
        self.array_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.array_builder.finish())
    }
}

pub(crate) struct IntReader {}
