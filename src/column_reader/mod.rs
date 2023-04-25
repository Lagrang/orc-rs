mod boolean_reader;
mod datetime_reader;
mod numeric_readers;

use crate::encoding::rle::{BooleanRleDecoder, IntRleDecoder, IntRleV1Decoder};
use crate::encoding::Integer;
use crate::io_utils::{self};
use crate::source::OrcFile;
use crate::{compression, proto, schema, OrcError, Result};

use arrow::datatypes::DataType;

use self::boolean_reader::BooleanReader;
use self::datetime_reader::TimestampReader;
use self::numeric_readers::{Int16Reader, Int32Reader, Int64Reader, Int8Reader};

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
) -> crate::Result<Box<dyn ColumnReader + 'a>> {
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

    let buffer_size = 4 * 1024;
    match column.data_type() {
        DataType::Boolean => Ok(Box::new(GenericReader::new(
            BooleanReader::new(data_stream),
            null_stream,
            buffer_size,
        ))),
        DataType::Int8 => Ok(Box::new(GenericReader::new(
            Int8Reader::new(data_stream, buffer_size),
            null_stream,
            buffer_size,
        ))),
        DataType::Int16 => Ok(Box::new(GenericReader::new(
            Int16Reader::new(
                data_stream,
                buffer_size,
                footer.columns[col_id as usize].clone(),
            ),
            null_stream,
            buffer_size,
        ))),
        DataType::Int32 => Ok(Box::new(GenericReader::new(
            Int32Reader::new(
                data_stream,
                buffer_size,
                footer.columns[col_id as usize].clone(),
            ),
            null_stream,
            buffer_size,
        ))),
        DataType::Int64 => Ok(Box::new(GenericReader::new(
            Int64Reader::new(
                data_stream,
                buffer_size,
                footer.columns[col_id as usize].clone(),
            ),
            null_stream,
            buffer_size,
        ))),
        DataType::Timestamp(_, _) => {
            let nanos_stream = open_stream_reader(
                col_id,
                proto::stream::Kind::Data,
                footer,
                stripe_meta,
                orc_file,
                compression,
            )?;
            Ok(Box::new(GenericReader::new(
                TimestampReader::new(
                    data_stream,
                    nanos_stream,
                    buffer_size,
                    footer.columns[col_id as usize].clone(),
                ),
                null_stream,
                buffer_size,
            )))
        }
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
    null_stream: BooleanRleDecoder<NullStream>,
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
            null_stream: BooleanRleDecoder::new(null_stream, buffer_size),
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
                self.remaining = buffer.len();
                self.nulls_chunk = Some(buffer);
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
        // TODO: optimize when there is no NULL or all values are NULL
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

fn create_int_rle<const N: usize, const M: usize, Input, IntType>(
    data_stream: Input,
    buffer_size: usize,
    encoding: &proto::ColumnEncoding,
) -> IntRleDecoder<Input, IntType>
where
    IntType: Integer<N, M>,
    Input: io_utils::BufRead,
{
    match encoding.kind() {
        proto::column_encoding::Kind::Direct | proto::column_encoding::Kind::Dictionary => {
            IntRleDecoder::new_v1(IntRleV1Decoder::<Input, IntType>::new(
                data_stream,
                buffer_size,
            ))
        }
        proto::column_encoding::Kind::DirectV2 | proto::column_encoding::Kind::DictionaryV2 => {
            todo!()
        }
    }
}