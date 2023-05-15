mod binary_reader;
mod boolean_reader;
mod complex_type_reader;
mod datetime_reader;
mod numeric_reader;

use crate::encoding::rlev1::{BooleanRleDecoder, IntRleDecoder, IntRleV1Decoder};
use crate::encoding::Integer;
use crate::io_utils::{self};
use crate::schema::OrcColumnId;
use crate::source::OrcFile;
use crate::{compression, proto, schema, OrcError, Result};

use arrow::datatypes::DataType;

use self::binary_reader::{BinaryReader, StringDictionaryReader, StringReader};
use self::boolean_reader::BooleanReader;
use self::complex_type_reader::{ListReader, StructReader};
use self::datetime_reader::{DateReader, TimestampReader};
use self::numeric_reader::{
    Decimal128Reader, Float32Reader, Float64Reader, Int16Reader, Int32Reader, Int64Reader,
    Int8Reader,
};

pub trait ColumnReader {
    /// Read next chunk of column values.
    ///
    /// Returns `None` if no more chunk left.
    fn read(&mut self, num_values: usize) -> crate::Result<Option<arrow::array::ArrayRef>>;
}

pub(crate) fn create_reader<'a>(
    column: &arrow::datatypes::Field,
    orc_file: &dyn OrcFile,
    footer: &proto::StripeFooter,
    stripe_meta: &proto::StripeInformation,
    compression: &'a compression::Compression,
    buffer_size: usize,
) -> crate::Result<Box<dyn ColumnReader + 'a>> {
    let col_id = column.orc_column_id()?;
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
    let col_encoding = &footer.columns[col_id as usize];

    match column.data_type() {
        DataType::Boolean => Ok(Box::new(GenericReader::new(
            BooleanReader::new(data_stream, buffer_size),
            null_stream,
            buffer_size,
        ))),
        DataType::Int8 => Ok(Box::new(GenericReader::new(
            Int8Reader::new(data_stream, buffer_size),
            null_stream,
            buffer_size,
        ))),
        DataType::Int16 => Ok(Box::new(GenericReader::new(
            Int16Reader::new(data_stream, buffer_size, col_encoding.clone()),
            null_stream,
            buffer_size,
        ))),
        DataType::Int32 => Ok(Box::new(GenericReader::new(
            Int32Reader::new(data_stream, buffer_size, col_encoding.clone()),
            null_stream,
            buffer_size,
        ))),
        DataType::Int64 => Ok(Box::new(GenericReader::new(
            Int64Reader::new(data_stream, buffer_size, col_encoding.clone()),
            null_stream,
            buffer_size,
        ))),
        DataType::Float32 => Ok(Box::new(GenericReader::new(
            Float32Reader::new(data_stream),
            null_stream,
            buffer_size,
        ))),
        DataType::Float64 => Ok(Box::new(GenericReader::new(
            Float64Reader::new(data_stream),
            null_stream,
            buffer_size,
        ))),
        DataType::Decimal128(precision, scale) => {
            let scale_stream = open_stream_reader(
                col_id,
                proto::stream::Kind::Secondary,
                footer,
                stripe_meta,
                orc_file,
                compression,
            )?;
            let decimal_reader = Decimal128Reader::new(
                *precision,
                *scale as u8,
                data_stream,
                scale_stream,
                buffer_size,
                col_encoding,
            );
            Ok(Box::new(GenericReader::new(
                decimal_reader,
                null_stream,
                buffer_size,
            )))
        }
        DataType::Date64 => Ok(Box::new(GenericReader::new(
            DateReader::new(data_stream, buffer_size, col_encoding),
            null_stream,
            buffer_size,
        ))),
        DataType::Timestamp(_, _) => {
            let nanos_stream = open_stream_reader(
                col_id,
                proto::stream::Kind::Secondary,
                footer,
                stripe_meta,
                orc_file,
                compression,
            )?;
            Ok(Box::new(GenericReader::new(
                TimestampReader::new(data_stream, nanos_stream, buffer_size, col_encoding),
                null_stream,
                buffer_size,
            )))
        }
        DataType::Binary => {
            let len_stream = open_stream_reader(
                col_id,
                proto::stream::Kind::Length,
                footer,
                stripe_meta,
                orc_file,
                compression,
            )?;
            Ok(Box::new(GenericReader::new(
                BinaryReader::new(data_stream, len_stream, buffer_size, col_encoding),
                null_stream,
                buffer_size,
            )))
        }
        DataType::Utf8 => {
            let len_stream = open_stream_reader(
                col_id,
                proto::stream::Kind::Length,
                footer,
                stripe_meta,
                orc_file,
                compression,
            )?;

            if col_encoding.kind() == proto::column_encoding::Kind::Dictionary
                || col_encoding.kind() == proto::column_encoding::Kind::DictionaryV2
            {
                let dict_data_stream = open_stream_reader(
                    col_id,
                    proto::stream::Kind::DictionaryData,
                    footer,
                    stripe_meta,
                    orc_file,
                    compression,
                )?;
                Ok(Box::new(GenericReader::new(
                    StringDictionaryReader::new(
                        data_stream,
                        dict_data_stream,
                        len_stream,
                        buffer_size,
                        col_encoding,
                    )?,
                    null_stream,
                    buffer_size,
                )))
            } else {
                Ok(Box::new(GenericReader::new(
                    StringReader::new(data_stream, len_stream, buffer_size, col_encoding),
                    null_stream,
                    buffer_size,
                )))
            }
        }
        DataType::Struct(fields) => {
            let mut child_readers: Vec<Box<dyn ColumnReader + 'a>> =
                Vec::with_capacity(fields.len());
            for f in fields {
                let reader: Box<dyn ColumnReader + 'a> =
                    create_reader(f, orc_file, footer, stripe_meta, compression, buffer_size)?;
                child_readers.push(reader);
            }
            Ok(Box::new(GenericReader::new(
                StructReader::new(fields.clone(), child_readers, footer, stripe_meta),
                null_stream,
                buffer_size,
            )))
        }
        dt @ DataType::List(field) => {
            let child_reader: Box<dyn ColumnReader + 'a> = create_reader(
                field,
                orc_file,
                footer,
                stripe_meta,
                compression,
                buffer_size,
            )?;
            let len_stream = open_stream_reader(
                col_id,
                proto::stream::Kind::Length,
                footer,
                stripe_meta,
                orc_file,
                compression,
            )?;
            Ok(Box::new(GenericReader::new(
                ListReader::new(
                    dt.clone(),
                    child_reader,
                    len_stream,
                    buffer_size,
                    col_encoding,
                ),
                null_stream,
                buffer_size,
            )))
        }
        DataType::Map(field, _) => {
            let child_reader: Box<dyn ColumnReader + 'a> = create_reader(
                field,
                orc_file,
                footer,
                stripe_meta,
                compression,
                buffer_size,
            )?;
            let len_stream = open_stream_reader(
                col_id,
                proto::stream::Kind::Length,
                footer,
                stripe_meta,
                orc_file,
                compression,
            )?;
            Ok(Box::new(GenericReader::new(
                ListReader::new(
                    DataType::List(field.clone()),
                    child_reader,
                    len_stream,
                    buffer_size,
                    col_encoding,
                ),
                null_stream,
                buffer_size,
            )))
        }
        _ => Err(OrcError::TypeNotSupported(column.data_type().clone())),
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

// TODO: replace all method by 1 which will take NULL bitmap and return Arrow array
trait ColumnProcessor {
    /// Read next chunk of data stream and stores it for the later use.
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()>;
    /// Signal to chunk reader to read value at index from data chunk(read by [`load_data_chunk`])
    /// and append it as next element into internal buffer which,
    /// at the end of the day, will be returned to user.
    fn append_value(&mut self, index: usize) -> crate::Result<()>;
    /// Append NULL as next value into internal buffer.
    fn append_null(&mut self) -> crate::Result<()>;
    /// Complete processing of current chunk and returns an Arrow array
    /// with column values(belong to the current chunk).
    fn complete(&mut self) -> crate::Result<arrow::array::ArrayRef>;
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
                let non_null_count = buffer.count_set_bits();
                self.nulls_chunk = Some(buffer);
                self.data_index = 0;
                // Data stream doesn't store placeholders for NULL values,
                // request to read rows equals count of not NULL values.
                if non_null_count != 0 {
                    self.type_reader.load_chunk(non_null_count)?;
                }
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
                self.type_reader.append_value(self.data_index)?;
                self.data_index += 1;
            } else {
                self.type_reader.append_null()?;
            }
            self.remaining -= 1;
        }

        let array = self.type_reader.complete()?;
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
    Input: std::io::Read,
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
