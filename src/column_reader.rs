use std::sync::Arc;

use crate::encoding::rle::{BooleanRleDecoder, ByteRleDecoder, IntRleDecoder, IntRleV1Decoder};
use crate::encoding::Integer;
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

struct BooleanReader<DataStream> {
    /// Decoded data of boolean column. Boolean column values encoded using byte RLE
    /// where each N'th bit is a boolean value of N'th row in column.
    /// Boolean values packed into bytes instead of using 1 byte per boolean value.
    rle: BooleanRleDecoder<DataStream>,
    /// Data chunk buffer. Contain values which are not a NULL.
    data_chunk: Option<arrow::buffer::BooleanBuffer>,
    /// Builder for a data array which will be returned to the user.
    array_builder: arrow::array::BooleanBuilder,
}

impl<'a, DataStream: io_utils::BufRead + 'a> BooleanReader<DataStream> {
    fn new(data_stream: DataStream) -> BooleanReader<DataStream> {
        Self {
            rle: BooleanRleDecoder::new(data_stream, 4 * 1024),
            data_chunk: None,
            array_builder: arrow::array::BooleanBuilder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for BooleanReader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        if let Some(buffer) = self.rle.read(num_values)? {
            self.data_chunk = Some(buffer);
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

fn create_int_rle<const N: usize, const M: usize, Input, IntType>(
    data_stream: Input,
    buffer_size: usize,
    encoding: proto::ColumnEncoding,
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

pub(crate) struct Int8Reader<Input> {
    rle: ByteRleDecoder<Input>,
    /// Data chunk buffer. Contain values which are not a NULL.
    data_chunk: Option<arrow::buffer::Buffer>,
    /// Builder for a data array which will be returned to the user.
    array_builder: arrow::array::Int8Builder,
}

impl<DataStream> Int8Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    fn new(data_stream: DataStream, buffer_size: usize) -> Self {
        Self {
            rle: ByteRleDecoder::new(data_stream, buffer_size),
            data_chunk: None,
            array_builder: arrow::array::Int8Builder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Int8Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        if let Some(buffer) = self.rle.read(num_values)? {
            self.data_chunk = Some(buffer);
        } else {
            self.data_chunk = None;
        }
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index] as i8;
        self.array_builder.append_value(col_val);
    }

    fn append_null(&mut self) {
        self.array_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.array_builder.finish())
    }
}

pub(crate) struct Int16Reader<Input> {
    rle: IntRleDecoder<Input, i16>,
    /// Data chunk buffer. Contain values which are not a NULL.
    data_chunk: Option<arrow::buffer::ScalarBuffer<i16>>,
    /// Builder for a data array which will be returned to the user.
    array_builder: arrow::array::Int16Builder,
}

impl<DataStream> Int16Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    fn new(data_stream: DataStream, buffer_size: usize, encoding: proto::ColumnEncoding) -> Self {
        Self {
            rle: create_int_rle(data_stream, buffer_size, encoding),
            data_chunk: None,
            array_builder: arrow::array::Int16Builder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Int16Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        if let Some(buffer) = self.rle.read(num_values)? {
            self.data_chunk = Some(buffer);
        } else {
            self.data_chunk = None;
        }
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index];
        self.array_builder.append_value(col_val);
    }

    fn append_null(&mut self) {
        self.array_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.array_builder.finish())
    }
}

pub(crate) struct Int32Reader<Input> {
    rle: IntRleDecoder<Input, i32>,
    /// Data chunk buffer. Contain values which are not a NULL.
    data_chunk: Option<arrow::buffer::ScalarBuffer<i32>>,
    /// Builder for a data array which will be returned to the user.
    array_builder: arrow::array::Int32Builder,
}

impl<DataStream> Int32Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    fn new(data_stream: DataStream, buffer_size: usize, encoding: proto::ColumnEncoding) -> Self {
        Self {
            rle: create_int_rle(data_stream, buffer_size, encoding),
            data_chunk: None,
            array_builder: arrow::array::Int32Builder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Int32Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        if let Some(buffer) = self.rle.read(num_values)? {
            self.data_chunk = Some(buffer);
        } else {
            self.data_chunk = None;
        }
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index];
        self.array_builder.append_value(col_val);
    }

    fn append_null(&mut self) {
        self.array_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.array_builder.finish())
    }
}

pub(crate) struct Int64Reader<Input> {
    rle: IntRleDecoder<Input, i64>,
    /// Data chunk buffer. Contain values which are not a NULL.
    data_chunk: Option<arrow::buffer::ScalarBuffer<i64>>,
    /// Builder for a data array which will be returned to the user.
    array_builder: arrow::array::Int64Builder,
}

impl<DataStream> Int64Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    fn new(data_stream: DataStream, buffer_size: usize, encoding: proto::ColumnEncoding) -> Self {
        Self {
            rle: create_int_rle(data_stream, buffer_size, encoding),
            data_chunk: None,
            array_builder: arrow::array::Int64Builder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Int64Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        if let Some(buffer) = self.rle.read(num_values)? {
            self.data_chunk = Some(buffer);
        } else {
            self.data_chunk = None;
        }
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index];
        self.array_builder.append_value(col_val);
    }

    fn append_null(&mut self) {
        self.array_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.array_builder.finish())
    }
}

pub(crate) struct Float32Reader<Input> {
    /// Raw float data read from file.
    data: Input,
    data_chunk: bytes::Bytes,
    /// Builder for a data array which will be returned to the user.
    array_builder: arrow::array::Float32Builder,
}

impl<DataStream> Float32Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    fn new(data_stream: DataStream) -> Self {
        Self {
            data: data_stream,
            data_chunk: bytes::Bytes::new(),
            array_builder: arrow::array::Float32Builder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Float32Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        let mut buffer = bytes::BytesMut::with_capacity(num_values * 4);
        io_utils::BufRead::read(&mut self.data, &mut buffer)?;
        self.data_chunk = buffer.freeze();
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        let float_bytes = [0u8; 4];
        let start_pos = index * float_bytes.len();
        float_bytes.copy_from_slice(&self.data_chunk[start_pos..start_pos + float_bytes.len()]);
        self.array_builder
            .append_value(f32::from_le_bytes(float_bytes));
    }

    fn append_null(&mut self) {
        self.array_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.array_builder.finish())
    }
}

pub(crate) struct Float64Reader<Input> {
    /// Raw float data read from file.
    data: Input,
    data_chunk: bytes::Bytes,
    /// Builder for a data array which will be returned to the user.
    array_builder: arrow::array::Float64Builder,
}

impl<DataStream> Float64Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    fn new(data_stream: DataStream) -> Self {
        Self {
            data: data_stream,
            data_chunk: bytes::Bytes::new(),
            array_builder: arrow::array::Float64Builder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Float64Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        let mut buffer = bytes::BytesMut::with_capacity(num_values * 8);
        io_utils::BufRead::read(&mut self.data, &mut buffer)?;
        self.data_chunk = buffer.freeze();
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        let float_bytes = [0u8; 8];
        let start_pos = index * float_bytes.len();
        float_bytes.copy_from_slice(&self.data_chunk[start_pos..start_pos + float_bytes.len()]);
        self.array_builder
            .append_value(f64::from_le_bytes(float_bytes));
    }

    fn append_null(&mut self) {
        self.array_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.array_builder.finish())
    }
}
