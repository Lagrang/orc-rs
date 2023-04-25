use std::sync::Arc;

use crate::encoding::rle::{ByteRleDecoder, IntRleDecoder};
use crate::{io_utils, proto, OrcError};

use super::{create_int_rle, ColumnProcessor};

pub struct Int8Reader<Input> {
    rle: ByteRleDecoder<Input>,
    /// Data chunk buffer. Contain values which are not a NULL.
    data_chunk: Option<arrow::buffer::Buffer>,
    /// Builder for a data array which will be returned to the user.
    result_builder: arrow::array::Int8Builder,
}

impl<DataStream> Int8Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    pub fn new(data_stream: DataStream, buffer_size: usize) -> Self {
        Self {
            rle: ByteRleDecoder::new(data_stream, buffer_size),
            data_chunk: None,
            result_builder: arrow::array::Int8Builder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Int8Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        self.data_chunk = Some(
            self.rle
                .read(num_values)?
                .ok_or(OrcError::MalformedPresentOrDataStream)?,
        );
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index] as i8;
        self.result_builder.append_value(col_val);
    }

    fn append_null(&mut self) {
        self.result_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.result_builder.finish())
    }
}

pub struct Int16Reader<Input> {
    rle: IntRleDecoder<Input, i16>,
    data_chunk: Option<arrow::buffer::ScalarBuffer<i16>>,
    result_builder: arrow::array::Int16Builder,
}

impl<DataStream> Int16Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    pub fn new(
        data_stream: DataStream,
        buffer_size: usize,
        encoding: proto::ColumnEncoding,
    ) -> Self {
        Self {
            rle: create_int_rle(data_stream, buffer_size, &encoding),
            data_chunk: None,
            result_builder: arrow::array::Int16Builder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Int16Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        self.data_chunk = Some(
            self.rle
                .read(num_values)?
                .ok_or(OrcError::MalformedPresentOrDataStream)?,
        );
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index];
        self.result_builder.append_value(col_val);
    }

    fn append_null(&mut self) {
        self.result_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.result_builder.finish())
    }
}

pub struct Int32Reader<Input> {
    rle: IntRleDecoder<Input, i32>,
    /// Data chunk buffer. Contain values which are not a NULL.
    data_chunk: Option<arrow::buffer::ScalarBuffer<i32>>,
    /// Builder for a data array which will be returned to the user.
    result_builder: arrow::array::Int32Builder,
}

impl<DataStream> Int32Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    pub fn new(
        data_stream: DataStream,
        buffer_size: usize,
        encoding: proto::ColumnEncoding,
    ) -> Self {
        Self {
            rle: create_int_rle(data_stream, buffer_size, &encoding),
            data_chunk: None,
            result_builder: arrow::array::Int32Builder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Int32Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        self.data_chunk = Some(
            self.rle
                .read(num_values)?
                .ok_or(OrcError::MalformedPresentOrDataStream)?,
        );
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index];
        self.result_builder.append_value(col_val);
    }

    fn append_null(&mut self) {
        self.result_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.result_builder.finish())
    }
}

pub struct Int64Reader<Input> {
    rle: IntRleDecoder<Input, i64>,
    data_chunk: Option<arrow::buffer::ScalarBuffer<i64>>,
    result_builder: arrow::array::Int64Builder,
}

impl<DataStream> Int64Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    pub fn new(
        data_stream: DataStream,
        buffer_size: usize,
        encoding: proto::ColumnEncoding,
    ) -> Self {
        Self {
            rle: create_int_rle(data_stream, buffer_size, &encoding),
            data_chunk: None,
            result_builder: arrow::array::Int64Builder::new(),
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Int64Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        self.data_chunk = Some(
            self.rle
                .read(num_values)?
                .ok_or(OrcError::MalformedPresentOrDataStream)?,
        );
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index];
        self.result_builder.append_value(col_val);
    }

    fn append_null(&mut self) {
        self.result_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.result_builder.finish())
    }
}

pub struct Float32Reader<Input> {
    /// Raw float data read from file.
    data: Input,
    data_chunk: bytes::Bytes,
    result_builder: arrow::array::Float32Builder,
}

impl<DataStream> Float32Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    pub fn new(data_stream: DataStream) -> Self {
        Self {
            data: data_stream,
            data_chunk: bytes::Bytes::new(),
            result_builder: arrow::array::Float32Builder::new(),
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
        let mut float_bytes = [0u8; 4];
        let len = float_bytes.len();
        let start_pos = index * len;
        float_bytes.copy_from_slice(&self.data_chunk[start_pos..start_pos + len]);
        self.result_builder
            .append_value(f32::from_le_bytes(float_bytes));
    }

    fn append_null(&mut self) {
        self.result_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.result_builder.finish())
    }
}

pub struct Float64Reader<Input> {
    /// Raw float data read from file.
    data: Input,
    data_chunk: bytes::Bytes,
    array_builder: arrow::array::Float64Builder,
}

impl<DataStream> Float64Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    pub fn new(data_stream: DataStream) -> Self {
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
        let mut float_bytes = [0u8; 8];
        let len = float_bytes.len();
        let start_pos = index * len;
        float_bytes.copy_from_slice(&self.data_chunk[start_pos..start_pos + len]);
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
