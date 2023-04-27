use std::sync::Arc;

use crate::encoding::rle::{ByteRleDecoder, IntRleDecoder};
use crate::encoding::UnsignedInteger;
use crate::{io_utils, proto, OrcError};

use super::{create_int_rle, ColumnProcessor};

pub struct Int8Reader<Input> {
    rle: ByteRleDecoder<Input>,
    /// Data chunk buffer. Contain values which are not a NULL.
    data_chunk: Option<arrow::buffer::Buffer>,
    /// Builder for a data array which will be returned to the user.
    result_builder: Option<arrow::array::Int8Builder>,
}

impl<DataStream> Int8Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    pub fn new(data_stream: DataStream, buffer_size: usize) -> Self {
        Self {
            rle: ByteRleDecoder::new(data_stream, buffer_size),
            data_chunk: None,
            result_builder: None,
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
        self.result_builder = Some(arrow::array::Int8Builder::with_capacity(num_values));
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index] as i8;
        self.result_builder.as_mut().unwrap().append_value(col_val);
        Ok(())
    }

    fn append_null(&mut self) -> crate::Result<()> {
        self.result_builder.as_mut().unwrap().append_null();
        Ok(())
    }

    fn complete(&mut self) -> crate::Result<arrow::array::ArrayRef> {
        Ok(Arc::new(self.result_builder.take().unwrap().finish()))
    }
}

pub struct Int16Reader<Input> {
    rle: IntRleDecoder<Input, i16>,
    data_chunk: Option<arrow::buffer::ScalarBuffer<i16>>,
    result_builder: Option<arrow::array::Int16Builder>,
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
            result_builder: None,
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
        self.result_builder = Some(arrow::array::Int16Builder::with_capacity(num_values));
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index];
        self.result_builder.as_mut().unwrap().append_value(col_val);
        Ok(())
    }

    fn append_null(&mut self) -> crate::Result<()> {
        self.result_builder.as_mut().unwrap().append_null();
        Ok(())
    }

    fn complete(&mut self) -> crate::Result<arrow::array::ArrayRef> {
        Ok(Arc::new(self.result_builder.take().unwrap().finish()))
    }
}

pub struct Int32Reader<Input> {
    rle: IntRleDecoder<Input, i32>,
    /// Data chunk buffer. Contain values which are not a NULL.
    data_chunk: Option<arrow::buffer::ScalarBuffer<i32>>,
    /// Builder for a data array which will be returned to the user.
    result_builder: Option<arrow::array::Int32Builder>,
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
            result_builder: None,
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
        self.result_builder = Some(arrow::array::Int32Builder::with_capacity(num_values));
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index];
        self.result_builder.as_mut().unwrap().append_value(col_val);
        Ok(())
    }

    fn append_null(&mut self) -> crate::Result<()> {
        self.result_builder.as_mut().unwrap().append_null();
        Ok(())
    }

    fn complete(&mut self) -> crate::Result<arrow::array::ArrayRef> {
        Ok(Arc::new(self.result_builder.take().unwrap().finish()))
    }
}

pub struct Int64Reader<Input> {
    rle: IntRleDecoder<Input, i64>,
    data_chunk: Option<arrow::buffer::ScalarBuffer<i64>>,
    result_builder: Option<arrow::array::Int64Builder>,
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
            result_builder: None,
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
        self.result_builder = Some(arrow::array::Int64Builder::with_capacity(num_values));
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data[index];
        self.result_builder.as_mut().unwrap().append_value(col_val);
        Ok(())
    }

    fn append_null(&mut self) -> crate::Result<()> {
        self.result_builder.as_mut().unwrap().append_null();
        Ok(())
    }

    fn complete(&mut self) -> crate::Result<arrow::array::ArrayRef> {
        Ok(Arc::new(self.result_builder.take().unwrap().finish()))
    }
}

pub struct Float32Reader<Input> {
    /// Raw float data read from file.
    data: Input,
    data_chunk: bytes::Bytes,
    result_builder: Option<arrow::array::Float32Builder>,
}

impl<DataStream> Float32Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    pub fn new(data_stream: DataStream) -> Self {
        Self {
            data: data_stream,
            data_chunk: bytes::Bytes::new(),
            result_builder: None,
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Float32Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        let mut buffer = bytes::BytesMut::with_capacity(num_values * 4);
        io_utils::BufRead::read(&mut self.data, &mut buffer)?;
        self.data_chunk = buffer.freeze();
        self.result_builder = Some(arrow::array::Float32Builder::with_capacity(num_values));
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        let mut float_bytes = [0u8; 4];
        let len = float_bytes.len();
        let start_pos = index * len;
        float_bytes.copy_from_slice(&self.data_chunk[start_pos..start_pos + len]);
        self.result_builder
            .as_mut()
            .unwrap()
            .append_value(f32::from_le_bytes(float_bytes));
        Ok(())
    }

    fn append_null(&mut self) -> crate::Result<()> {
        self.result_builder.as_mut().unwrap().append_null();
        Ok(())
    }

    fn complete(&mut self) -> crate::Result<arrow::array::ArrayRef> {
        Ok(Arc::new(self.result_builder.take().unwrap().finish()))
    }
}

pub struct Float64Reader<Input> {
    /// Raw float data read from file.
    data: Input,
    data_chunk: bytes::Bytes,
    array_builder: Option<arrow::array::Float64Builder>,
}

impl<DataStream> Float64Reader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    pub fn new(data_stream: DataStream) -> Self {
        Self {
            data: data_stream,
            data_chunk: bytes::Bytes::new(),
            array_builder: None,
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Float64Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        let mut buffer = bytes::BytesMut::with_capacity(num_values * 8);
        io_utils::BufRead::read(&mut self.data, &mut buffer)?;
        self.data_chunk = buffer.freeze();
        self.array_builder = Some(arrow::array::Float64Builder::with_capacity(num_values));
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        let mut float_bytes = [0u8; 8];
        let len = float_bytes.len();
        let start_pos = index * len;
        float_bytes.copy_from_slice(&self.data_chunk[start_pos..start_pos + len]);
        self.array_builder
            .as_mut()
            .unwrap()
            .append_value(f64::from_le_bytes(float_bytes));
        Ok(())
    }

    fn append_null(&mut self) -> crate::Result<()> {
        self.array_builder.as_mut().unwrap().append_null();
        Ok(())
    }

    fn complete(&mut self) -> crate::Result<arrow::array::ArrayRef> {
        Ok(Arc::new(self.array_builder.take().unwrap().finish()))
    }
}

pub struct Decimal128Reader<Input> {
    precision: u8,
    scale: u8,
    data_stream: std::io::BufReader<Input>,
    // Scale RLE is signed
    scale_rle: IntRleDecoder<Input, i8>,
    scale_chunk: arrow::buffer::ScalarBuffer<i8>,
    result_builder: Option<arrow::array::Decimal128Builder>,
}

impl<DataStream> Decimal128Reader<DataStream>
where
    DataStream: std::io::Read,
{
    pub fn new(
        precision: u8,
        scale: u8,
        data_stream: DataStream,
        scale_stream: DataStream,
        buffer_size: usize,
        encoding: &proto::ColumnEncoding,
    ) -> Self {
        Self {
            precision,
            scale,
            data_stream: std::io::BufReader::with_capacity(buffer_size, data_stream),
            scale_rle: create_int_rle(scale_stream, buffer_size, encoding),
            scale_chunk: arrow::buffer::ScalarBuffer::from(Vec::new()),
            result_builder: None,
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for Decimal128Reader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        self.scale_chunk = self
            .scale_rle
            .read(num_values)?
            .ok_or(OrcError::MalformedPresentOrDataStream)?;
        self.result_builder = Some(arrow::array::Decimal128Builder::with_capacity(num_values));
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        let mut value = u128::varint_decode(&mut self.data_stream)?
            .0
            .zigzag_decode();

        // Fix scaling, ported from C++ Decimal64ColumnReader.
        let val_scale = self.scale_chunk[index] as u8;
        let signed = self.scale as i32 - val_scale as i32;
        let scale_fix = signed.unsigned_abs();
        // const DECIMAL64_MAX_PRECISION: u8 = 18;
        // if scale_fix <= DECIMAL64_MAX_PRECISION as u32 && scale_fix != 0 {
        //     return Err(OrcError::InvalidDecimalScale(
        //         self.scale,
        //         val_scale,
        //         DECIMAL64_MAX_PRECISION,
        //     ));
        // }

        if signed > 0 {
            value *= 1i128.pow(scale_fix);
        } else {
            value /= 1i128.pow(scale_fix);
        }

        self.result_builder.as_mut().unwrap().append_value(value);
        Ok(())
    }

    fn append_null(&mut self) -> crate::Result<()> {
        self.result_builder.as_mut().unwrap().append_null();
        Ok(())
    }

    fn complete(&mut self) -> crate::Result<arrow::array::ArrayRef> {
        Ok(Arc::new(self.result_builder.take().unwrap().finish()))
    }
}
