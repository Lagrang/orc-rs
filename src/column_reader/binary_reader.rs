use std::io::Read;
use std::num;
use std::sync::Arc;

use crate::encoding::rle::IntRleDecoder;
use crate::{io_utils, proto, OrcError};

use super::{create_int_rle, ColumnProcessor};

pub struct BinaryReader<RleInput, Input> {
    length_rle: IntRleDecoder<RleInput, i64>,
    data: std::io::BufReader<Input>,
    buffers: Vec<bytes::Bytes>,
    result_builder: Option<arrow::array::BinaryBuilder>,
}

impl<RleStream, DataStream> BinaryReader<RleStream, DataStream>
where
    DataStream: std::io::Read,
    RleStream: io_utils::BufRead,
{
    pub fn new(
        data_stream: DataStream,
        length_stream: RleStream,
        buffer_size: usize,
        encoding: &proto::ColumnEncoding,
    ) -> Self {
        Self {
            length_rle: create_int_rle(length_stream, buffer_size, encoding),
            data: std::io::BufReader::with_capacity(buffer_size, data_stream),
            buffers: Vec::new(),
            result_builder: None,
        }
    }
}

impl<RleStream: io_utils::BufRead, DataStream: std::io::Read> ColumnProcessor
    for BinaryReader<RleStream, DataStream>
{
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        let sizes = self
            .length_rle
            .read(num_values)?
            .ok_or(OrcError::MalformedPresentOrDataStream)?;
        self.buffers = Vec::with_capacity(num_values);
        let mut total_size = 0;
        for size in &sizes {
            let size = *size as usize;
            let mut buffer = bytes::BytesMut::with_capacity(size);
            unsafe { buffer.set_len(buffer.capacity()) };
            self.data.read_exact(&mut buffer)?;
            self.buffers.push(buffer.freeze());
            total_size += size;
        }
        self.result_builder = Some(arrow::array::BinaryBuilder::with_capacity(
            num_values, total_size,
        ));
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        self.result_builder
            .as_mut()
            .unwrap()
            .append_value(&self.buffers[index]);
        self.buffers[index].clear();
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

pub struct StringReader<RleInput, Input> {
    length_rle: IntRleDecoder<RleInput, u64>,
    data: std::io::BufReader<Input>,
    strings: Vec<String>,
    result_builder: Option<arrow::array::StringBuilder>,
}

impl<RleStream, DataStream> StringReader<RleStream, DataStream>
where
    DataStream: std::io::Read,
    RleStream: io_utils::BufRead,
{
    pub fn new(
        data_stream: DataStream,
        length_stream: RleStream,
        buffer_size: usize,
        encoding: &proto::ColumnEncoding,
    ) -> Self {
        Self {
            length_rle: create_int_rle(length_stream, buffer_size, encoding),
            data: std::io::BufReader::with_capacity(buffer_size, data_stream),
            strings: Vec::new(),
            result_builder: None,
        }
    }
}

impl<RleStream: io_utils::BufRead, DataStream: std::io::Read> ColumnProcessor
    for StringReader<RleStream, DataStream>
{
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        let sizes = self
            .length_rle
            .read(num_values)?
            .ok_or(OrcError::MalformedPresentOrDataStream)?;
        self.strings = Vec::with_capacity(num_values);
        let mut total_size = 0;
        for size in &sizes {
            let size = *size as usize;
            let mut buffer = Vec::with_capacity(size);
            #[allow(clippy::uninit_vec)]
            unsafe {
                buffer.set_len(buffer.capacity())
            };
            self.data.read_exact(&mut buffer)?;
            self.strings
                .push(unsafe { String::from_utf8_unchecked(buffer) });
            total_size += size;
        }

        self.result_builder = Some(arrow::array::StringBuilder::with_capacity(
            num_values, total_size,
        ));
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        self.result_builder
            .as_mut()
            .unwrap()
            .append_value(&self.strings[index]);
        self.strings[index].clear();
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

pub struct StringDictionaryReader<RleInput> {
    dict_rle: IntRleDecoder<RleInput, u32>,
    buffer: arrow::buffer::ScalarBuffer<u32>,
    dict_values: arrow::array::StringArray,
    result_builder: Option<arrow::array::StringDictionaryBuilder<arrow::datatypes::UInt32Type>>,
}

impl<RleStream> StringDictionaryReader<RleStream>
where
    RleStream: io_utils::BufRead,
{
    pub fn new<DataStream: std::io::Read>(
        dict_idx_stream: RleStream,
        dict_data_stream: DataStream,
        length_stream: RleStream,
        buffer_size: usize,
        encoding: &proto::ColumnEncoding,
    ) -> crate::Result<Self> {
        let dict_size = encoding.dictionary_size() as usize;
        let mut len_rle: IntRleDecoder<RleStream, u32> =
            create_int_rle(length_stream, buffer_size, encoding);
        let sizes = len_rle
            .read(dict_size)?
            .ok_or(OrcError::MalformedDictionaryLengthStream)?;

        if dict_size != sizes.len() {
            return Err(OrcError::MalformedDictionaryLengthStream);
        }

        let mut dict_values = Vec::with_capacity(dict_size);
        let mut dict_data_stream = std::io::BufReader::with_capacity(buffer_size, dict_data_stream);
        for size in &sizes {
            let mut buffer = Vec::with_capacity(*size as usize);
            #[allow(clippy::uninit_vec)]
            unsafe {
                buffer.set_len(buffer.capacity())
            };
            dict_data_stream.read_exact(&mut buffer)?;
            dict_values.push(unsafe { String::from_utf8_unchecked(buffer) });
        }

        Ok(Self {
            dict_rle: create_int_rle(dict_idx_stream, buffer_size, encoding),
            buffer: arrow::buffer::ScalarBuffer::from(Vec::new()),
            dict_values: arrow::array::StringArray::from(dict_values),
            result_builder: None,
        })
    }
}

impl<RleStream: io_utils::BufRead> ColumnProcessor for StringDictionaryReader<RleStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        self.buffer = self
            .dict_rle
            .read(num_values)?
            .ok_or(OrcError::MalformedPresentOrDataStream)?;
        self.result_builder= Some(arrow::array::StringDictionaryBuilder::new();
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        // FIXME: blind access by index can cause panics if ORC file is corrupted.
        let value_index = self.buffer[index] as usize;
        let value = self.dict_values.value(value_index);
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
