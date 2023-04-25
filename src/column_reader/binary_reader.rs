use std::io::Read;
use std::sync::Arc;

use crate::encoding::rle::IntRleDecoder;
use crate::{io_utils, proto, OrcError};

use super::{create_int_rle, ColumnProcessor};

pub struct BinaryReader<RleInput, Input> {
    length_rle: IntRleDecoder<RleInput, i64>,
    data: std::io::BufReader<Input>,
    buffers: Vec<bytes::Bytes>,
    result_builder: arrow::array::BinaryBuilder,
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
            result_builder: arrow::array::BinaryBuilder::new(),
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
        for size in &sizes {
            let mut buffer = bytes::BytesMut::with_capacity(*size as usize);
            unsafe { buffer.set_len(buffer.capacity()) };
            self.data.read_exact(&mut buffer)?;
            self.buffers.push(buffer.freeze());
        }
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        self.result_builder.append_value(&self.buffers[index]);
        self.buffers[index].clear();
    }

    fn append_null(&mut self) {
        self.result_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.result_builder.finish())
    }
}

pub struct StringReader<RleInput, Input> {
    length_rle: IntRleDecoder<RleInput, i64>,
    data: std::io::BufReader<Input>,
    strings: Vec<String>,
    result_builder: arrow::array::StringBuilder,
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
            result_builder: arrow::array::StringBuilder::new(),
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
        for size in &sizes {
            let mut buffer = Vec::with_capacity(*size as usize);
            #[allow(clippy::uninit_vec)]
            unsafe {
                buffer.set_len(buffer.capacity())
            };
            self.data.read_exact(&mut buffer)?;
            self.strings
                .push(unsafe { String::from_utf8_unchecked(buffer) });
        }
        Ok(())
    }

    fn append_value(&mut self, index: usize) {
        self.result_builder.append_value(&self.strings[index]);
        self.strings[index].clear();
    }

    fn append_null(&mut self) {
        self.result_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.result_builder.finish())
    }
}
