use std::num;
use std::sync::Arc;

use crate::encoding::rle::BooleanRleDecoder;
use crate::{io_utils, OrcError};

use super::ColumnProcessor;

pub struct BooleanReader<DataStream> {
    /// Decoded data of boolean column. Boolean column values encoded using byte RLE
    /// where each N'th bit is a boolean value of N'th row in column.
    /// Boolean values packed into bytes instead of using 1 byte per boolean value.
    rle: BooleanRleDecoder<DataStream>,
    /// Data chunk buffer. Contain values which are not a NULL.
    data_chunk: Option<arrow::buffer::BooleanBuffer>,
    /// Builder for a data array which will be returned to the user.
    array_builder: Option<arrow::array::BooleanBuilder>,
}

impl<'a, DataStream: io_utils::BufRead + 'a> BooleanReader<DataStream> {
    pub fn new(data_stream: DataStream, buffer_size: usize) -> BooleanReader<DataStream> {
        Self {
            rle: BooleanRleDecoder::new(data_stream, buffer_size),
            data_chunk: None,
            array_builder: None,
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for BooleanReader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        self.data_chunk = Some(
            self.rle
                .read(num_values)?
                .ok_or(OrcError::MalformedPresentOrDataStream)?,
        );
        self.array_builder = Some(arrow::array::BooleanBuilder::with_capacity(num_values));
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data.value(index);
        self.array_builder.as_mut().unwrap().append_value(col_val);
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
