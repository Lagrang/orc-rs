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
    array_builder: arrow::array::BooleanBuilder,
}

impl<'a, DataStream: io_utils::BufRead + 'a> BooleanReader<DataStream> {
    pub fn new(data_stream: DataStream) -> BooleanReader<DataStream> {
        Self {
            rle: BooleanRleDecoder::new(data_stream, 4 * 1024),
            data_chunk: None,
            array_builder: arrow::array::BooleanBuilder::new(),
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
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        let data = self.data_chunk.as_ref().unwrap();
        let col_val = data.value(index);
        self.array_builder.append_value(col_val);
        Ok(())
    }

    fn append_null(&mut self) {
        self.array_builder.append_null();
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.array_builder.finish())
    }
}
