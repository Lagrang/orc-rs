use std::sync::Arc;

use chrono::TimeZone;

use crate::encoding::IntRleDecoder;
use crate::io_utils::{self};
use crate::{proto, OrcError};

use super::{create_int_rle, ColumnProcessor};

pub struct TimestampReader<Input> {
    time_offset: chrono::Duration,
    seconds_rle: IntRleDecoder<8, 10, Input, i64>,
    nanos_rle: IntRleDecoder<8, 10, Input, u64>,
    seconds_chunk: Option<arrow::buffer::ScalarBuffer<i64>>,
    nanos_chunk: Option<arrow::buffer::ScalarBuffer<u64>>,
    result_builder: Option<arrow::array::TimestampNanosecondBuilder>,
}

impl<DataStream> TimestampReader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    pub fn new(
        seconds_stream: DataStream,
        nanos_stream: DataStream,
        buffer_size: usize,
        encoding: &proto::ColumnEncoding,
    ) -> Self {
        Self {
            // Each timestamp in ORC file stored as number of seconds since 2015-01-01
            time_offset: chrono_tz::UTC
                .with_ymd_and_hms(2015, 1, 1, 0, 0, 0)
                .unwrap()
                - chrono_tz::UTC.timestamp_opt(0, 0).unwrap(),
            seconds_rle: create_int_rle(seconds_stream, buffer_size, encoding),
            nanos_rle: create_int_rle(nanos_stream, buffer_size, encoding),
            seconds_chunk: None,
            nanos_chunk: None,
            result_builder: None,
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for TimestampReader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        self.seconds_chunk = self.seconds_rle.read(num_values)?;
        self.nanos_chunk = self.nanos_rle.read(num_values)?;
        self.result_builder = Some(arrow::array::TimestampNanosecondBuilder::with_capacity(
            num_values,
        ));
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        let mut seconds = self.seconds_chunk.as_ref().unwrap()[index];
        let mut nanos = self.nanos_chunk.as_ref().unwrap()[index];
        // 3 LS bits stores count of decimal zeros which are truncated from the actual value.
        // Usually nanoseconds part contains values with many trailing 0 digits, e.g. 10000000.
        // If number has at least two trailing 0s, they all will be truncated and encoded
        // in 3 least significant bis.
        let mult = nanos & 0x07;
        nanos >>= 3;
        if mult != 0 {
            for _ in 0..=mult {
                nanos *= 10;
            }
        }

        // Ported from C++ version, I don't know why this needed,
        // commit message says it aligns C++ reader with Java version.
        if seconds < 0 && nanos > 999999 {
            seconds -= 1;
        }
        let ts = seconds * 10i64.pow(9) + nanos as i64;
        self.result_builder
            .as_mut()
            .unwrap()
            .append_value(self.time_offset.num_nanoseconds().unwrap() + ts);
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

pub struct DateReader<Input> {
    rle: IntRleDecoder<8, 10, Input, i64>,
    data_chunk: Option<arrow::buffer::ScalarBuffer<i64>>,
    result_builder: Option<arrow::array::Date64Builder>,
}

impl<DataStream> DateReader<DataStream>
where
    DataStream: io_utils::BufRead,
{
    pub fn new(
        data_stream: DataStream,
        buffer_size: usize,
        encoding: &proto::ColumnEncoding,
    ) -> Self {
        Self {
            rle: create_int_rle(data_stream, buffer_size, encoding),
            data_chunk: None,
            result_builder: None,
        }
    }
}

impl<DataStream: io_utils::BufRead> ColumnProcessor for DateReader<DataStream> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        self.data_chunk = Some(
            self.rle
                .read(num_values)?
                .ok_or(OrcError::MalformedPresentOrDataStream)?,
        );
        self.result_builder = Some(arrow::array::Date64Builder::with_capacity(num_values));
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
