use std::cell::Cell;
use std::ops::Deref;
use std::sync::Arc;

use crate::encoding::rle::IntRleDecoder;
use crate::{proto, OrcError};

use super::{create_int_rle, ColumnProcessor, ColumnReader};

pub struct StructReader<'a> {
    child_readers: Vec<Box<dyn ColumnReader + 'a>>,
    struct_fields: arrow::datatypes::Fields,
    footer: proto::StripeFooter,
    stripe_meta: proto::StripeInformation,
    col_chunks: Cell<Vec<(arrow::datatypes::Field, arrow::array::ArrayRef)>>,
    validity_bitmap: arrow::array::BooleanBufferBuilder,
}

impl<'a> StructReader<'a> {
    pub fn new(
        struct_fields: arrow::datatypes::Fields,
        child_readers: Vec<Box<dyn ColumnReader + 'a>>,
        footer: &proto::StripeFooter,
        stripe_meta: &proto::StripeInformation,
    ) -> Self {
        Self {
            child_readers,
            struct_fields,
            col_chunks: Cell::default(),
            footer: footer.clone(),
            stripe_meta: stripe_meta.clone(),
            validity_bitmap: arrow::array::BooleanBufferBuilder::new(1024),
        }
    }
}

impl<'a> ColumnProcessor for StructReader<'a> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        let mut result: Vec<(arrow::datatypes::Field, arrow::array::ArrayRef)> =
            Vec::with_capacity(self.struct_fields.len());
        for (i, field) in self.struct_fields.iter().enumerate() {
            let f = field.deref().clone();
            let col_chunk = self.child_readers[i].read(num_values)?;
            if !result.is_empty() && col_chunk.is_none() {
                return Err(OrcError::ColumnLenNotEqual(
                    self.stripe_meta.clone(),
                    self.footer.clone(),
                ));
            }
            result.push((f, col_chunk.unwrap()));
        }
        self.col_chunks = Cell::new(result);
        Ok(())
    }

    fn append_value(&mut self, _: usize) -> crate::Result<()> {
        self.validity_bitmap.append(true);
        Ok(())
    }

    fn append_null(&mut self) {
        self.validity_bitmap.append(false);
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        Arc::new(arrow::array::StructArray::from((
            self.col_chunks.take(),
            self.validity_bitmap.finish(),
        )))
    }
}

pub struct ListReader<'a, RleInput> {
    data_type: arrow::datatypes::DataType,
    data: Box<dyn ColumnReader + 'a>,
    data_buffer: Option<arrow::array::ArrayRef>,
    length_rle: IntRleDecoder<RleInput, u64>,
    length_chunk: arrow::buffer::ScalarBuffer<u64>,
    validity_bitmap: arrow::array::BooleanBufferBuilder,
}

impl<'a, RleStream> ListReader<'a, RleStream>
where
    RleStream: std::io::Read,
{
    pub fn new(
        data_type: arrow::datatypes::DataType,
        data: Box<dyn ColumnReader + 'a>,
        length_stream: RleStream,
        buffer_size: usize,
        encoding: &proto::ColumnEncoding,
    ) -> Self {
        Self {
            data_type,
            data,
            data_buffer: None,
            length_rle: create_int_rle(length_stream, buffer_size, encoding),
            length_chunk: arrow::buffer::ScalarBuffer::from(Vec::new()),
            validity_bitmap: arrow::array::BooleanBufferBuilder::new(buffer_size),
        }
    }
}

impl<'a, RleStream> ColumnProcessor for ListReader<'a, RleStream>
where
    RleStream: std::io::Read,
{
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        self.length_chunk = self
            .length_rle
            .read(num_values)?
            .ok_or(OrcError::MalformedPresentOrDataStream)?;

        let total_size = self.length_chunk.iter().sum::<u64>() as usize;
        self.data_buffer = Some(
            self.data
                .read(total_size)?
                .ok_or(OrcError::MalformedPresentOrDataStream)?,
        );
        Ok(())
    }

    fn append_value(&mut self, _: usize) -> crate::Result<()> {
        self.validity_bitmap.append(true);
        Ok(())
    }

    fn append_null(&mut self) {
        self.validity_bitmap.append(false);
    }

    fn complete(&mut self) -> arrow::array::ArrayRef {
        let array = self.data_buffer.take().unwrap();
        let child_data = array.to_data();
        let mut offsets = Vec::with_capacity(self.length_chunk.len());
        let mut offset = 0i32;
        for len in self.length_chunk.iter() {
            offsets.push(offset);
            offset += *len as i32;
        }
        offsets.push(offset);
        let offset_buffer = arrow::buffer::ScalarBuffer::<i32>::from(offsets);

        let array_data = unsafe {
            arrow::array::ArrayData::builder(self.data_type.clone())
                .len(self.length_chunk.len())
                .add_buffer(offset_buffer.into_inner())
                .add_child_data(child_data)
                .null_bit_buffer(Some(self.validity_bitmap.finish()))
                .build_unchecked()
        };
        Arc::new(arrow::array::ListArray::from(array_data))
    }
}
