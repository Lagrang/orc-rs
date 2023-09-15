use std::cell::Cell;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use arrow::datatypes::UnionFields;

use crate::encoding::rlev1::{ByteRleDecoder, IntRleDecoder};
use crate::{proto, OrcError};

use super::{create_int_rle, ColumnProcessor, ColumnReader};

pub struct StructReader<'a> {
    child_readers: Vec<Box<dyn ColumnReader + 'a>>,
    struct_fields: arrow::datatypes::Fields,
    footer: proto::StripeFooter,
    stripe_meta: proto::StripeInformation,
    col_chunks: Cell<Vec<(arrow::datatypes::FieldRef, arrow::array::ArrayRef)>>,
    validity_bitmap: Option<arrow::array::BooleanBufferBuilder>,
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
            validity_bitmap: None,
        }
    }
}

impl<'a> ColumnProcessor for StructReader<'a> {
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        let mut result: Vec<(arrow::datatypes::FieldRef, arrow::array::ArrayRef)> =
            Vec::with_capacity(self.struct_fields.len());
        for (i, field) in self.struct_fields.iter().enumerate() {
            let col_chunk = self.child_readers[i].read(num_values)?;
            if !result.is_empty() && col_chunk.is_none() {
                return Err(OrcError::ColumnLenNotEqual(
                    self.stripe_meta.clone(),
                    self.footer.clone(),
                ));
            }
            result.push((field.clone(), col_chunk.unwrap()));
        }
        self.col_chunks = Cell::new(result);
        self.validity_bitmap = Some(arrow::array::BooleanBufferBuilder::new(num_values));
        Ok(())
    }

    fn append_value(&mut self, _: usize) -> crate::Result<()> {
        self.validity_bitmap.as_mut().unwrap().append(true);
        Ok(())
    }

    fn append_null(&mut self) -> crate::Result<()> {
        self.validity_bitmap.as_mut().unwrap().append(false);
        Ok(())
    }

    fn complete(&mut self) -> crate::Result<arrow::array::ArrayRef> {
        Ok(Arc::new(arrow::array::StructArray::from((
            self.col_chunks.take(),
            self.validity_bitmap.take().unwrap().into(),
        ))))
    }
}

pub struct ListReader<'a, RleInput> {
    data_type: arrow::datatypes::DataType,
    data: Box<dyn ColumnReader + 'a>,
    length_rle: IntRleDecoder<RleInput, u64>,
    length_chunk: arrow::buffer::ScalarBuffer<u64>,
    validity_bitmap: Option<arrow::array::BooleanBufferBuilder>,
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
            length_rle: create_int_rle(length_stream, buffer_size, encoding),
            length_chunk: arrow::buffer::ScalarBuffer::from(Vec::new()),
            validity_bitmap: None,
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
        self.validity_bitmap = Some(arrow::array::BooleanBufferBuilder::new(num_values));
        Ok(())
    }

    fn append_value(&mut self, _: usize) -> crate::Result<()> {
        self.validity_bitmap.as_mut().unwrap().append(true);
        Ok(())
    }

    fn append_null(&mut self) -> crate::Result<()> {
        self.validity_bitmap.as_mut().unwrap().append(false);
        Ok(())
    }

    fn complete(&mut self) -> crate::Result<arrow::array::ArrayRef> {
        let total_size = self.length_chunk.iter().sum::<u64>() as usize;
        let array = self
            .data
            .read(total_size)?
            .ok_or(OrcError::MalformedPresentOrDataStream)?;

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
                .null_bit_buffer(Some(self.validity_bitmap.take().unwrap().into()))
                .build_unchecked()
        };
        Ok(Arc::new(arrow::array::ListArray::from(array_data)))
    }
}

pub struct UnionReader<'a, RleStream> {
    // Union schema
    fields: UnionFields,
    /// Readers for union fields(in order defined by the union schema).
    field_readers: Vec<Box<dyn ColumnReader + 'a>>,
    /// RLE data which contains sequence of union field IDs
    /// which indicates field of the next value found in ORC union stream.
    /// In our case, this is the same as type_id buffer(according to the Arrow Union spec).
    field_id_rle: ByteRleDecoder<RleStream>,
    field_id_chunk: arrow::buffer::Buffer,
}

impl<'a, RleStream> UnionReader<'a, RleStream>
where
    RleStream: std::io::Read,
{
    pub fn new(
        fields: UnionFields,
        fields_data: Vec<Box<dyn ColumnReader + 'a>>,
        field_id_stream: RleStream,
        buffer_size: usize,
    ) -> Self {
        Self {
            fields,
            field_readers: fields_data,
            field_id_rle: ByteRleDecoder::new(field_id_stream, buffer_size),
            field_id_chunk: arrow::buffer::Buffer::from(Vec::new()),
        }
    }
}

impl<'a, RleStream> ColumnProcessor for UnionReader<'a, RleStream>
where
    RleStream: std::io::Read,
{
    fn load_chunk(&mut self, num_values: usize) -> crate::Result<()> {
        self.field_id_chunk = self
            .field_id_rle
            .read(num_values)?
            .ok_or(OrcError::MalformedPresentOrDataStream)?;
        Ok(())
    }

    fn append_value(&mut self, index: usize) -> crate::Result<()> {
        Ok(())
    }

    fn append_null(&mut self) -> crate::Result<()> {
        // Currently, there is no support for nullable unions,
        // because they require modification of children arrays(
        // which will be chosen randomly because ORC doesn't provide
        // information about the field which contains NULL).
        Err(OrcError::TypeNotSupported(
            arrow::datatypes::DataType::Union(
                self.fields.clone(),
                arrow::datatypes::UnionMode::Dense,
            ),
        ))
    }

    fn complete(&mut self) -> crate::Result<arrow::array::ArrayRef> {
        // Per subtype value offsets: points to some entry in the associated
        // Arrow array which stores values for this subtype.
        let mut offsets: Vec<i32> = Vec::with_capacity(self.fields.len());
        // How many values we should read from particular column. Stores count per subtype.
        let mut counts: Vec<usize> = Vec::with_capacity(self.fields.len());
        counts.resize(self.fields.len(), 0);
        for id in self.field_id_chunk.iter() {
            let i = *id as usize;
            offsets[i] = counts[i] as i32; // offset is i32 according to Arrow union spec
            counts[i] += 1;
        }

        let fields: Vec<&arrow::datatypes::Field> =
            self.fields.iter().map(|(_, f)| f.deref()).collect();
        let mut fields_data: Vec<(arrow::datatypes::Field, arrow::array::ArrayRef)> =
            Vec::with_capacity(self.fields.len());
        for (i, batch_size) in counts.iter().enumerate() {
            fields_data.push((
                fields[i].clone(),
                self.field_readers[i]
                    .read(*batch_size)?
                    .ok_or(OrcError::MalformedUnion)?,
            ));
        }

        Ok(Arc::new(arrow::array::UnionArray::try_new(
            &self.fields.iter().map(|(i, _)| i).collect::<Vec<i8>>(),
            self.field_id_chunk.slice(0),
            Some(arrow::buffer::Buffer::from_vec(offsets)),
            fields_data,
        )?))
    }
}
