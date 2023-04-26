use std::cell::Cell;
use std::ops::Deref;
use std::sync::Arc;

use crate::{proto, OrcError};

use super::{ColumnProcessor, ColumnReader};

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
