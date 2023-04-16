use std::io::Read;

use arrow::datatypes::DataType;

use crate::source::OrcFile;
use crate::{compression, proto, schema, stripe, OrcError, Result};

pub trait ColumnReader {
    // Read next column chunk.
    //
    // Returns `None` if no more chunk left.
    fn read(&mut self) -> Option<arrow::array::ArrayRef>;
}

pub(crate) fn create_reader<'a>(
    column: &arrow::datatypes::Field,
    orc_file: &'a dyn OrcFile,
    footer: &proto::StripeFooter,
    stripe_meta: &proto::StripeInformation,
    compression: &'a compression::Compression,
) -> crate::Result<Box<dyn ColumnReader + 'a>> {
    let col_id = schema::get_column_id(column)?;
    let null_stream = open_stream_reader(
        col_id,
        proto::stream::Kind::Present,
        footer,
        stripe_meta,
        orc_file,
        compression,
    )?;
    let data_stream = open_stream_reader(
        col_id,
        proto::stream::Kind::Data,
        footer,
        stripe_meta,
        orc_file,
        compression,
    )?;
    match column.data_type() {
        DataType::Boolean => Ok(BooleanReader::boxed(data_stream, null_stream)),
        _ => panic!(""),
    }
}

fn open_stream_reader<'a>(
    col_id: u32,
    stream_kind: proto::stream::Kind,
    footer: &proto::StripeFooter,
    stripe_meta: &proto::StripeInformation,
    orc_file: &dyn OrcFile,
    compression: &'a compression::Compression,
) -> Result<impl Read + 'a> {
    let mut offset = stripe_meta.offset();
    for s in &footer.streams {
        if s.column() == col_id && s.kind() == stream_kind {
            return compression
                .new_reader(orc_file, offset, s.length())
                .map_err(|e| OrcError::IoError(e.kind(), e.to_string()));
        }
        offset += s.length();
    }

    Err(OrcError::InvalidStreamKind(stream_kind, col_id))
}

pub struct BooleanReader<NullStream, DataStream> {
    null: NullStream,
    data: DataStream,
}

impl<'a, NullStream: Read + 'a, DataStream: Read + 'a> BooleanReader<NullStream, DataStream> {
    fn boxed(data_stream: DataStream, null_stream: NullStream) -> Box<dyn ColumnReader + 'a> {
        Box::new(Self {
            data: data_stream,
            null: null_stream,
        })
    }
}

impl<NullStream: Read, DataStream: Read> ColumnReader for BooleanReader<NullStream, DataStream> {
    fn read(&mut self) -> Option<arrow::array::ArrayRef> {
        todo!()
    }
}

pub struct IntReader {}
