use arrow::datatypes::DataType;

use crate::io_utils::PositionalReader;
use crate::{proto, schema};

pub trait ColumnReader {
    // Read next column chunk.
    //
    // Returns `None` if no more chunk left.
    fn read(&mut self) -> Option<arrow::array::ArrayRef>;
}

// pub fn create_reader(
//     file_reader: Box<dyn PositionalReader>,
//     footer: &proto::StripeFooter,
//     column: &arrow::datatypes::Field,
// ) -> crate::Result<Box<dyn ColumnReader>> {
//     let col_id = schema::get_column_id(column)?;
//     let streams = footer
//         .streams
//         .iter()
//         .filter(|s| s.column() == col_id)
//         .map(|s| s.clone())
//         .collect();
//     match column {
//         DataType::Boolean => {
//             if streams.len() != 2 {
//                 return Err(crate::OrcError::MalformedColumnStreams(
//                     col_id,
//                     footer.clone(),
//                 ));
//             }
//             BooleanReader::new(file_reader, streams)
//         }
//         _ => panic!(""),
//     }
// }

pub struct BooleanReader {
    file_reader: Box<dyn PositionalReader>,
}

impl BooleanReader {
    fn new(
        file_reader: Box<dyn PositionalReader>,
        data_stream: proto::Stream,
        null_stream: proto::Stream,
    ) -> Box<dyn ColumnReader> {
        Box::new(Self { file_reader })
    }
}

impl ColumnReader for BooleanReader {
    fn read(&mut self) -> Option<arrow::array::ArrayRef> {
        todo!()
    }
}

pub struct IntReader {}
