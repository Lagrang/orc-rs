use bytes::BytesMut;
use prost::Message;

use crate::io_utils;
use crate::proto;

pub struct StripeInfo {
    pub data_size: u64,
    pub index_size: u64,
    pub num_rows: u64,
}

pub struct StripeReader {
    stripe_info: proto::StripeInformation,
    stripe_stats: Vec<proto::StripeStatistics>,
    file_reader: Box<dyn io_utils::PositionalReader>,
}

impl StripeReader {
    pub fn new(
        stripe: proto::StripeInformation,
        stripe_stats: Vec<proto::StripeStatistics>,
        file_reader: Box<dyn io_utils::PositionalReader>,
    ) -> Self {
        StripeReader {
            stripe_info: stripe,
            stripe_stats,
            file_reader,
        }
    }

    // pub fn read(&mut self) -> std::io::Result<arrow::record_batch::RecordBatch> {
    //     // read the stripe footer
    //     let footer_offset = self.stripe_info.offset()
    //         + self.stripe_info.index_length()
    //         + self.stripe_info.data_length();
    //     let footer_size = self.stripe_info.footer_length() as usize;
    //     let footer_buf = BytesMut::with_capacity(footer_size);
    //     let actual_len = self.file_reader.read_at(footer_offset, &mut footer_buf)?;
    //     if actual_len != footer_size {
    //         return Err(std::io::Error::new(
    //             std::io::ErrorKind::InvalidData,
    //             format!(
    //                 "Declared stripe footer size is {}, but actual length is {}",
    //                 footer_size, actual_len
    //             ),
    //         ));
    //     }

    //     let footer = proto::StripeFooter::decode(footer_buf)?;
    //     for stream in &footer.streams {
    //         stream.kind
    //     }

    //     Ok(())
    // }
}

#[cfg(test)]
mod tests {}
