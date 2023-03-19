use crate::io_utils;
use crate::proto;

pub struct StripeInfo {
    pub data_size: u64,
    pub index_size: u64,
    pub num_rows: u64,
}

pub struct StripeReader {
    stripe_info: proto::StripeInformation,
    file_reader: Box<dyn io_utils::PositionalReader>,
}

impl StripeReader {
    pub fn new(
        stripe: proto::StripeInformation,
        file_reader: Box<dyn io_utils::PositionalReader>,
    ) -> Self {
        StripeReader {
            stripe_info: stripe,
            file_reader,
        }
    }
}

#[cfg(test)]
mod tests {}
