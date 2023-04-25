pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/orc.proto.rs"));
}

mod encoding;
mod io_utils;
mod schema;
mod stripe;
mod tail;
#[cfg(test)]
mod test;

pub mod column_reader;
pub mod compression;
pub mod reader;
pub mod source;
pub use reader::new_reader;
use thiserror::Error;

type Result<T> = std::result::Result<T, OrcError>;

#[derive(Error, Debug)]
pub enum OrcError {
    #[error("IO error: kind={0}, message={1}")]
    IoError(std::io::ErrorKind, String),
    #[error("Stripe index {0} is out of bound ({1} stripe(s) in the file)")]
    InvalidStripeIndex(usize, usize),
    #[error("ORC file is too short, only {0} bytes")]
    UnexpectedEof(usize),
    #[error("Tail size({0} byte(s)) is greater than file size({0} byte(s))")]
    InvalidTail(u64, u64),
    #[error("Feature is not supported: {0}")]
    UnsupportedFeature(String),
    #[error("Corrupted protobuf message: {0}")]
    CorruptedProtobuf(String),
    #[error("{0}")]
    General(String),
    #[error("Malformed stream {1:?} in stripe {0:?}")]
    MalformedStream(proto::StripeInformation, proto::Stream),
    #[error("Malformed column metadata for column {0}. Footer {1:?}")]
    MalformedColumnStreams(u32, proto::StripeFooter),
    #[error("Malformed RLE block")]
    MalformedRleBlock,
    #[error("Column {1:?} doesn't contain a stream with kind {0:?}")]
    InvalidStreamKind(proto::stream::Kind, u32),
    #[error(
        "Arrow batch can't be created: some ORC file columns returned more data than others.
        Stripe footer: {0:?}, stripe info: {1:?}"
    )]
    ColumnLenNotEqual(proto::StripeInformation, proto::StripeFooter),
    #[error("Column PRESENT and DATA stream contains different number of rows.")]
    MalformedPresentOrDataStream,
}

impl From<std::io::Error> for OrcError {
    fn from(e: std::io::Error) -> Self {
        OrcError::IoError(e.kind(), e.to_string())
    }
}

impl From<prost::DecodeError> for OrcError {
    fn from(e: prost::DecodeError) -> Self {
        OrcError::CorruptedProtobuf(e.to_string())
    }
}

impl From<arrow::error::ArrowError> for OrcError {
    fn from(e: arrow::error::ArrowError) -> Self {
        OrcError::General(e.to_string())
    }
}

impl From<chrono_tz::ParseError> for OrcError {
    fn from(e: chrono_tz::ParseError) -> Self {
        OrcError::General(format!("Timezone parsing failed: {}", e))
    }
}
