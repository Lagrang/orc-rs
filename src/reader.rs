use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::{
    format,
    io::{Read, Seek},
};

use crate::compression::CompressionRegistry;
use crate::source::OrcSource;
use crate::stripe::{StripeInfo, StripeReader};
use crate::tail::{FileMetadataReader, FileTail, FileVersion};
use crate::Result;
use crate::{proto, OrcError};

use bytes::Bytes;
use prost::Message;

#[derive(Default)]
pub struct ReaderOptions {
    pub compression: Arc<CompressionRegistry>,
    //TODO: add custom allocator support, pub allocator: &'a dyn std::alloc::Allocator,
}

/// Creates new ORC file reader.
pub fn new_reader<T>(orc_file: Box<dyn OrcSource>, opts: ReaderOptions) -> Result<impl OrcReader>
where
    T: Read + Seek,
{
    OrcSourceReader::new(orc_file, opts)
}

/// ORC file reader interface.
///
/// Implementation can be obtained from [`new_reader`].
pub trait OrcReader {
    fn version(&self) -> FileVersion;
    fn num_rows(&self) -> u64;
    fn schema(&self) -> arrow::datatypes::SchemaRef;
    fn metadata(&self) -> HashMap<String, Bytes>;
    fn column_statistics(&self) -> HashMap<String, proto::ColumnStatistics>;
    fn stripes(&self) -> Vec<StripeInfo>;

    fn read_stripe(&self, stripe: usize) -> Result<StripeReader>;
}

struct OrcSourceReader {
    tail: FileTail,
    stripe_stats: Option<Vec<proto::StripeStatistics>>,
    orc_file: Box<dyn OrcSource>,
    compression: Arc<CompressionRegistry>,
}

impl OrcSourceReader {
    fn new(orc_file: Box<dyn OrcSource>, opts: ReaderOptions) -> Result<Self> {
        let mut tail_reader = FileMetadataReader::new(
            orc_file
                .reader()
                .map_err(|e| OrcError::IoError(e.kind(), e.to_string()))?,
            opts.compression.clone(),
        )?;

        let tail = tail_reader.read_tail()?;
        let metadata =
            tail_reader.read_metadata(&tail.postscript, tail.postscript.encoded_len())?;
        Ok(OrcSourceReader {
            tail,
            stripe_stats: metadata.map(|m| m.stripe_stats),
            orc_file,
            compression: opts.compression,
        })
    }
}

impl OrcReader for OrcSourceReader {
    /// Returns list with stripes metadata.
    ///
    /// Stripe's data can be accessed using [`OrcReader::read_stripe`] by passing
    /// a stripe index obtained from return value of this method.
    fn stripes(&self) -> Vec<StripeInfo> {
        self.tail
            .stripes
            .iter()
            .map(|stripe| StripeInfo {
                data_size: stripe.data_length(),
                index_size: stripe.index_length(),
                num_rows: stripe.number_of_rows(),
            })
            .collect()
    }

    /// Returns the ORC file schema.
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.tail.schema.clone()
    }

    /// ORC file format version.
    fn version(&self) -> FileVersion {
        self.tail.version
    }

    /// Total number of rows in ORC file.
    fn num_rows(&self) -> u64 {
        self.tail.row_count
    }

    /// Custom metadata contained in ORC file.
    fn metadata(&self) -> HashMap<String, Bytes> {
        self.tail.metadata.clone()
    }

    fn column_statistics(&self) -> HashMap<String, proto::ColumnStatistics> {
        let mut result = HashMap::with_capacity(self.tail.column_statistics.len());
        for (i, stat) in self.tail.column_statistics.iter().enumerate() {
            let prev = result.insert(self.tail.schema.field(i).name().clone(), stat.clone());
            debug_assert!(prev.is_none());
        }
        result
    }

    /// Returns stripe data reader.
    ///
    /// Index of stripe can be deduced from vector returned by [`OrcReader::stripes`].
    fn read_stripe(&self, stripe: usize) -> Result<StripeReader> {
        if stripe >= self.tail.stripes.len() {
            return Err(OrcError::InvalidStripeIndex(
                stripe,
                self.tail.stripes.len(),
            ));
        }

        Ok(StripeReader::new(
            self.tail.stripes[stripe].clone(),
            &self.tail,
            self.orc_file.as_ref(),
            &self.compression,
        ))
    }
}

#[cfg(test)]
mod tests {}
