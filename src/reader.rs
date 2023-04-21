use std::collections::HashMap;
use std::io::{Read, Seek};

use crate::compression::{self, CompressionRegistry};
use crate::source::OrcFile;
use crate::stripe::{StripeMetadata, StripeReader};
use crate::tail::{FileMetadataReader, FileTail, FileVersion};
use crate::Result;
use crate::{proto, OrcError};

use bytes::Bytes;
use prost::Message;

#[derive(Default)]
pub struct ReaderOptions {
    pub compression_codecs: CompressionRegistry,
    //TODO: add custom allocator support, pub allocator: &'a dyn std::alloc::Allocator,
}

/// Creates new ORC file reader.
pub fn new_reader<T>(orc_file: Box<dyn OrcFile>, opts: ReaderOptions) -> Result<impl Reader>
where
    T: Read + Seek,
{
    ReaderImpl::new(orc_file, opts)
}

/// ORC file reader interface.
///
/// Implementation can be obtained from [`new_reader`].
pub trait Reader {
    /// ORC file format version.
    fn version(&self) -> FileVersion;

    /// Total number of rows in ORC file.
    fn num_rows(&self) -> u64;

    /// Returns the ORC file schema.
    fn schema(&self) -> arrow::datatypes::SchemaRef;

    /// Custom metadata contained in ORC file.
    fn metadata(&self) -> HashMap<String, Bytes>;

    /// Returns column statistics for entire ORC file.
    fn column_statistics(&self) -> HashMap<String, proto::ColumnStatistics>;

    /// Returns list with stripes metadata.
    ///
    /// Stripe's data can be accessed using [`OrcReader::read_stripe`] by passing
    /// a stripe index obtained from return value of this method.
    fn stripes(&self) -> Vec<StripeMetadata>;

    /// Return stripe metadata by index.
    fn stripe_metadata(&self, stripe: usize) -> StripeMetadata;

    /// Return count of stripes in ORC file.
    fn stripes_count(&self) -> usize;

    /// Returns stripe data reader.
    ///
    /// Index of stripe can be deduced from vector returned by [`OrcReader::stripes`].
    fn read_stripe(&self, stripe: usize) -> Result<StripeReader>;
}

struct ReaderImpl {
    tail: FileTail,
    stripe_stats: Option<Vec<proto::StripeStatistics>>,
    orc_file: Box<dyn OrcFile>,
    compression: compression::Compression,
}

impl ReaderImpl {
    fn new(orc_file: Box<dyn OrcFile>, opts: ReaderOptions) -> Result<Self> {
        let mut tail_reader = FileMetadataReader::new(
            orc_file
                .positional_reader()
                .map_err(|e| OrcError::IoError(e.kind(), e.to_string()))?,
        )?;

        let (tail, compression) = tail_reader.read_tail(opts.compression_codecs)?;
        let metadata =
            tail_reader.read_metadata(&tail.postscript, tail.postscript.encoded_len())?;

        Ok(ReaderImpl {
            tail,
            stripe_stats: metadata.map(|m| m.stripe_stats),
            orc_file,
            compression,
        })
    }
}

impl Reader for ReaderImpl {
    fn stripes(&self) -> Vec<StripeMetadata> {
        (0..self.tail.stripes.len())
            .map(|i| self.stripe_metadata(i))
            .collect()
    }

    #[inline]
    fn stripe_metadata(&self, stripe: usize) -> StripeMetadata {
        let stripe = &self.tail.stripes[stripe];
        StripeMetadata {
            data_size: stripe.data_length(),
            index_size: stripe.index_length(),
            num_rows: stripe.number_of_rows(),
            col_stats: self.stripe_stats.as_ref().map(|stats| {
                stats
                    .iter()
                    .flat_map(|col_stats| col_stats.col_stats.clone())
                    .collect()
            }),
        }
    }

    fn stripes_count(&self) -> usize {
        self.tail.stripes.len()
    }

    #[inline]
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.tail.schema.clone()
    }

    #[inline]
    fn version(&self) -> FileVersion {
        self.tail.version
    }

    #[inline]
    fn num_rows(&self) -> u64 {
        self.tail.row_count
    }

    #[inline]
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

    fn read_stripe(&self, stripe: usize) -> Result<StripeReader> {
        if stripe >= self.tail.stripes.len() {
            return Err(OrcError::InvalidStripeIndex(
                stripe,
                self.tail.stripes.len(),
            ));
        }

        Ok(StripeReader::new(
            self.tail.stripes[stripe].clone(),
            self.tail.schema.clone(),
            self.orc_file.as_ref(),
            &self.compression,
        ))
    }
}

#[cfg(test)]
mod tests {}
