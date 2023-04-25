use prost::Message;

use crate::column_reader;
use crate::compression;
use crate::io_utils::PositionalRead;
use crate::proto;
use crate::proto::stream;
use crate::source::OrcFile;

use self::index::StripeIndexSet;

pub struct StripeMetadata {
    pub data_size: u64,
    pub index_size: u64,
    pub num_rows: u64,
    pub col_stats: Option<Vec<proto::ColumnStatistics>>,
}

pub struct StripeReader<'a> {
    orc_file: &'a dyn OrcFile,
    compression: &'a compression::Compression,
    stripe_meta: proto::StripeInformation,
    stripe_footer: proto::StripeFooter,
    indexes: Option<index::StripeIndexSet>,
    file_schema: arrow::datatypes::SchemaRef,
    // Schema for record batch returned by this reader. Used to filter out unnecessary columns.
    out_schema: arrow::datatypes::SchemaRef,
    col_readers: Vec<Box<dyn column_reader::ColumnReader + 'a>>,
}

impl<'a> StripeReader<'a> {
    pub(crate) fn new(
        stripe: proto::StripeInformation,
        file_schema: arrow::datatypes::SchemaRef,
        orc_file: &'a dyn OrcFile,
        reader_factory: &'a compression::Compression,
    ) -> Self {
        StripeReader {
            orc_file,
            compression: reader_factory,
            stripe_meta: stripe,
            stripe_footer: Default::default(),
            file_schema: file_schema.clone(),
            indexes: None,
            out_schema: file_schema.clone(),
            col_readers: Vec::with_capacity(file_schema.fields().len()),
        }
    }

    pub fn read(
        &mut self,
        batch_size: usize,
    ) -> crate::Result<Option<arrow::record_batch::RecordBatch>> {
        if self.col_readers.is_empty() {
            self.init()?;
        }

        let mut arrays = Vec::with_capacity(self.col_readers.len());
        for col_reader in &mut self.col_readers {
            if let Some(array) = col_reader.read(batch_size)? {
                arrays.push(array);
            }
        }

        // All column readers have no more data, all stripe data consumed.
        if arrays.is_empty() {
            return Ok(None);
        }

        // File corrupted, some column have more rows than another.
        if arrays.len() != self.col_readers.len() {
            return Err(crate::OrcError::ColumnLenNotEqual(
                self.stripe_meta.clone(),
                self.stripe_footer.clone(),
            ));
        }

        arrow::record_batch::RecordBatch::try_new(self.out_schema.clone(), arrays)
            .map(Some)
            .map_err(|e| e.into())
    }

    pub fn init(&mut self) -> crate::Result<()> {
        let mut file_reader = self.orc_file.positional_reader()?;
        // read the stripe footer
        let footer_offset = self.stripe_meta.offset()
            + self.stripe_meta.index_length()
            + self.stripe_meta.data_length();
        let footer_buf = file_reader.read_exact_at(
            footer_offset,
            self.stripe_meta.footer_length() as usize,
            "Stripe footer",
        )?;

        self.stripe_footer = proto::StripeFooter::decode(footer_buf)?;
        // validate streams
        let stripe_end_offset = self.stripe_meta.offset()
            + self.stripe_meta.index_length()
            + self.stripe_meta.data_length();
        for stream in &self.stripe_footer.streams {
            if self.stripe_meta.offset() + stream.length() > stripe_end_offset {
                return Err(crate::OrcError::MalformedStream(
                    self.stripe_meta.clone(),
                    stream.clone(),
                ));
            }
        }

        self.indexes = Some(self.read_index(file_reader.as_mut(), &self.stripe_footer)?);
        // footer.writer_timezone.map(|tz| );

        for column in self.out_schema.fields() {
            self.col_readers.push(column_reader::create_reader(
                column,
                self.orc_file,
                &self.stripe_footer,
                &self.stripe_meta,
                self.compression,
            )?);
        }

        Ok(())
    }

    fn read_index(
        &self,
        file_reader: &mut dyn PositionalRead,
        footer: &proto::StripeFooter,
    ) -> std::io::Result<StripeIndexSet> {
        let mut index_set = index::StripeIndexSet::new(self.file_schema.fields.len());
        // Read file offset where streams are starting
        let mut offset = self.stripe_meta.offset();
        for stream in &footer.streams {
            match stream.kind() {
                stream::Kind::RowIndex => {
                    let buffer = file_reader.read_exact_at(
                        offset,
                        stream.length() as usize,
                        "Stripe index stream",
                    )?;

                    let index_buffer = self.compression.decompress_buffer(buffer)?;
                    let col_id = stream.column() as usize;
                    index_set.add_row_index(col_id, proto::RowIndex::decode(index_buffer)?);
                }
                stream::Kind::BloomFilterUtf8 => {
                    let buffer = file_reader.read_exact_at(
                        offset,
                        stream.length() as usize,
                        "Stripe index stream",
                    )?;

                    let index_buffer = self.compression.decompress_buffer(buffer)?;
                    let col_id = stream.column() as usize;
                    let bloom_filter = proto::BloomFilterIndex::decode(index_buffer)?;
                    index_set.add_bloom_index(col_id, bloom_filter, &footer.columns[col_id]);
                }
                _ => {}
            }
            offset += stream.length();
        }

        Ok(index_set)
    }
}

mod index {
    use crate::proto;

    struct BloomIndex {
        filter: Vec<proto::BloomFilter>,
    }

    impl BloomIndex {
        fn new(mut filter: proto::BloomFilterIndex) -> Self {
            Self {
                filter: filter
                    .bloom_filter
                    .drain(..)
                    .filter(|bf| bf.num_hash_functions.is_some() && bf.utf8bitset.is_some())
                    .collect(),
            }
        }
    }

    pub(crate) struct StripeIndexSet {
        // Per column indexes. If there is no index for column, then `None`.
        row_index: Vec<Option<proto::RowIndex>>,
        bloom_index: Vec<Option<BloomIndex>>,
    }

    impl StripeIndexSet {
        pub fn new(expected_size: usize) -> Self {
            Self {
                row_index: (0..expected_size).map(|_| None).collect(),
                bloom_index: (0..expected_size).map(|_| None).collect(),
            }
        }

        pub fn add_row_index(&mut self, column_index: usize, index: proto::RowIndex) {
            self.row_index[column_index] = Some(index);
        }

        pub fn add_bloom_index(
            &mut self,
            column_index: usize,
            index: proto::BloomFilterIndex,
            encoding: &proto::ColumnEncoding,
        ) {
            // Make sure we don't use unknown encodings or original timestamp encodings
            if encoding.bloom_encoding.is_none() || encoding.bloom_encoding() != 1 {
                return;
            }
            self.bloom_index[column_index] = Some(BloomIndex::new(index));
        }
    }
}

#[cfg(test)]
mod tests {}
