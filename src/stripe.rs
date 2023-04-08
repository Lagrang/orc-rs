use bytes::Buf;
use prost::Message;

use crate::column_reader;
use crate::compression;
use crate::compression::decompress;
use crate::compression::CompressionRegistry;
use crate::io_utils;
use crate::io_utils::PositionalReader;
use crate::proto;
use crate::proto::stream;
use crate::schema::get_column_id;
use crate::source::OrcSource;
use crate::tail::FileTail;

use self::index::StripeIndexSet;

pub struct StripeInfo {
    pub data_size: u64,
    pub index_size: u64,
    pub num_rows: u64,
}

pub struct StripeReader<'a> {
    tail: &'a FileTail,
    stripe_meta: proto::StripeInformation,
    orc_file: &'a dyn OrcSource,
    is_initialized: bool,
    compression: &'a CompressionRegistry,
    indexes: Option<index::StripeIndexSet>,
    // Schema for record batch returned by this reader. Used to filter out unnecessary columns.
    out_schema: arrow::datatypes::SchemaRef,
    col_readers: Vec<Box<dyn column_reader::ColumnReader>>,
}

impl<'a> StripeReader<'a> {
    pub fn new(
        stripe: proto::StripeInformation,
        tail: &'a FileTail,
        orc_file: &'a dyn OrcSource,
        compression: &'a CompressionRegistry,
    ) -> Self {
        StripeReader {
            stripe_meta: stripe,
            tail,
            orc_file,
            is_initialized: false,
            compression,
            indexes: None,
            out_schema: tail.schema.clone(),
            col_readers: Vec::with_capacity(tail.schema.fields().len()),
        }
    }

    pub fn read(&mut self) -> crate::Result<arrow::record_batch::RecordBatch> {
        if !self.is_initialized {
            self.init()?;
        }

        Ok(arrow::record_batch::RecordBatch::new_empty(
            self.out_schema.clone(),
        ))
    }

    pub fn init(&mut self) -> crate::Result<()> {
        let mut file_reader = self.orc_file.reader()?;
        // read the stripe footer
        let footer_offset = self.stripe_meta.offset()
            + self.stripe_meta.index_length()
            + self.stripe_meta.data_length();
        let footer_buf = file_reader.read_exact_at(
            footer_offset,
            self.stripe_meta.footer_length() as usize,
            "Stripe footer",
        )?;

        let footer = proto::StripeFooter::decode(footer_buf)?;
        // validate streams
        let stripeEndOffset = self.stripe_meta.offset()
            + self.stripe_meta.index_length()
            + self.stripe_meta.data_length();
        for stream in &footer.streams {
            if self.stripe_meta.offset() + stream.length() > stripeEndOffset {
                return Err(crate::OrcError::MalformedStream(
                    self.stripe_meta.clone(),
                    stream.clone(),
                ));
            }
        }

        let codec = self.compression.codec(self.tail.postscript.compression())?;
        self.indexes = Some(self.read_index(file_reader.as_mut(), &footer, codec)?);
        // footer.writer_timezone.map(|tz| );

        for column in self.out_schema.fields() {
            self.col_readers.push(column_reader::create_reader(
                self.orc_file.reader()?,
                &footer,
                column,
            )?);
        }

        Ok(())
    }

    fn read_index(
        &self,
        file_reader: &mut dyn PositionalReader,
        footer: &proto::StripeFooter,
        codec: &dyn compression::BlockCodec,
    ) -> std::io::Result<StripeIndexSet> {
        let mut index_set = index::StripeIndexSet::new(self.tail.schema.fields.len());
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

                    let index_buffer = decompress(
                        buffer.reader(),
                        codec,
                        self.tail.postscript.compression_block_size(),
                    )?;

                    let colId = stream.column() as usize;
                    index_set.add_row_index(colId, proto::RowIndex::decode(index_buffer)?);
                }
                stream::Kind::BloomFilterUtf8 => {
                    let buffer = file_reader.read_exact_at(
                        offset,
                        stream.length() as usize,
                        "Stripe index stream",
                    )?;

                    let index_buffer = decompress(
                        buffer.reader(),
                        codec,
                        self.tail.postscript.compression_block_size(),
                    )?;

                    let colId = stream.column() as usize;
                    let bloom_filter = proto::BloomFilterIndex::decode(index_buffer)?;
                    index_set.add_bloom_index(colId, bloom_filter, &footer.columns[colId]);
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
