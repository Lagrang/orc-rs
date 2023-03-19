use bytes::Buf;
use bytes::BytesMut;
use prost::Message;

use crate::compression::decompress;
use crate::compression::new_decompress_stream;
use crate::compression::CompressionRegistry;
use crate::io_utils;
use crate::proto;
use crate::proto::stream;
use crate::proto::Stream;
use crate::tail::FileTail;

pub struct StripeInfo {
    pub data_size: u64,
    pub index_size: u64,
    pub num_rows: u64,
}

pub struct StripeReader<'a> {
    tail: &'a FileTail,
    stripe_meta: proto::StripeInformation,
    file_reader: Box<dyn io_utils::PositionalReader>,
    is_initialized: bool,
    compression: &'a CompressionRegistry,
    // Per column indexes. If there is no index for column, then `None`.
    row_index: Vec<Option<proto::RowIndex>>,
    bloom_index: Vec<Option<proto::BloomFilterIndex>>,
}

impl<'a> StripeReader<'a> {
    pub fn new(
        stripe: proto::StripeInformation,
        tail: &'a FileTail,
        file_reader: Box<dyn io_utils::PositionalReader>,
        compression: &'a CompressionRegistry,
    ) -> Self {
        StripeReader {
            stripe_meta: stripe,
            tail,
            file_reader,
            is_initialized: false,
            compression,
            row_index: (0..tail.schema.fields().len()).map(|_| None).collect(),
            bloom_index: (0..tail.schema.fields().len()).map(|_| None).collect(),
        }
    }

    pub fn read(&mut self) -> std::io::Result<arrow::record_batch::RecordBatch> {
        if !self.is_initialized {
            self.init()?;
        }
        Ok(())
    }

    pub fn init(&mut self) -> std::io::Result<()> {
        // read the stripe footer
        let footer_offset = self.stripe_meta.offset()
            + self.stripe_meta.index_length()
            + self.stripe_meta.data_length();
        let footer_buf = self.file_reader.read_exact_at(
            footer_offset,
            self.stripe_meta.footer_length() as usize,
            "Stripe footer",
        )?;

        let footer = proto::StripeFooter::decode(footer_buf)?;
        let codec = self.compression.codec(self.tail.postscript.compression())?;

        let mut offset = self.stripe_meta.offset();
        for stream in &footer.streams {
            match stream.kind() {
                stream::Kind::BloomFilterUtf8 => {
                    let buffer = self.file_reader.read_exact_at(
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
                    deserialize(bloom_filter.bloom_filter);
                }
                stream::Kind::RowIndex | stream::Kind::BloomFilterUtf8 => {
                    let buffer = self.file_reader.read_exact_at(
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
                    self.row_index[colId] = Some(proto::RowIndex::decode(index_buffer)?);
                }
                _ => {}
            }
            offset += stream.length();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
