use crate::proto;
use bytes::{Buf, Bytes, BytesMut};
use std::cmp;
use std::io::{BufReader, Read, Result};

pub struct ZstdOptions {
    pub dictionary: Bytes,
}

impl Default for ZstdOptions {
    fn default() -> Self {
        Self {
            dictionary: Default::default(),
        }
    }
}

pub struct CompressionFactory {
    zstd_opts: ZstdOptions,
}

impl Default for CompressionFactory {
    fn default() -> Self {
        Self {
            zstd_opts: Default::default(),
        }
    }
}

impl CompressionFactory {
    pub fn new(zstd_opts: ZstdOptions) -> Self {
        Self { zstd_opts }
    }

    pub fn decoder(
        &self,
        compressed_stream: impl Read,
        compression_type: proto::CompressionKind,
    ) -> Result<Box<dyn Read>> {
        match compression_type {
            proto::CompressionKind::Snappy => {
                Ok(Box::new(snap::read::FrameDecoder::new(compressed_stream)))
            }
            proto::CompressionKind::Zstd => {
                let decoder = if !self.zstd_opts.dictionary.is_empty() {
                    zstd::Decoder::with_dictionary(
                        BufReader::new(compressed_stream),
                        &self.zstd_opts.dictionary,
                    )
                } else {
                    zstd::Decoder::new(compressed_stream)
                };
                decoder.map(|decoder| {
                    let reader: Box<dyn Read> = Box::new(decoder);
                    reader
                })
            }
            proto::CompressionKind::Lz4 => Ok(Box::new(lz4_flex::frame::FrameDecoder::new(
                compressed_stream,
            ))),
            proto::CompressionKind::Zlib => {
                Ok(Box::new(flate2::read::ZlibDecoder::new(compressed_stream)))
            }
            proto::CompressionKind::None => Ok(Box::new(compressed_stream)),
            _ => panic!("Compression type '{:?}' is not supported", compression_type),
        }
    }
}

trait BlockCodec {
    fn decompress(&self, input: &mut dyn Buf) -> Result<Bytes>;
}

struct DecompressionStream<BaseStream: Read, Codec: BlockCodec> {
    compressed_stream: BaseStream,
    /// End of stream indicator
    eos: bool,
    codec: Codec,
    block_size: usize,
    /// Buffer is used for all read operations from the underlying stream.
    /// Allows to track how many bytes already decompressed.
    current_block: BytesMut,
    /// Chunk of decompressed data which is not consumed yet.
    /// if `None`, then current block is not compressed and
    /// data should be read from `current_block`.
    decompressed_chunk: Bytes,
}

impl<BaseStream: Read, Codec: BlockCodec> DecompressionStream<BaseStream, Codec> {
    fn new(codec: Codec, compressed: BaseStream, block_size: u64) -> Self {
        let block_size = block_size as usize;
        let current_block: BytesMut = BytesMut::with_capacity(block_size);
        Self {
            compressed_stream: compressed,
            eos: false,
            codec,
            block_size,
            current_block,
            decompressed_chunk: Bytes::new(),
        }
    }

    /// Method read next block from the stream and decompress it if required.
    /// Returns `false` if no more data in the stream.
    fn decompress_next_block(&mut self) -> Result<bool> {
        debug_assert!(
            self.decompressed_chunk.is_empty(),
            "Decompressed chunk is not consumed yet and we are trying to decompress the next!"
        );

        if self.eos {
            return Ok(false);
        }

        // Drop the previous buffer to ensure that we can reuse current block buffer without reallocation.
        // Decompressed chunk can point to subset of current block if chunk was not compressed originally.
        // This link should be dropped before we will reuse the current block buffer to read the next block.
        // Otherwise, block buffer will allocate a new internal byte array and will copy remaining data to it.
        self.decompressed_chunk = Bytes::new();

        let mut read_from_stream = true;
        let mut header: Option<(usize, bool)> = None;
        // we have pending uncompressed data in the current block
        if self.current_block.has_remaining() {
            if let h @ Some((compressed_size, _)) = self.decode_block_header() {
                // Request more data from the stream: compressed size of the next chunk
                // is greater than we currently have in the current block buffer.
                // Otherwise, if we have enough data in the current buffer, we will skip stream read.
                read_from_stream = compressed_size > self.current_block.remaining();
                header = h;
            }
        }

        // need more data from the underlying stream to read entire block
        if read_from_stream {
            // Try to reuse current buffer:
            //  - no remaining bytes left: `reserve` should reuse existing underlying buffer
            // because buffer size at least as block size.
            //  - we have remaining bytes: `reserve` will expand internal buffer if needed and
            // copy remaining bytes to the beginning of the buffer.
            self.current_block
                .reserve(self.block_size + self.current_block.remaining());
            let bytes_read = self.compressed_stream.read(&mut self.current_block)?;
            if bytes_read == 0 {
                // end of stream reached
                self.eos = true;
                return Ok(false);
            }

            unsafe {
                self.current_block.set_len(bytes_read);
            }
        }

        // We didn't decode header from existing block buffer, try again with the new one
        if header.is_none() {
            header = self.decode_block_header();
        }

        if let Some((compressed_size, is_compressed)) = header {
            if is_compressed {
                let mut compressed_chunk = self.current_block.copy_to_bytes(compressed_size);
                self.decompressed_chunk = self.codec.decompress(&mut compressed_chunk)?;
            } else {
                // `split_to` creates a new link to shared buffer of current block.
                // This can prevent from reusing the existing block buffer at the next stream read.
                // To make reuse to happen, decompressed chunk is released before current block is reused.
                self.decompressed_chunk = self.current_block.split_to(compressed_size).freeze();
            }
        } else {
            // TODO: no more data in the stream to read the header.
            // We need something like a 'backup' method to return bytes to the stream
            // if the don't needed by this stream.
            panic!("Unexpected stream end");
        }

        Ok(true)
    }

    /// Decode header which consist of:
    /// - block size
    /// - flag which is indicate that this block is compressed or not.
    ///
    /// Returns `None` if not enough data in current block to read the header.
    fn decode_block_header(&mut self) -> Option<(usize, bool)> {
        if self.current_block.remaining() < 3 {
            return None;
        }

        let mut header = [0; 4];
        // Least significant bit of first byte is indicate that data is compressed.
        // Next 7 bit + 2 bytes represent the block size.
        self.current_block.copy_to_slice(&mut header[..3]);
        let is_compressed = header[0] & 1 == 1;
        // skip LSB with compression indicator
        let block_len = u32::from_le_bytes(header) as usize >> 1;
        Some((block_len, is_compressed))
    }
}

impl<BaseStream: Read, Codec: BlockCodec> Read for DecompressionStream<BaseStream, Codec> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut remaining = buf.len();
        if remaining == 0 {
            return Ok(0);
        }

        loop {
            if self.decompressed_chunk.has_remaining() {
                let to_copy = cmp::min(self.decompressed_chunk.remaining(), remaining);
                self.decompressed_chunk.copy_to_slice(&mut buf[..to_copy]);
                remaining -= to_copy;
            }

            // We read entire decompressed chunk before and still need more data
            if remaining > 0 && self.decompress_next_block()? {
                continue;
            }
            break;
        }

        return Ok(buf.len() - remaining);
    }
}
