use crate::io_utils::UninitBytesMut;
use crate::proto;
use bytes::{Buf, Bytes};
use flate2::Status;
use std::cmp;
use std::io::{Error, Read, Result};

pub fn new_decompress_stream<'stream>(
    input: &'stream mut dyn Read,
    codec: &'stream mut dyn BlockCodec,
    compressed_block_size: u64,
) -> impl Read + 'stream {
    DecompressionStream::new(codec, input, compressed_block_size)
}

pub struct CompressionRegistry {
    snappy_codec: Box<dyn BlockCodec>,
    zstd_codec: Box<dyn BlockCodec>,
    lz4_codec: Box<dyn BlockCodec>,
    zlib_codec: Box<dyn BlockCodec>,
}

impl Default for CompressionRegistry {
    fn default() -> Self {
        CompressionRegistry::new()
    }
}

impl CompressionRegistry {
    pub fn new() -> Self {
        Self {
            snappy_codec: Box::new(SnappyCodec {}),
            zstd_codec: Box::new(ZstdCodec {}),
            lz4_codec: Box::new(Lz4Codec {}),
            zlib_codec: Box::new(ZlibCodec::new()),
        }
    }

    /// Overrides Snappy codec.
    pub fn with_snappy_codec(&mut self, new_codec: Box<dyn BlockCodec>) {
        self.snappy_codec = new_codec;
    }

    /// Overrides Zstd codec.
    pub fn with_zstd_codec(&mut self, new_codec: Box<dyn BlockCodec>) {
        self.zstd_codec = new_codec;
    }

    /// Overrides Lz4 codec.
    pub fn with_lz4_codec(&mut self, new_codec: Box<dyn BlockCodec>) {
        self.lz4_codec = new_codec;
    }

    /// Overrides Zlib codec.
    pub fn with_zlib_codec(&mut self, new_codec: Box<dyn BlockCodec>) {
        self.zlib_codec = new_codec;
    }

    pub fn codec(
        &mut self,
        compression_type: proto::CompressionKind,
    ) -> Result<&mut dyn BlockCodec> {
        match compression_type {
            proto::CompressionKind::Snappy => Ok(self.snappy_codec.as_mut()),
            proto::CompressionKind::Zstd => Ok(self.zstd_codec.as_mut()),
            proto::CompressionKind::Lz4 => Ok(self.lz4_codec.as_mut()),
            proto::CompressionKind::Zlib => Ok(self.zlib_codec.as_mut()),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Compression type '{compression_type:?}' is not supported"),
            )),
        }
    }
}

pub trait BlockCodec: Send + Sync {
    fn decompress(&mut self, input: &[u8], max_output_len: usize) -> Result<Bytes>;
}

#[derive(Debug, Default)]
pub struct ZstdCodec;

impl BlockCodec for ZstdCodec {
    fn decompress(&mut self, input: &[u8], max_output_len: usize) -> Result<Bytes> {
        Ok(zstd::bulk::decompress(input, max_output_len)?.into())
    }
}

#[derive(Debug, Default)]
pub struct SnappyCodec;

impl BlockCodec for SnappyCodec {
    fn decompress(&mut self, input: &[u8], _max_output_len: usize) -> Result<Bytes> {
        let out_size = snap::raw::decompress_len(input)?;
        let mut output = UninitBytesMut::new(out_size);
        output.write_from(|write_into| snap::raw::Decoder::new().decompress(input, write_into))?;

        Ok(output.freeze())
    }
}

#[derive(Debug, Default)]
pub struct Lz4Codec;

impl BlockCodec for Lz4Codec {
    fn decompress(&mut self, input: &[u8], max_output_len: usize) -> Result<Bytes> {
        let mut output = UninitBytesMut::new(max_output_len);
        let actual_len = output.write_from(|write_into| {
            lz4_flex::block::decompress_into(input, write_into).map_err(|err| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
            })
        })?;
        Ok(output.freeze())
    }
}

#[derive(Debug)]
pub struct ZlibCodec {
    decoder: flate2::Decompress,
}

impl Default for ZlibCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl ZlibCodec {
    pub fn new() -> Self {
        Self {
            decoder: flate2::Decompress::new(false),
        }
    }
}

impl BlockCodec for ZlibCodec {
    fn decompress(&mut self, input: &[u8], max_output_len: usize) -> Result<Bytes> {
        self.decoder.reset(false);
        let mut output = UninitBytesMut::new(max_output_len);
        output.write_from(
            |write_into| -> std::result::Result<usize, flate2::DecompressError> {
                let bytes_before = self.decoder.total_out();
                let status =
                    self.decoder
                        .decompress(input, write_into, flate2::FlushDecompress::Finish)?;
                debug_assert_eq!(status, Status::StreamEnd);
                Ok((self.decoder.total_out() - bytes_before) as usize)
            },
        )?;
        Ok(output.freeze())
    }
}

struct DecompressionStream<'codec, 'stream> {
    compressed_stream: &'stream mut dyn Read,
    /// End of stream indicator
    eos: bool,
    codec: &'codec mut dyn BlockCodec,
    block_size: usize,
    /// Buffer is used for all read operations from the underlying stream.
    /// Allows to track how many bytes already decompressed.
    current_block: UninitBytesMut,
    /// Chunk of decompressed data which is not consumed yet.
    /// if `None`, then current block is not compressed and
    /// data should be read from `current_block`.
    decompressed_chunk: Bytes,
}

const HEADER_SIZE: usize = 4;

impl<'codec, 'stream> DecompressionStream<'codec, 'stream> {
    fn new(
        codec: &'codec mut dyn BlockCodec,
        compressed: &'stream mut dyn Read,
        block_size: u64,
    ) -> Self {
        let block_size = block_size as usize;
        let current_block = UninitBytesMut::new(block_size);
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
        // have more compressed data in the current block
        if self.current_block.has_remaining() {
            if let h @ Some((compressed_size, _)) = self.decode_block_header() {
                // Request more data from the stream: compressed size of the next chunk
                // is greater than we currently have in the current block buffer.
                // Otherwise, if we have enough data in the current buffer, we will
                // skip read from the underlying stream.
                read_from_stream = compressed_size > self.current_block.remaining();
                header = h;
            }
        }

        // need more data from the underlying stream to read entire block
        if read_from_stream && self.read_from_stream(self.block_size + HEADER_SIZE)? == 0 {
            // no more data in underlying stream
            return Ok(false);
        }

        // We didn't decode header from existing block buffer,
        // try again with the new one(expanded using more data
        // from underlying stream).
        if header.is_none() {
            header = self.decode_block_header();
        }

        if let Some((block_size, is_compressed)) = header {
            if is_compressed {
                debug_assert!(
                    block_size <= self.current_block.len(),
                    "Compressed block size in header is greater than expected max compressed block size: \
                    header value={block_size}, expected={}",
                    self.current_block.len()
                );
                let compressed_chunk = self.current_block.copy_to_bytes(block_size);
                self.decompressed_chunk =
                    self.codec.decompress(&compressed_chunk, self.block_size)?;
            } else {
                // uncompressed block spans more than 'block_size' bytes, need more data from the underlying stream
                if block_size > self.current_block.len() {
                    // read missing bytes of current uncompressed block from the stream
                    self.read_from_stream(block_size - self.current_block.len())?;

                    if self.current_block.len() < block_size {
                        return Err(Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "Uncompressed block size is {block_size}, but only {} bytes available \
                                from the ORC file stream. Looks like data is corrupted.",
                                self.current_block.len()
                            ),
                        ));
                    }
                }

                // `split_to` creates a new link to shared buffer of current block.
                // This can prevent from reusing the existing block buffer at the next stream read.
                // To make reuse to happen, decompressed chunk is released before current block is reused.
                self.decompressed_chunk = self.current_block.split_to(block_size).freeze();
            }
        } else {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Current block with compressed data contains {} bytes. \
                    Not enough data to read compressed block header from underlying data stream. \
                    Looks like data is corrupted.",
                    self.current_block.remaining()
                ),
            ));
        }

        Ok(true)
    }

    fn read_from_stream(&mut self, read_bytes: usize) -> Result<usize> {
        self.current_block.ensure_capacity(read_bytes);

        let bytes_read = self
            .current_block
            .write_from(|write_into| self.compressed_stream.read(write_into))?;

        // end of stream reached
        self.eos = (bytes_read == 0);

        Ok(bytes_read)
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

        let mut header = [0; HEADER_SIZE];
        // Least significant bit of first byte is indicate that data is compressed.
        // Next 7 bit + 2 bytes represent the block size.
        self.current_block.copy_to_slice(&mut header[..3]);
        let is_compressed = header[0] & 1 == 0;
        // skip LSB with compression indicator
        let block_len = u32::from_le_bytes(header) as usize >> 1;
        Some((block_len, is_compressed))
    }
}

impl<'codec, 'stream> Read for DecompressionStream<'codec, 'stream> {
    fn read(&mut self, output: &mut [u8]) -> Result<usize> {
        let mut remaining = output.len();
        if remaining == 0 {
            return Ok(0);
        }

        loop {
            if self.decompressed_chunk.has_remaining() {
                let to_copy = cmp::min(self.decompressed_chunk.remaining(), remaining);
                self.decompressed_chunk
                    .copy_to_slice(&mut output[..to_copy]);
                remaining -= to_copy;
            }

            // We read all existing decompressed data and need more
            if remaining > 0 && self.decompress_next_block()? {
                continue;
            }
            break;
        }

        Ok(output.len() - remaining)
    }
}
