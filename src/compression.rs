use crate::io_utils::UninitBytesMut;
use crate::proto;
use bytes::{Buf, Bytes};
use flate2::Status;
use std::cmp;
use std::io::{Error, Read, Result};

/// Create the stream which will decompress the data from compressed data blocks.
pub(crate) fn new_decompress_stream<'codec, Input: Read + 'codec>(
    input: Input,
    codec: &'codec mut dyn BlockCodec,
    compressed_block_size: u64,
) -> impl Read + 'codec {
    DecompressionStream::new(codec, input, compressed_block_size)
}

/// Decompress data from the input stream into in-memory buffer.
pub(crate) fn decompress<'codec, Input: Read + 'codec>(
    input: Input,
    codec: &'codec dyn BlockCodec,
    compressed_block_size: u64,
) -> std::io::Result<Bytes> {
    let mut stream = DecompressionStream::new(codec, input, compressed_block_size);
    let mut res_vec = Vec::new();
    stream.read_to_end(&mut res_vec)?;
    Ok(Bytes::from(res_vec))
}

/// Compression registry contains references to codecs for all supported compression formats.
/// Can be used to provide a custom block codec implementation.
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
    /// Creates new compression registry with default codecs implementation.
    pub fn new() -> Self {
        Self {
            snappy_codec: Box::new(SnappyCodec {}),
            zstd_codec: Box::new(ZstdCodec {}),
            lz4_codec: Box::new(Lz4Codec {}),
            zlib_codec: Box::new(ZlibCodec {}),
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

    /// Find the codec for passed compression type.
    ///
    /// Returns an error if compression is not supported.
    pub fn codec(&self, compression_type: proto::CompressionKind) -> Result<&dyn BlockCodec> {
        match compression_type {
            proto::CompressionKind::Snappy => Ok(self.snappy_codec.as_ref()),
            proto::CompressionKind::Zstd => Ok(self.zstd_codec.as_ref()),
            proto::CompressionKind::Lz4 => Ok(self.lz4_codec.as_ref()),
            proto::CompressionKind::Zlib => Ok(self.zlib_codec.as_ref()),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Compression type '{compression_type:?}' is not supported"),
            )),
        }
    }
}

pub trait BlockCodec: Send + Sync {
    fn decompress(&self, input: &[u8], max_output_len: usize) -> Result<Bytes>;
}

#[derive(Debug, Default)]
pub struct ZstdCodec;

impl BlockCodec for ZstdCodec {
    fn decompress(&self, input: &[u8], max_output_len: usize) -> Result<Bytes> {
        Ok(zstd::bulk::decompress(input, max_output_len)?.into())
    }
}

#[derive(Debug, Default)]
pub struct SnappyCodec;

impl BlockCodec for SnappyCodec {
    fn decompress(&self, input: &[u8], _max_output_len: usize) -> Result<Bytes> {
        let out_size = snap::raw::decompress_len(input)?;
        let mut output = UninitBytesMut::new(out_size);
        output.write_from(|write_into| snap::raw::Decoder::new().decompress(input, write_into))?;

        Ok(output.freeze())
    }
}

#[derive(Debug, Default)]
pub struct Lz4Codec;

impl BlockCodec for Lz4Codec {
    fn decompress(&self, input: &[u8], max_output_len: usize) -> Result<Bytes> {
        let mut output = UninitBytesMut::new(max_output_len);
        output.write_from(|write_into| {
            lz4_flex::block::decompress_into(input, write_into).map_err(|err| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
            })
        })?;
        Ok(output.freeze())
    }
}

#[derive(Debug, Default)]
pub struct ZlibCodec {}

impl BlockCodec for ZlibCodec {
    fn decompress(&self, input: &[u8], max_output_len: usize) -> Result<Bytes> {
        let mut decoder = flate2::Decompress::new(false);
        decoder.reset(false);
        let mut output = UninitBytesMut::new(max_output_len);
        output.write_from(
            |write_into| -> std::result::Result<usize, flate2::DecompressError> {
                let bytes_before = decoder.total_out();
                let status =
                    decoder.decompress(input, write_into, flate2::FlushDecompress::Finish)?;
                debug_assert_eq!(status, Status::StreamEnd);
                Ok((decoder.total_out() - bytes_before) as usize)
            },
        )?;
        Ok(output.freeze())
    }
}

/// Decompression stream allows to decompress compressed blocks of ORC file.
/// This stream reads and decompresses data contained in the input stream.
///
/// Each compressed block has following format:
/// - header(3 bytes). Format described in [`DecompressionStream::decode_block_header`].
/// - data inside block
///
/// Various compression types supported though [`BlockCodec`].
struct DecompressionStream<'codec, Input: Read> {
    // Input data stream with data blocks(compressed or not).
    compressed_stream: Input,
    /// End of stream indicator
    end_of_stream: bool,
    codec: &'codec dyn BlockCodec,
    // Max size of compressed block in ORC file. Used to preallocate decompression buffers.
    block_size: usize,
    /// Buffer is used for all read operations from the underlying stream.
    /// Allows to track how many bytes already decompressed.
    current_block: UninitBytesMut,
    /// Chunk of decompressed data which is not consumed yet.
    /// if `None`, then current block is not compressed and
    /// data should be read from `current_block`.
    decompressed_chunk: Bytes,
}

const HEADER_SIZE: usize = 3;

impl<'codec, Input: Read> DecompressionStream<'codec, Input> {
    fn new(codec: &'codec dyn BlockCodec, compressed: Input, block_size: u64) -> Self {
        let block_size = block_size as usize;
        let current_block = UninitBytesMut::new(block_size);
        Self {
            compressed_stream: compressed,
            end_of_stream: false,
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

        if self.end_of_stream {
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

        // We didn't decode header from existing block buffer(not enough bytes in buffer to read header),
        // try again with the new one (extended using more data from underlying stream).
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

    /// Read next N bytes from underlying input stream.
    fn read_from_stream(&mut self, read_bytes: usize) -> Result<usize> {
        self.current_block.ensure_capacity(read_bytes);

        let bytes_read = self
            .current_block
            .write_from(|write_into| self.compressed_stream.read(write_into))?;

        // end of stream reached
        self.end_of_stream = bytes_read == 0;

        Ok(bytes_read)
    }

    /// Decode header which consist of:
    /// - block size
    /// - flag which is indicate that this block is compressed or not.
    ///
    /// Header format(24 bit size): [23 bits: length of data block, 1 bit: is data compressed ot not]
    ///
    /// Returns `None` if not enough data in current block to read the header.
    fn decode_block_header(&mut self) -> Option<(usize, bool)> {
        if self.current_block.remaining() < 3 {
            return None;
        }

        let mut header = [0; HEADER_SIZE + 1];
        // Least significant bit of first byte is indicate that data is compressed.
        // Next 7 bit + 2 bytes represent the block size.
        self.current_block.copy_to_slice(&mut header[..3]);
        let is_compressed = header[0] & 1 == 0;
        // skip LSB with compression indicator
        let block_len = u32::from_le_bytes(header) as usize >> 1;
        Some((block_len, is_compressed))
    }
}

impl<'codec, Input: Read> Read for DecompressionStream<'codec, Input> {
    fn read(&mut self, output: &mut [u8]) -> Result<usize> {
        let max_size = output.len();
        let mut remaining = max_size;
        if remaining == 0 {
            return Ok(0);
        }

        let mut out_buf = output;
        loop {
            if self.decompressed_chunk.has_remaining() {
                let to_copy = cmp::min(self.decompressed_chunk.remaining(), remaining);
                self.decompressed_chunk
                    .copy_to_slice(&mut out_buf[..to_copy]);
                out_buf = &mut out_buf[to_copy..];
                remaining -= to_copy;
            }

            // We read all existing decompressed data and need more
            if remaining > 0 && self.decompress_next_block()? {
                continue;
            }
            break;
        }

        Ok(max_size - remaining)
    }
}
