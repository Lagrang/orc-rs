use crate::io_utils::{BufRead, RangeRead, WriteBuffer};
use crate::{proto, source};
use bytes::{Buf, Bytes, BytesMut};
use flate2::Status;
use std::cmp;
use std::io::{Error, Read, Result};

/// Factory structure which allows to create ORC file readers which
/// will decompress the data before returning it to the user.
pub(crate) struct Compression {
    codec: Option<Box<dyn BlockCodec>>,
    block_size: u64,
}

impl Compression {
    pub fn new(
        mut registry: CompressionRegistry,
        kind: proto::CompressionKind,
        compression_block_size: u64,
    ) -> Result<Self> {
        if kind != proto::CompressionKind::None {
            Ok(Self {
                codec: Some(registry.extract_codec(kind)?),
                block_size: compression_block_size,
            })
        } else {
            Ok(Self {
                codec: None,
                block_size: compression_block_size,
            })
        }
    }

    /// Create new reader positioned at some offset in ORC file
    /// and count of bytes which can be read by returned reader.
    ///
    /// Offset must point to the start of the compressed block.
    /// Compression format defined in [`DecompressionStream`] docs.
    pub fn new_reader<'s>(
        &'s self,
        source: &dyn source::OrcFile,
        start_offset: u64,
        len: u64,
    ) -> std::io::Result<Box<dyn BufRead + 's>> {
        return if let Some(codec) = &self.codec {
            let reader = source.positional_reader()?;
            let range_reader = RangeRead::new(reader, start_offset..len)?;
            Ok(Box::new(DecompressionStream::new(
                range_reader,
                codec.as_ref(),
                self.block_size,
            )))
        } else {
            Ok(source.reader()?)
        };
    }

    /// Decompress data from the input stream into in-memory buffer.
    pub fn decompress(&self, mut input: impl Read) -> std::io::Result<Bytes> {
        let mut buffer = Vec::new();
        if let Some(codec) = &self.codec {
            let mut stream = DecompressionStream::new(input, codec.as_ref(), self.block_size);
            stream.read_to_end(&mut buffer)?;
        } else {
            input.read_to_end(&mut buffer)?;
        }
        Ok(Bytes::from(buffer))
    }

    /// Decompress data from the byte buffer into another in-memory buffer.
    pub fn decompress_buffer(&self, input: Bytes) -> std::io::Result<Bytes> {
        if self.codec.is_some() {
            self.decompress(input.reader())
        } else {
            Ok(input)
        }
    }
}

/// Compression registry contains references to codecs for all supported compression formats.
/// Can be used to provide a custom block codec implementation.
pub struct CompressionRegistry {
    snappy_codec: Option<Box<dyn BlockCodec>>,
    zstd_codec: Option<Box<dyn BlockCodec>>,
    lz4_codec: Option<Box<dyn BlockCodec>>,
    zlib_codec: Option<Box<dyn BlockCodec>>,
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
            snappy_codec: Some(Box::new(SnappyCodec {})),
            zstd_codec: Some(Box::new(ZstdCodec {})),
            lz4_codec: Some(Box::new(Lz4Codec {})),
            zlib_codec: Some(Box::new(ZlibCodec {})),
        }
    }

    /// Overrides Snappy codec.
    pub fn with_snappy_codec(&mut self, new_codec: Box<dyn BlockCodec>) {
        self.snappy_codec = Some(new_codec);
    }

    /// Overrides Zstd codec.
    pub fn with_zstd_codec(&mut self, new_codec: Box<dyn BlockCodec>) {
        self.zstd_codec = Some(new_codec);
    }

    /// Overrides Lz4 codec.
    pub fn with_lz4_codec(&mut self, new_codec: Box<dyn BlockCodec>) {
        self.lz4_codec = Some(new_codec);
    }

    /// Overrides Zlib codec.
    pub fn with_zlib_codec(&mut self, new_codec: Box<dyn BlockCodec>) {
        self.zlib_codec = Some(new_codec);
    }

    /// Find the codec for passed compression type and remove it from registry.
    ///
    /// Returns an error if compression is not supported or codec already removed.
    pub fn extract_codec(
        &mut self,
        compression_type: proto::CompressionKind,
    ) -> Result<Box<dyn BlockCodec>> {
        let not_found = std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Compression codec '{compression_type:?}' not found"),
        );
        match compression_type {
            proto::CompressionKind::Snappy => self.snappy_codec.take().ok_or(not_found),
            proto::CompressionKind::Zstd => self.zstd_codec.take().ok_or(not_found),
            proto::CompressionKind::Lz4 => self.lz4_codec.take().ok_or(not_found),
            proto::CompressionKind::Zlib => self.zlib_codec.take().ok_or(not_found),
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

#[derive(Debug, Default, Copy, Clone)]
pub struct ZstdCodec;

impl BlockCodec for ZstdCodec {
    fn decompress(&self, input: &[u8], max_output_len: usize) -> Result<Bytes> {
        Ok(zstd::bulk::decompress(input, max_output_len)?.into())
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct SnappyCodec;

impl BlockCodec for SnappyCodec {
    fn decompress(&self, input: &[u8], _max_output_len: usize) -> Result<Bytes> {
        let out_size = snap::raw::decompress_len(input)?;
        let mut output = WriteBuffer::new(out_size);
        output.write_from(|write_into| snap::raw::Decoder::new().decompress(input, write_into))?;
        let buffer: BytesMut = output.into();
        Ok(buffer.freeze())
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct Lz4Codec;

impl BlockCodec for Lz4Codec {
    fn decompress(&self, input: &[u8], max_output_len: usize) -> Result<Bytes> {
        let mut output = WriteBuffer::new(max_output_len);
        output.write_from(|write_into| {
            lz4_flex::block::decompress_into(input, write_into).map_err(|err| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
            })
        })?;
        let buffer: BytesMut = output.into();
        Ok(buffer.freeze())
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct ZlibCodec {}

impl BlockCodec for ZlibCodec {
    fn decompress(&self, input: &[u8], max_output_len: usize) -> Result<Bytes> {
        let mut decoder = flate2::Decompress::new(false);
        decoder.reset(false);
        let mut output = WriteBuffer::new(max_output_len);
        output.write_from(
            |write_into| -> std::result::Result<usize, flate2::DecompressError> {
                let bytes_before = decoder.total_out();
                let status =
                    decoder.decompress(input, write_into, flate2::FlushDecompress::Finish)?;
                debug_assert_eq!(status, Status::StreamEnd);
                Ok((decoder.total_out() - bytes_before) as usize)
            },
        )?;
        let buffer: BytesMut = output.into();
        Ok(buffer.freeze())
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
    current_block: WriteBuffer,
    /// Chunk of decompressed data which is not consumed yet.
    /// if `None`, then current block is not compressed and
    /// data should be read from `current_block`.
    decompressed_chunk: Bytes,
}

const HEADER_SIZE: usize = 3;

impl<'codec, Input: Read> DecompressionStream<'codec, Input> {
    fn new(compressed: Input, codec: &'codec dyn BlockCodec, block_size: u64) -> Self {
        let block_size = block_size as usize;
        let current_block = WriteBuffer::new(block_size);
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
                self.decompressed_chunk = self.current_block.as_mut().split_to(block_size).freeze();
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

impl<'codec, Input: Read> BufRead for DecompressionStream<'codec, Input> {}
