use crate::proto;
use bytes::{Buf, Bytes};
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
    codec: Codec,
    decompressed_chunk: Bytes,
    block_size: usize,
}

impl<BaseStream: Read, Codec: BlockCodec> DecompressionStream<BaseStream, Codec> {
    fn new(codec: Codec, compressed: BaseStream, block_size: u64) -> Self {
        Self {
            compressed_stream: compressed,
            codec,
            block_size: block_size as usize,
            decompressed_chunk: Bytes::new(),
        }
    }
}

impl<BaseStream: Read, Codec: BlockCodec> Read for DecompressionStream<BaseStream, Codec> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut remaining = buf.len();

        while remaining > 0 {
            if self.decompressed_chunk.has_remaining() {
                let to_copy = cmp::min(self.decompressed_chunk.remaining(), remaining);
                self.decompressed_chunk.copy_to_slice(&mut buf[..to_copy]);
                remaining -= to_copy;
            } else {
                // need more decompressed data
                let next_chunk = Vec::with_capacity(self.block_size);
                unsafe {
                    next_chunk.set_len(self.block_size);
                }
                let bytes_read = self.compressed_stream.read(&mut next_chunk)?;
                if bytes_read == 0 {
                    break;
                }
                unsafe {
                    next_chunk.set_len(bytes_read);
                }
                let next_buf = Bytes::from(next_chunk);
                let (compressed_size, is_compressed) = decode_header(&mut next_buf);
                if is_compressed {
                    self.decompressed_chunk = self
                        .codec
                        .decompress(&mut next_buf.slice(..compressed_size))?;
                } else {
                    self.decompressed_chunk = next_buf;
                }
            }
        }

        return Ok(buf.len() - remaining);
    }
}

fn decode_header(next_chunk: &dyn Buf) -> (usize, bool) {
    if next_chunk.remaining() < 3 {
        return (0, true);
    }
    let header = [0; 4];
    next_chunk.copy_to_slice(&mut header[..3]);
    (u32::from_le_bytes(header) as usize, header[0] & 1 == 1)
}
