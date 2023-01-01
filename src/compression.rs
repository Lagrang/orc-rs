use crate::proto;
use bytes::Bytes;
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

    fn decoder(
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
            _ => panic!("Compression type '{:?}' is not supported", compression_type),
        }
    }
}
