use std::io;

use serde::{Deserialize, Deserializer, Serialize};

/// Compressor can be used on `IndexSettings` to choose
/// the compressor used to compress the doc store.
///
/// The default is Lz4Block, but also depends on the enabled feature flags.
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum Compressor {
    /// No compression
    None,
    /// Use the lz4 compressor (block format)
    #[cfg(feature = "lz4-compression")]
    Lz4,
    /// Use the zstd compressor
    #[cfg(feature = "zstd-compression")]
    Zstd(ZstdCompressor),
}

impl Serialize for Compressor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match *self {
            Compressor::None => serializer.serialize_str("none"),
            #[cfg(feature = "lz4-compression")]
            Compressor::Lz4 => serializer.serialize_str("lz4"),
            #[cfg(feature = "zstd-compression")]
            Compressor::Zstd(zstd) => serializer.serialize_str(&zstd.ser_to_string()),
        }
    }
}

impl<'de> Deserialize<'de> for Compressor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let buf = String::deserialize(deserializer)?;
        let compressor = match buf.as_str() {
            "none" => Compressor::None,
            #[cfg(feature = "lz4-compression")]
            "lz4" => Compressor::Lz4,
            #[cfg(not(feature = "lz4-compression"))]
            "lz4" => {
                return Err(serde::de::Error::custom(
                    "unsupported variant `lz4`, please enable Tantivy's `lz4-compression` feature",
                ))
            }
            #[cfg(feature = "zstd-compression")]
            _ if buf.starts_with("zstd") => Compressor::Zstd(
                ZstdCompressor::deser_from_str(&buf).map_err(serde::de::Error::custom)?,
            ),
            #[cfg(not(feature = "zstd-compression"))]
            _ if buf.starts_with("zstd") => {
                return Err(serde::de::Error::custom(
                    "unsupported variant `zstd`, please enable Tantivy's `zstd-compression` \
                     feature",
                ))
            }
            _ => {
                return Err(serde::de::Error::unknown_variant(
                    &buf,
                    &[
                        "none",
                        #[cfg(feature = "lz4-compression")]
                        "lz4",
                        #[cfg(feature = "zstd-compression")]
                        "zstd",
                        #[cfg(feature = "zstd-compression")]
                        "zstd(compression_level=5)",
                    ],
                ));
            }
        };

        Ok(compressor)
    }
}

#[derive(Clone, Default, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// The Zstd compressor, with optional compression level.
pub struct ZstdCompressor {
    /// The compression level, if unset defaults to zstd::DEFAULT_COMPRESSION_LEVEL = 3
    pub compression_level: Option<i32>,
}

#[cfg(feature = "zstd-compression")]
impl ZstdCompressor {
    fn deser_from_str(val: &str) -> Result<ZstdCompressor, String> {
        if !val.starts_with("zstd") {
            return Err(format!("needs to start with zstd, but got {val}"));
        }
        if val == "zstd" {
            return Ok(ZstdCompressor::default());
        }
        let options = &val["zstd".len() + 1..val.len() - 1];

        let mut compressor = ZstdCompressor::default();
        for option in options.split(',') {
            let (opt_name, value) = options
                .split_once('=')
                .ok_or_else(|| format!("no '=' found in option {option:?}"))?;

            match opt_name {
                "compression_level" => {
                    let value = value.parse::<i32>().map_err(|err| {
                        format!("Could not parse value {value} of option {opt_name}, e: {err}")
                    })?;
                    if value >= 15 {
                        warn!(
                            "High zstd compression level detected: {:?}. High compression levels \
                             (>=15) are slow and will limit indexing speed.",
                            value
                        )
                    }
                    compressor.compression_level = Some(value);
                }
                _ => {
                    return Err(format!("unknown zstd option {opt_name:?}"));
                }
            }
        }
        Ok(compressor)
    }
    fn ser_to_string(&self) -> String {
        if let Some(compression_level) = self.compression_level {
            format!("zstd(compression_level={compression_level})")
        } else {
            "zstd".to_string()
        }
    }
}

impl Default for Compressor {
    #[allow(unreachable_code)]
    fn default() -> Self {
        #[cfg(feature = "lz4-compression")]
        return Compressor::Lz4;

        #[cfg(feature = "zstd-compression")]
        return Compressor::Zstd(ZstdCompressor::default());

        Compressor::None
    }
}

impl Compressor {
    #[inline]
    pub(crate) fn compress_into(
        &self,
        uncompressed: &[u8],
        compressed: &mut Vec<u8>,
    ) -> io::Result<()> {
        match self {
            Self::None => {
                compressed.clear();
                compressed.extend_from_slice(uncompressed);
                Ok(())
            }
            #[cfg(feature = "lz4-compression")]
            Self::Lz4 => super::compression_lz4_block::compress(uncompressed, compressed),
            #[cfg(feature = "zstd-compression")]
            Self::Zstd(_zstd_compressor) => super::compression_zstd_block::compress(
                uncompressed,
                compressed,
                _zstd_compressor.compression_level,
            ),
        }
    }
}

#[cfg(all(feature = "zstd-compression", test))]
mod tests {
    use super::*;

    #[test]
    fn zstd_serde_roundtrip() {
        let compressor = ZstdCompressor {
            compression_level: Some(15),
        };

        assert_eq!(
            ZstdCompressor::deser_from_str(&compressor.ser_to_string()).unwrap(),
            compressor
        );

        assert_eq!(
            ZstdCompressor::deser_from_str(&ZstdCompressor::default().ser_to_string()).unwrap(),
            ZstdCompressor::default()
        );
    }

    #[test]
    fn deser_zstd_test() {
        assert_eq!(
            ZstdCompressor::deser_from_str("zstd").unwrap(),
            ZstdCompressor::default()
        );

        assert!(ZstdCompressor::deser_from_str("zzstd").is_err());
        assert!(ZstdCompressor::deser_from_str("zzstd()").is_err());
        assert_eq!(
            ZstdCompressor::deser_from_str("zstd(compression_level=15)").unwrap(),
            ZstdCompressor {
                compression_level: Some(15)
            }
        );
        assert_eq!(
            ZstdCompressor::deser_from_str("zstd(compresion_level=15)").unwrap_err(),
            "unknown zstd option \"compresion_level\""
        );
        assert_eq!(
            ZstdCompressor::deser_from_str("zstd(compression_level->2)").unwrap_err(),
            "no '=' found in option \"compression_level->2\""
        );
        assert_eq!(
            ZstdCompressor::deser_from_str("zstd(compression_level=over9000)").unwrap_err(),
            "Could not parse value over9000 of option compression_level, e: invalid digit found \
             in string"
        );
    }
}
