use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{max_dependent, min_dependent};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct IOProfile {
    #[garde(skip)]
    pub enable: bool,
    #[garde(dive)]
    pub latency: LatencyProfile,
    #[garde(dive)]
    pub fault: FaultProfile,
    // TODO: expand here with header corruption options and faults on specific IO operations
}

impl Default for IOProfile {
    fn default() -> Self {
        Self {
            enable: true,
            latency: Default::default(),
            fault: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct LatencyProfile {
    #[garde(skip)]
    pub enable: bool,
    #[garde(range(min = 0, max = 100))]
    /// Added IO latency probability
    pub latency_probability: usize,
    #[garde(custom(max_dependent(&self.max_tick)))]
    /// Minimum tick time in microseconds for simulated time
    pub min_tick: u64,
    #[garde(custom(min_dependent(&self.min_tick)))]
    /// Maximum tick time in microseconds for simulated time
    pub max_tick: u64,
}

impl Default for LatencyProfile {
    fn default() -> Self {
        Self {
            enable: true,
            latency_probability: 1,
            min_tick: 1,
            max_tick: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct FaultProfile {
    #[garde(skip)]
    pub enable: bool,
    // TODO: modify SimIo impls to have a FaultProfile inside so they can skip faults depending on the profile
    #[garde(skip)]
    pub read: bool,
    #[garde(skip)]
    pub write: bool,
    #[garde(skip)]
    pub sync: bool,
    #[garde(dive)]
    pub short_write: ShortWriteProfile,
}

impl Default for FaultProfile {
    fn default() -> Self {
        Self {
            enable: true,
            read: true,
            write: true,
            sync: true,
            short_write: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct ShortWriteProfile {
    #[garde(skip)]
    pub enable: bool,
    #[garde(range(min = 0, max = 100))]
    pub probability: usize,
    #[garde(range(min = 1, max = 8192))]
    /// Minimum bytes to write in a short write (must be at least 1)
    pub min_bytes: usize,
    #[garde(range(min = 1, max = 8192))]
    /// Maximum bytes to subtract from full write (creates partial write)
    pub max_bytes_short: usize,
    #[garde(skip)]
    /// Only apply short writes to WAL files (files ending with "-wal")
    pub wal_only: bool,
}

impl Default for ShortWriteProfile {
    fn default() -> Self {
        Self {
            enable: false,
            probability: 5,
            min_bytes: 1,
            max_bytes_short: 512,
            wal_only: true,
        }
    }
}
