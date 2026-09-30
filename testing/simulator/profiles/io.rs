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
    pub latency_probability: u8,
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
}

impl Default for FaultProfile {
    fn default() -> Self {
        Self {
            enable: true,
            read: true,
            write: true,
            sync: true,
        }
    }
}
