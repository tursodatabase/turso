use std::{
    fmt::Display,
    fs,
    num::NonZeroU32,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::Context;
use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sql_generation::generation::{InsertOpts, LargeTableOpts, Opts, QueryOpts, TableOpts};
use strum::EnumString;

use crate::profiles::{
    io::{FaultProfile, IOProfile},
    query::QueryProfile,
};

pub mod io;
pub mod query;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct Profile {
    #[garde(skip)]
    /// Experimental MVCC feature
    pub experimental_mvcc: bool,
    #[garde(range(min = 1))]
    pub max_connections: usize,
    #[garde(dive)]
    pub io: IOProfile,
    #[garde(dive)]
    pub query: QueryProfile,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            experimental_mvcc: false,
            max_connections: 10,
            io: Default::default(),
            query: Default::default(),
        }
    }
}

impl Profile {
    pub fn write_heavy() -> Self {
        let profile = Profile {
            query: QueryProfile {
                gen_opts: Opts {
                    // TODO: in the future tweak blob size for bigger inserts
                    // TODO: increase number of rows as well
                    table: TableOpts {
                        large_table: LargeTableOpts {
                            large_table_prob: 0.4,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    query: QueryOpts {
                        insert: InsertOpts {
                            min_rows: NonZeroU32::new(5).unwrap(),
                            max_rows: NonZeroU32::new(11).unwrap(),
                        },
                        ..Default::default()
                    },
                    ..Default::default()
                },
                select_weight: 30,
                insert_weight: 70,
                delete_weight: 0,
                update_weight: 0,
                ..Default::default()
            },
            ..Default::default()
        };

        // Validate that we as the developer are not creating an incorrect default profile
        profile.validate().unwrap();
        profile
    }

    pub fn faultless() -> Self {
        let profile = Profile {
            io: IOProfile {
                fault: FaultProfile {
                    enable: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            query: QueryProfile {
                create_table_weight: 0,
                create_index_weight: 4,
                ..Default::default()
            },
            ..Default::default()
        };

        // Validate that we as the developer are not creating an incorrect default profile
        profile.validate().unwrap();
        profile
    }

    pub fn parse_from_type(profile_type: ProfileType) -> anyhow::Result<Self> {
        let profile = match profile_type {
            ProfileType::Default => Self::default(),
            ProfileType::WriteHeavy => Self::write_heavy(),
            ProfileType::Faultless => Self::faultless(),
            ProfileType::Custom(path) => {
                Self::parse(path).with_context(|| "failed to parse JSON profile")?
            }
        };
        Ok(profile)
    }

    // TODO: in the future handle extension and composability of profiles here
    pub fn parse(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let contents = fs::read_to_string(path)?;
        // use json5 so we can support comments and trailing commas
        let profile: Profile = json5::from_str(&contents)?;
        profile.validate()?;
        Ok(profile)
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    Serialize,
    Deserialize,
    EnumString,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::Display,
    strum::VariantNames,
)]
#[serde(rename_all = "snake_case")]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum ProfileType {
    #[default]
    Default,
    WriteHeavy,
    Faultless,
    #[strum(disabled)]
    Custom(PathBuf),
}

impl ProfileType {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        if let Ok(prof) = ProfileType::from_str(s) {
            Ok(prof)
        } else if let path = PathBuf::from(s)
            && path.exists()
        {
            Ok(ProfileType::Custom(path))
        } else {
            Err(anyhow::anyhow!(
                "failed identifying predifined profile or custom profile path"
            ))
        }
    }
}

/// Minimum value of field is dependent on another field in the struct
fn min_dependent<T: PartialOrd + Display>(min: &T) -> impl FnOnce(&T, &()) -> garde::Result + '_ {
    move |value, _| {
        if value < min {
            return Err(garde::Error::new(format!(
                "`{value}` is smaller than `{min}`"
            )));
        }
        Ok(())
    }
}

/// Maximum value of field is dependent on another field in the struct
fn max_dependent<T: PartialOrd + Display>(max: &T) -> impl FnOnce(&T, &()) -> garde::Result + '_ {
    move |value, _| {
        if value > max {
            return Err(garde::Error::new(format!(
                "`{value}` is bigger than `{max}`"
            )));
        }
        Ok(())
    }
}
