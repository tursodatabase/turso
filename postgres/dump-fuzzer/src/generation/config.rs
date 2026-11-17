use anyhow::{bail, Result};
use clap::ValueEnum;

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
pub enum Profile {
    Tables,
    Compatible,
}

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
pub enum Feature {
    Defaults,
    MixedCaseIdentifiers,
    Indexes,
    CompositeIndexes,
    UniqueIndexes,
    DescendingIndexes,
    Rls,
    ForceRls,
    Policies,
    AlterPolicy,
    DropPolicy,
    Grants,
    Revokes,
    DefaultPrivileges,
}

#[derive(Clone, Debug)]
pub(super) struct Config {
    pub profile: Profile,
    pub disabled: Vec<Feature>,
}

const DISABLED_BY_DEFAULT: [Feature; 7] = [
    Feature::Defaults,
    Feature::MixedCaseIdentifiers,
    Feature::DescendingIndexes,
    Feature::Rls,
    Feature::Policies,
    Feature::Grants,
    Feature::DefaultPrivileges,
];

pub fn disabled_features(enable: &[Feature], disable: &[Feature]) -> Result<Vec<Feature>> {
    let mut enabled = enable.to_vec();
    for feature in enable {
        if let Some(parent) = prerequisite(*feature) {
            enabled.push(parent);
        }
    }
    for feature in &enabled {
        if disable.contains(feature) {
            bail!("feature {} is explicitly disabled but also enabled or required by an enabled feature",
                feature.to_possible_value().unwrap().get_name());
        }
    }
    let mut disabled = DISABLED_BY_DEFAULT.to_vec();
    disabled.retain(|feature| !enabled.contains(feature));
    for feature in disable {
        if !disabled.contains(feature) {
            disabled.push(*feature);
        }
    }
    Ok(disabled)
}

impl Config {
    pub fn enabled(&self, feature: Feature) -> bool {
        !self.disabled.contains(&feature)
    }
}

fn prerequisite(feature: Feature) -> Option<Feature> {
    match feature {
        Feature::CompositeIndexes | Feature::UniqueIndexes | Feature::DescendingIndexes => {
            Some(Feature::Indexes)
        }
        Feature::ForceRls => Some(Feature::Rls),
        Feature::AlterPolicy | Feature::DropPolicy => Some(Feature::Policies),
        Feature::Revokes => Some(Feature::Grants),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::{generators as gs, TestCase};

    #[test]
    fn compatible_defaults_exclude_known_failures() {
        assert_eq!(disabled_features(&[], &[]).unwrap(), DISABLED_BY_DEFAULT);
    }
    #[hegel::test(test_cases = 2000)]
    fn explicit_feature_overrides_apply_to_the_default_config(tc: TestCase) {
        let feature = tc.draw(gs::sampled_from(Feature::value_variants()));
        let enable = tc.draw(gs::booleans());
        let disabled = if enable {
            disabled_features(&[feature], &[]).unwrap()
        } else {
            disabled_features(&[], &[feature]).unwrap()
        };
        assert_eq!(disabled.contains(&feature), !enable);
        for other in DISABLED_BY_DEFAULT {
            if other != feature && Some(other) != prerequisite(feature) {
                assert!(disabled.contains(&other));
            }
        }
    }

    #[test]
    fn enabling_children_enables_their_prerequisites() {
        for (child, parent) in [
            (Feature::CompositeIndexes, Feature::Indexes),
            (Feature::UniqueIndexes, Feature::Indexes),
            (Feature::DescendingIndexes, Feature::Indexes),
            (Feature::ForceRls, Feature::Rls),
            (Feature::AlterPolicy, Feature::Policies),
            (Feature::DropPolicy, Feature::Policies),
            (Feature::Revokes, Feature::Grants),
        ] {
            let disabled = disabled_features(&[child], &[]).unwrap();
            assert!(!disabled.contains(&child));
            assert!(!disabled.contains(&parent));
            assert!(disabled_features(&[child], &[parent]).is_err());
        }
    }

    #[hegel::test(test_cases = 2000)]
    fn conflicting_feature_flags_are_rejected(tc: TestCase) {
        let feature = tc.draw(gs::sampled_from(Feature::value_variants()));
        assert!(disabled_features(&[feature], &[feature]).is_err());
    }

    #[test]
    fn enabling_parent_allows_disabling_its_child() {
        let disabled = disabled_features(&[Feature::Policies], &[Feature::AlterPolicy]).unwrap();
        assert!(!disabled.contains(&Feature::Policies));
        assert!(disabled.contains(&Feature::AlterPolicy));
    }
}
