use crate::{LimboError, Result};

#[derive(Debug, Clone, Copy)]
pub enum FunctionArity {
    Exact(usize),
    OneOf(&'static [usize]),
    Variadic,
    AtLeast(usize),
}

impl FunctionArity {
    pub fn accepts(self, count: usize) -> bool {
        match self {
            Self::Exact(expected) => count == expected,
            Self::OneOf(counts) => counts.contains(&count),
            Self::Variadic => true,
            Self::AtLeast(minimum) => count >= minimum,
        }
    }

    pub(crate) fn validate(self) -> Result<()> {
        let valid = match self {
            Self::Exact(count) | Self::AtLeast(count) => i32::try_from(count).is_ok(),
            Self::OneOf(counts) => {
                !counts.is_empty() && counts.iter().all(|count| i32::try_from(*count).is_ok())
            }
            Self::Variadic => true,
        };
        if !valid {
            return Err(LimboError::InvalidArgument(
                "function argument counts must be nonempty and fit in i32".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn arg_counts(self) -> Vec<i32> {
        match self {
            Self::Exact(count) => vec![i32::try_from(count).expect("validated argument count")],
            Self::OneOf(counts) => counts
                .iter()
                .map(|count| i32::try_from(*count).expect("validated argument count"))
                .collect(),
            Self::Variadic | Self::AtLeast(_) => vec![-1],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FunctionArity;

    #[test]
    fn argument_counts_match_each_variant() {
        for count in [0, 1, 2, 3, 4, 5, i32::MAX as usize, usize::MAX] {
            assert_eq!(FunctionArity::Exact(0).accepts(count), count == 0);
            assert_eq!(FunctionArity::Exact(2).accepts(count), count == 2);
            assert_eq!(
                FunctionArity::OneOf(&[1, 3]).accepts(count),
                count == 1 || count == 3
            );
            assert_eq!(FunctionArity::AtLeast(2).accepts(count), count >= 2);
            assert!(FunctionArity::Variadic.accepts(count));
        }
    }

    #[test]
    fn sqlite_argument_counts_preserve_fixed_and_variable_signatures() {
        for (arity, expected) in [
            (FunctionArity::Exact(0), vec![0]),
            (FunctionArity::OneOf(&[1, 2]), vec![1, 2]),
            (FunctionArity::AtLeast(2), vec![-1]),
            (FunctionArity::Variadic, vec![-1]),
        ] {
            arity.validate().unwrap();
            assert_eq!(arity.arg_counts(), expected);
        }
        for invalid in [
            FunctionArity::OneOf(&[]),
            FunctionArity::Exact(usize::MAX),
            FunctionArity::OneOf(&[usize::MAX]),
            FunctionArity::AtLeast(usize::MAX),
        ] {
            assert!(invalid.validate().is_err());
        }
    }
}
