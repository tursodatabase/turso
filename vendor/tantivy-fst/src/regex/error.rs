use std::error;
use std::fmt;

use super::regex_syntax;

/// An error that occurred while compiling a regular expression.
#[derive(Debug)]
pub enum Error {
    /// A problem with the syntax of a regular expression.
    Syntax(regex_syntax::Error),
    /// Too many instructions resulting from the regular expression.
    ///
    /// The number given is the limit that was exceeded.
    CompiledTooBig(usize),
    /// Too many automata states resulting from the regular expression.
    ///
    /// This is distinct from `CompiledTooBig` because `TooManyStates` refers
    /// to the DFA construction where as `CompiledTooBig` refers to the NFA
    /// construction.
    ///
    /// The number given is the limit that was exceeded.
    TooManyStates(usize),
    /// Lazy quantifiers are not allowed (because they have no useful
    /// interpretation when used purely for automata intersection, as is the
    /// case in this crate).
    NoLazy,
    /// Word boundaries are currently not allowed.
    ///
    /// This restriction may be lifted in the future.
    NoWordBoundary,
    /// Empty or "zero width assertions" such as `^` or `$` are currently
    /// not allowed.
    ///
    /// This restriction may be lifted in the future.
    NoEmpty,
    /// Byte literals such as `(?-u:\xff)` are not allowed.
    ///
    /// This restriction may be lifted in the future.
    NoBytes,
}

impl From<regex_syntax::Error> for Error {
    #[inline]
    fn from(err: regex_syntax::Error) -> Error {
        Error::Syntax(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match *self {
            Syntax(ref err) => err.fmt(f),
            CompiledTooBig(size_limit) => write!(
                f,
                "Compiled regex exceeds size limit of {} bytes",
                size_limit
            ),
            TooManyStates(size_limit) => write!(
                f,
                "Compiled regex exceeds size limit of {} states",
                size_limit
            ),
            NoLazy => write!(
                f,
                "Lazy reptition operators such as '+?' are \
                 not allowed."
            ),
            NoWordBoundary => write!(
                f,
                "Word boundary operators are not \
                 allowed."
            ),
            NoEmpty => write!(
                f,
                "Empty match operators are not allowed \
                 (hopefully temporary)."
            ),
            NoBytes => write!(f, "Byte literals are not allowed."),
        }
    }
}

impl error::Error for Error {
    fn cause(&self) -> Option<&dyn error::Error> {
        use self::Error::*;
        match *self {
            Syntax(ref err) => Some(err),
            _ => None,
        }
    }
}
