use super::*;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum FSInteractionError {
    AlreadyExists,
    AlreadyOpened,
    SoftLinksForbidden,
    // IOError is simply our 'catch all' error type for 'non-special' issues
    IOError { source: io::Error },
}
pub type Result<T> = std::result::Result<T, FSInteractionError>;

impl From<io::Error> for FSInteractionError {
    fn from(error: io::Error) -> Self {
        Self::IOError { source: error }
    }
}
impl fmt::Display for FSInteractionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error when accessing the FS ({:?})", self)
    }
}
impl Error for FSInteractionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IOError { ref source } => Some(source),
            Self::AlreadyExists => None,
            Self::SoftLinksForbidden => None,
            Self::AlreadyOpened => None,
        }
    }
}