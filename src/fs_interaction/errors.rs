use super::*;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum FSInteractionError {
    MetadataDirAlreadyExists,
    MetadataDirAlreadyOpened,
    SoftLinksForbidden,
    // IOError is simply our 'catch all' error type for 'non-special' issues
    IOError {
        source: io::Error,
        kind: std::io::ErrorKind,
    },
}
pub type Result<T> = std::result::Result<T, FSInteractionError>;

impl FSInteractionError {
    pub fn is_io_not_found(&self) -> bool {
        match self {
            Self::IOError {
                kind: std::io::ErrorKind::NotFound,
                ..
            } => true,
            _ => false,
        }
    }

    pub fn is_io_already_exists(&self) -> bool {
        match self {
            Self::IOError {
                kind: std::io::ErrorKind::AlreadyExists,
                ..
            } => true,
            _ => false,
        }
    }

    pub fn is_io_no_directory(&self) -> bool {
        match self {
            Self::IOError {
                kind: std::io::ErrorKind::Other,
                source: io_error,
            } if io_error.raw_os_error() == Some(20) => true,
            _ => false,
        }
    }
}
impl From<io::Error> for FSInteractionError {
    fn from(error: io::Error) -> Self {
        Self::IOError {
            kind: error.kind(),
            source: error,
        }
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
            Self::IOError { ref source, .. } => Some(source),
            Self::MetadataDirAlreadyExists => None,
            Self::SoftLinksForbidden => None,
            Self::MetadataDirAlreadyOpened => None,
        }
    }
}
