use super::*;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum MigrationError {
    ReadWriteDBVersion { source: diesel::result::Error },
    UnknownDBVersion { version: DBVersion },
    SQLError { source: diesel::result::Error },
}
pub type Result<T> = std::result::Result<T, MigrationError>;

// Error Boilerplate (Error display, conversion and source)
impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error During Database Migration ({:?})", self)
    }
}
impl From<diesel::result::Error> for MigrationError {
    fn from(error: diesel::result::Error) -> Self {
        Self::SQLError { source: error }
    }
}
impl Error for MigrationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ReadWriteDBVersion { ref source } => Some(source),
            Self::UnknownDBVersion { .. } => None,
            Self::SQLError { ref source } => Some(source),
        }
    }
}
