use filetime::FileTime;
use std::io;
use std::path::{Path, PathBuf};

/// Virtual abstraction layer above the actual FS implementation and API.
///
/// Two main implementations are planned:
/// 1) thin wrapper around actual FS API providing all functionality we require
/// 2) in-memory mock that allows testing for FS errors (weird permissions, access errors, ...)
///
/// We only wrap/implement functions we actually require in our code. This can be less or sometimes
/// more than the std::fs module provides (e.g. we would like to be able to set times on files).
pub trait FS: Clone {
    fn default() -> Self;

    fn canonicalize<P: AsRef<Path>>(&self, path: P) -> io::Result<PathBuf>;
    fn metadata<P: AsRef<Path>>(&self, path: P) -> io::Result<Metadata>;

    fn create_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;
    fn remove_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;
    fn list_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<Vec<DirEntry>>;

    fn create_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;
    fn remove_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;

    fn read_file<P: AsRef<Path>>(&self, path: P) -> io::Result<Box<dyn io::Read>>;

    fn db_access_type(&self) -> DBAccessType;
}

/// Represents a single entry in a directory.
/// Has the bare minimum information it needs attached to it.
pub struct DirEntry {
    pub file_name: OsString,
}

/// A wrapper around metadata we require in our application.
/// It should both represent the exact metadata of the file (OS specific), as well as
/// giving an abstraction of the smallest common denominator between the operating systems.
/// The goal is to use the exact data for update checks and use the platform agnostic
/// information when actually syncing between computers (e.g. also store it in the database).
#[derive(Debug, Clone)]
pub struct Metadata {
    file_type: FileType,
    read_only: bool,
    last_acc_time: FileTime,
    last_mod_time: FileTime,
    creation_time: FileTime,
}
impl Metadata {
    pub fn file_type(&self) -> FileType {
        self.file_type
    }
    pub fn read_only(&self) -> bool {
        self.read_only
    }
    pub fn last_acc_time(&self) -> FileTime {
        self.last_acc_time
    }
    pub fn last_mod_time(&self) -> FileTime {
        self.last_mod_time
    }
    pub fn creation_time(&self) -> FileTime {
        self.creation_time
    }
}
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum FileType {
    File,
    Dir,
    Link,
}

/// Depending on the file system there are different capabilities regarding running a databases
/// stare directly on them (e.g. SQLite won't work properly on network drives and not at all on
/// on AWS).
/// We differ between direct/in-place access, temporary copy access (i.e. copy the DB file to a
/// local directory, work with it, then update the remote copy) and in memory (for debugging only).
///
/// This way of handling the DB capabilities is not optimal and should be re-worked in the future.
pub enum DBAccessType {
    InPlace,
    TmpCopy,
    InMemory,
}

// Actual Implementations in Sub-Modules
mod wrapper_fs;
pub use self::wrapper_fs::WrapperFS;

mod in_memory_fs;
pub use self::in_memory_fs::InMemoryFS;
use std::ffi::OsString;
