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
    fn update_metadata<P: AsRef<Path>>(
        &self,
        path: P,
        mod_time: FileTime,
        read_only: bool,
    ) -> io::Result<()>;

    fn create_dir<P: AsRef<Path>>(&self, path: P, ignore_existing: bool) -> io::Result<()>;
    fn remove_dir_recursive<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;
    fn list_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<Vec<DirEntry>>;

    fn create_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;
    fn remove_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;

    /// Renames a file or folder. The destination_path must not exist already.
    fn rename<P1: AsRef<Path>, P2: AsRef<Path>>(
        &self,
        source_path: P1,
        dest_path: P2,
    ) -> io::Result<()>;

    fn read_file<P: AsRef<Path>>(&self, path: P) -> io::Result<Box<dyn io::Read>>;
    fn overwrite_file<'a, P: AsRef<Path>>(
        &self,
        path: P,
        data: Box<dyn io::Read + 'a>,
    ) -> io::Result<usize>;
    fn append_file<'a, P: AsRef<Path>>(
        &self,
        path: P,
        data: Box<dyn io::Read + 'a>,
    ) -> io::Result<usize>;
    fn db_access_type(&self) -> DBAccessType;
}

/// Represents a single entry in a directory.
/// Has the bare minimum information it needs attached to it.
#[derive(Debug, PartialEq)]
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
    pub fn is_file(&self) -> bool {
        self.file_type == FileType::File
    }
    pub fn is_dir(&self) -> bool {
        self.file_type == FileType::Dir
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

    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }
    pub fn set_last_mod_time(&mut self, last_mod_time: FileTime) {
        self.last_mod_time = last_mod_time;
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
use std::io::BufReader;
