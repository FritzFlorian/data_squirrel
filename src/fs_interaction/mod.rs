use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum DataStoreError {
    AlreadyExists,
    AlreadyOpened,
    UnlockFailed { source: io::Error },
    IOError { source: io::Error },
}
impl From<io::Error> for DataStoreError {
    fn from(error: io::Error) -> Self {
        Self::IOError { source: error }
    }
}
pub type Result<T> = std::result::Result<T, DataStoreError>;

pub struct DataStore {
    root_path: PathBuf,
    locked: bool,
}

const METADATA_DIR: &str = ".__data_squirrel__";
const LOCK_FILE: &str = ".lock";

impl DataStore {
    pub fn open_for_directory(path: &Path) -> Result<DataStore> {
        let mut result = DataStore {
            root_path: path.to_path_buf(),
            locked: false,
        };
        result.acquire_exclusive_lock()?;

        Ok(result)
    }

    pub fn close(mut self) -> Result<()> {
        self.release_exclusive_lock()
    }

    pub fn create_for_directory(path: &Path) -> Result<DataStore> {
        // Create Metadata Directory (fail on io-errors or if it already exists).
        let metadata_path = path.join(METADATA_DIR);
        match fs::DirBuilder::new().recursive(false).create(metadata_path) {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                return Err(DataStoreError::AlreadyExists);
            }
            Err(e) => {
                return Err(DataStoreError::IOError { source: e });
            }
            _ => (),
        };

        Self::open_for_directory(&path)
    }

    fn lock_path(&self) -> PathBuf {
        self.root_path.join(METADATA_DIR).join(LOCK_FILE)
    }

    fn acquire_exclusive_lock(&mut self) -> Result<()> {
        if self.locked {
            return Ok(());
        }

        match fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .append(true)
            .open(self.lock_path())
        {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                return Err(DataStoreError::AlreadyOpened);
            }
            Err(e) => {
                return Err(DataStoreError::IOError { source: e });
            }
            Ok(file) => file,
        };

        self.locked = true;
        Ok(())
    }

    fn release_exclusive_lock(&mut self) -> Result<()> {
        if !self.locked {
            return Ok(());
        }

        fs::remove_file(self.lock_path())
            .map_err(|e| DataStoreError::UnlockFailed { source: e })?;

        self.locked = false;
        Ok(())
    }
}

impl Drop for DataStore {
    fn drop(&mut self) {
        // This is kind of a fatal fail...we can not release the lock?!
        self.release_exclusive_lock().unwrap();
    }
}

#[cfg(test)]
mod tests {
    extern crate tempfile;

    use super::*;

    #[test]
    fn create_data_store_in_empty_folder() {
        let test_dir = tempfile::tempdir().unwrap();

        let data_store = DataStore::create_for_directory(test_dir.path()).unwrap();
        assert_eq!(data_store.root_path, test_dir.path());

        assert!(
            test_dir.path().join(METADATA_DIR).is_dir(),
            "Must have created a special data_squirrel metadata folder."
        );
    }

    #[test]
    fn data_store_creates_and_releases_locks() {
        let test_dir = tempfile::tempdir().unwrap();

        let data_store = DataStore::create_for_directory(test_dir.path()).unwrap();
        assert!(
            test_dir.path().join(METADATA_DIR).join(LOCK_FILE).is_file(),
            "Must create lock file when having an open data_store."
        );

        drop(data_store);
        assert!(
            !test_dir.path().join(METADATA_DIR).join(LOCK_FILE).is_file(),
            "Must delete the lock file when closing a data_store."
        );
    }

    #[test]
    fn can_not_open_data_store_multiple_times() {
        let test_dir = tempfile::tempdir().unwrap();

        // Create and close
        let data_store_1 = DataStore::create_for_directory(test_dir.path()).unwrap();
        drop(data_store_1);

        // Open first instance
        let _data_store_2 = DataStore::open_for_directory(test_dir.path()).unwrap();

        // Opening second instance should fail
        match DataStore::open_for_directory(test_dir.path()) {
            Err(DataStoreError::AlreadyOpened) => (),
            _ => panic!("Must report error that data_store is in use."),
        };
    }
}
