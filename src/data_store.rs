use crate::fs_interaction;
use crate::fs_interaction::virtual_fs;
use crate::fs_interaction::FSInteraction;
use crate::metadata_db;
use crate::metadata_db::MetadataDB;
use crate::version_vector;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum DataStoreError {
    Whoops,
    FSInteractionError {
        source: fs_interaction::FSInteractionError,
    },
    MetadataDBError {
        source: metadata_db::MetadataDBError,
    },
}
pub type Result<T> = std::result::Result<T, DataStoreError>;

impl From<fs_interaction::FSInteractionError> for DataStoreError {
    fn from(error: fs_interaction::FSInteractionError) -> Self {
        DataStoreError::FSInteractionError { source: error }
    }
}
impl From<metadata_db::MetadataDBError> for DataStoreError {
    fn from(error: metadata_db::MetadataDBError) -> Self {
        DataStoreError::MetadataDBError { source: error }
    }
}

pub struct DataStore<FS: virtual_fs::FS> {
    fs_access: FSInteraction<FS>,
    metadata_db: MetadataDB,
}
pub type DefaultDataStore = DataStore<virtual_fs::WrapperFS>;

impl<FS: virtual_fs::FS> DataStore<FS> {
    pub fn open(path: &Path, create: bool) -> Result<Self> {
        Self::open_with_fs(&path, create, FS::default())
    }
    pub fn open_with_fs(path: &Path, create: bool, fs: FS) -> Result<Self> {
        let fs_interaction = if create {
            FSInteraction::create_with_fs(&path, fs)?
        } else {
            FSInteraction::open_with_fs(&path, fs)?
        };

        let metadata_db = MetadataDB::open(fs_interaction.metadata_db_path().to_str().unwrap())?;

        Ok(Self {
            fs_access: fs_interaction,
            metadata_db: metadata_db,
        })
    }

    pub fn perform_full_scan(&self) -> Result<()> {
        self.perform_scan(&PathBuf::from(""))
    }

    fn perform_scan(&self, relative_path: &Path) -> Result<()> {
        let items = self.fs_access.index(relative_path)?;
        for item in &items {
            if item.issues.is_empty() {
                match item.metadata.as_ref().unwrap().file_type() {
                    virtual_fs::FileType::File => {
                        println!("Indexing File {:?}...", item.relative_path);
                        let hash = self.fs_access.calculate_hash(&item.relative_path)?;
                        println!("Hash: {:?}", hash);
                    }
                    virtual_fs::FileType::Link => {
                        println!("Skipping Link {:?}...", item.relative_path);
                    }
                    virtual_fs::FileType::Dir => {
                        println!("Indexing Directory {:?}...", item.relative_path);
                        self.perform_scan(&item.relative_path)?;
                    }
                }
            } else {
                // TODO: Properly collect issues and report them to the caller instead of
                //       handling them in place. This will allow menu/user driven repairs.
                eprintln!(
                    "Issues with data item {:?}: {:?}",
                    item.relative_path, item.issues
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn create_data_store() {
        DataStore::<virtual_fs::InMemoryFS>::open(&PathBuf::from("/"), true).unwrap();
    }

    #[test]
    fn re_open_data_store() {
        let test_dir = tempfile::tempdir().unwrap();

        // Should succeed in creating a new data-store in the empty directory.
        let data_store_1 = DefaultDataStore::open(test_dir.path(), true).unwrap();
        drop(data_store_1);

        // Should fail because we can not re-create in this directory.
        assert!(DefaultDataStore::open(test_dir.path(), true).is_err());

        // Should succeed to open the just opened data-store.
        let _data_store_2 = DefaultDataStore::open(test_dir.path(), false).unwrap();

        // Should fail, as the data store is already opened.
        assert!(DefaultDataStore::open(test_dir.path(), true).is_err());
    }

    #[test]
    fn scan_new_data_store() {
        let test_dir = tempfile::tempdir().unwrap();

        fs::create_dir(test_dir.path().join(&PathBuf::from("sub-1"))).unwrap();
        fs::create_dir(test_dir.path().join(&PathBuf::from("sub-1/sub-1-1"))).unwrap();
        fs::create_dir(test_dir.path().join(&PathBuf::from("sub-2"))).unwrap();

        fs::File::create(test_dir.path().join(&PathBuf::from("file-1"))).unwrap();
        fs::File::create(test_dir.path().join(&PathBuf::from("file-2"))).unwrap();
        fs::File::create(test_dir.path().join(&PathBuf::from("sub-1/file-1"))).unwrap();

        let data_store_1 = DefaultDataStore::open(test_dir.path(), true).unwrap();
        data_store_1.perform_full_scan().unwrap();
    }
}
