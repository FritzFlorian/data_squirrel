use crate::fs_interaction;
use crate::fs_interaction::virtual_fs;
use crate::fs_interaction::FSInteraction;
use crate::metadata_db;
use crate::metadata_db::MetadataDB;
use crate::version_vector;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum DataStoreError {
    DataStoreNotSetup,
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
    db_access: MetadataDB,
}
pub type DefaultDataStore = DataStore<virtual_fs::WrapperFS>;

impl<FS: virtual_fs::FS> DataStore<FS> {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_fs(&path, FS::default())
    }
    pub fn open_with_fs(path: &Path, fs: FS) -> Result<Self> {
        let fs_interaction = FSInteraction::open_with_fs(&path, fs)?;
        let metadata_db = MetadataDB::open(fs_interaction.metadata_db_path().to_str().unwrap())?;

        Ok(Self {
            fs_access: fs_interaction,
            db_access: metadata_db,
        })
    }

    pub fn create<P: AsRef<Path>>(
        path: P,
        data_set_unique_name: &str,
        data_set_human_name: &str,
        data_store_name: &str,
    ) -> Result<Self> {
        Self::create_with_fs(
            &path,
            &data_set_unique_name,
            &data_set_human_name,
            &data_store_name,
            FS::default(),
        )
    }
    pub fn create_with_fs<P: AsRef<Path>>(
        path: P,
        data_set_unique_name: &str,
        data_set_human_name: &str,
        data_store_name: &str,
        fs: FS,
    ) -> Result<Self> {
        let fs_interaction = FSInteraction::create_with_fs(path.as_ref(), fs)?;
        let metadata_db = MetadataDB::open(fs_interaction.metadata_db_path().to_str().unwrap())?;

        // Make sure we got an initial data_set created (might be a copy from a different store or
        // a newly created one, details are up to the application/ui flow).
        let data_set = metadata_db.create_data_set(&data_set_unique_name)?;
        metadata_db.update_data_set_name(&data_set_human_name)?;

        // Create an entry for our local data_store. Others might be added when interacting with
        // different disks to gain knowledge of them.
        let unique_id = uuid::Uuid::new_v4();
        metadata_db.create_data_store(&metadata_db::DataStore {
            id: 0,
            data_set_id: data_set.id,
            unique_name: format!("{:}-{:}", data_store_name, unique_id),
            human_name: data_store_name.to_string(),
            creation_date: chrono::Utc::now().naive_local(),
            is_this_store: true,
            path_on_device: fs_interaction.root_path().to_str().unwrap().to_string(),
            location_note: String::new(),
            version: 0,
        })?;

        Ok(Self {
            fs_access: fs_interaction,
            db_access: metadata_db,
        })
    }

    pub fn perform_full_scan(&self) -> Result<ScanResult> {
        let root_path = PathBuf::from("");
        let root_metadata = self.fs_access.metadata(&root_path)?;

        self.perform_scan(&root_path, &root_metadata)
    }

    fn perform_scan(
        &self,
        path: &Path,
        path_metadata: &virtual_fs::Metadata,
    ) -> Result<ScanResult> {
        let mut scan_result = ScanResult::new();
        let data_store = self.db_access.get_this_data_store()?;

        let dir_db_entry = self
            .db_access
            .get_data_item(&data_store, path.to_str().unwrap())?;
        let dir_dib_entry = if let Some(entry) = dir_db_entry {
            entry
        } else {
            // TODO: pass all required properties...
            scan_result.new_items += 1;
            self.db_access
                .create_local_data_item(path.to_str().unwrap())?
        };

        let items = self.fs_access.index(path)?;
        for item in &items {
            scan_result.indexed_items += 1;
            if item.issues.is_empty() {
                let item_metadata = item.metadata.as_ref().unwrap();
                match item_metadata.file_type() {
                    virtual_fs::FileType::File => {
                        println!("Indexing File {:?}...", item.relative_path);
                        let item_db_entry = self
                            .db_access
                            .get_data_item(&data_store, path.to_str().unwrap())?;
                        if let Some((data_item, metadata)) = item_db_entry {
                            // if metadata differs
                            {
                                scan_result.changed_items += 1;
                                let hash = self.fs_access.calculate_hash(&item.relative_path)?;
                                use data_encoding::HEXUPPER;
                                println!("Hash: {:}", HEXUPPER.encode(hash.as_ref()));
                                // if hash differs
                                {
                                    // record update in modification version vector
                                    // TODO: pass all required information for the update.
                                    self.db_access.modify_local_data_item()?;
                                }
                                // TODO: handle cases where ONLY metadata changes
                            }
                            // else optional bit-rot detection hash calculation
                            {
                                // TODO: calculate file hash and possibly report issue.
                            }
                        } else {
                            scan_result.new_items += 1;
                            let hash = self.fs_access.calculate_hash(&item.relative_path)?;
                            use data_encoding::HEXUPPER;
                            println!("Hash: {:}", HEXUPPER.encode(hash.as_ref()));
                            // TODO: pass all required arguments
                            self.db_access
                                .create_local_data_item(&item.relative_path.to_str().unwrap())?;
                        }
                    }
                    virtual_fs::FileType::Link => {
                        println!("Skipping Link {:?}...", item.relative_path);
                    }
                    virtual_fs::FileType::Dir => {
                        println!("Indexing Directory {:?}...", item.relative_path);
                        self.perform_scan(&item.relative_path, item_metadata)?;
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
        scan_result.deleted_items += 0;
        // TODO: Look at all items that are in DB but no longer present in folder.
        //       We need to delete them and recursively delete their child entries.
        Ok(scan_result)
    }
}

#[derive(Debug, PartialEq)]
pub struct ScanResult {
    pub indexed_items: usize,
    pub changed_items: usize,
    pub new_items: usize,
    pub deleted_items: usize,
}
impl ScanResult {
    pub fn new() -> Self {
        Self {
            indexed_items: 0,
            changed_items: 0,
            new_items: 0,
            deleted_items: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_interaction::virtual_fs::FS;
    use std::fs;
    use std::io::Write;

    #[test]
    fn create_data_store() {
        let test_dir = tempfile::tempdir().unwrap();

        DefaultDataStore::create(test_dir.path(), "XYZ", "XYZ", "local-data-store").unwrap();

        // TODO: Some tests to see if DB entries are created as intended.
        //       We will add them when we have more of the data_store interface, i.e.
        //       when there is the required interaction between two data_store instances.
    }

    #[test]
    fn re_open_data_store() {
        let test_dir = tempfile::tempdir().unwrap();

        // Should succeed in creating a new data-store in the empty directory.
        let data_store_1 =
            DefaultDataStore::create(&test_dir.path(), "XYZ", "XYZ", "local-data-store").unwrap();
        drop(data_store_1);

        // Should fail because we can not re-create in this directory.
        assert!(
            DefaultDataStore::create(&test_dir.path(), "XYZ", "XYZ", "local-data-store").is_err()
        );

        // Should succeed to open the just opened data-store.
        let _data_store_2 = DefaultDataStore::open(test_dir.path()).unwrap();

        // Should fail, as the data store is already opened.
        assert!(DefaultDataStore::open(test_dir.path()).is_err());
    }

    #[test]
    fn scan_data_store_directory() {
        let in_memory_fs = virtual_fs::InMemoryFS::new();

        in_memory_fs.create_dir("sub-1").unwrap();
        in_memory_fs.create_dir("sub-1/sub-1-1").unwrap();
        in_memory_fs.create_dir("sub-2").unwrap();

        in_memory_fs.create_file("file-1").unwrap();
        in_memory_fs.create_file("file-2").unwrap();
        in_memory_fs.create_file("sub-1/file-1").unwrap();

        let data_store_1 =
            DataStore::create_with_fs("", "XYZ", "XYZ", "local-data-store", in_memory_fs.clone())
                .unwrap();

        let changes = data_store_1.perform_full_scan().unwrap();
        assert_eq!(
            changes,
            ScanResult {
                indexed_items: 7,
                changed_items: 0,
                new_items: 7,
                deleted_items: 0
            }
        );

        // TODO: simulate changes and see if the re-scans pick them up correctly.
    }
}
