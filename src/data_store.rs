use crate::fs_interaction;
use crate::fs_interaction::virtual_fs;
use crate::fs_interaction::FSInteraction;
use crate::metadata_db;
use crate::metadata_db::MetadataDB;
use chrono::NaiveDateTime;
use std::collections::HashSet;
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
        metadata_db.create_data_store(&metadata_db::data_store::InsertFull {
            data_set_id: data_set.id,
            unique_name: &format!("{:}-{:}", data_store_name, unique_id),
            human_name: data_store_name,
            creation_date: &chrono::Utc::now().naive_local(),
            is_this_store: true,
            path_on_device: fs_interaction.root_path().to_str().unwrap(),
            location_note: "",
            version: 0,
        })?;

        Ok(Self {
            fs_access: fs_interaction,
            db_access: metadata_db,
        })
    }

    pub fn local_version(&self) -> Result<i64> {
        Ok(self.db_access.get_this_data_store()?.version)
    }

    pub fn perform_full_scan(&self) -> Result<ScanResult> {
        let root_path = PathBuf::from("");
        let root_metadata = self.fs_access.metadata(&root_path)?;

        self.perform_scan(&root_path, &root_metadata)
    }

    fn fs_to_date_time(fs_time: &filetime::FileTime) -> NaiveDateTime {
        NaiveDateTime::from_timestamp(fs_time.unix_seconds(), fs_time.nanoseconds())
    }

    fn index_dir(
        &self,
        path: &Path,
        metadata: &virtual_fs::Metadata,
        data_store: &metadata_db::DataStore,
    ) -> Result<ScanResult> {
        let mut result = ScanResult::new();
        result.indexed_items += 1;

        let dir_db_entry = self
            .db_access
            .get_data_item(&data_store, path.to_str().unwrap())?;

        if let Some((_db_item, _db_owner, _db_metadata)) = dir_db_entry {
            // TODO: Check if we find metadata changes and apply them.
            //       An open question is if we handle this as a change or not.
            // TODO: Optionally also see if the path changed in upper/lower cases and note it.
        } else {
            result.new_items += 1;

            self.db_access.create_local_data_item(
                &path,
                Self::fs_to_date_time(&metadata.creation_time()),
                Self::fs_to_date_time(&metadata.last_mod_time()),
                false,
                "",
            )?;
        }

        Ok(result)
    }

    fn index_file(
        &self,
        path: &Path,
        metadata: &virtual_fs::Metadata,
        data_store: &metadata_db::DataStore,
        detect_bitrot: bool,
    ) -> Result<ScanResult> {
        let mut result = ScanResult::new();
        result.indexed_items += 1;

        let item_db_entry = self
            .db_access
            .get_data_item(&data_store, path.to_str().unwrap())?;

        if let Some((_db_item, db_owner, db_metadata)) = item_db_entry {
            // TODO: Inspect more changes in metadata.
            //       Decide how we handle them, e.g. is a permission change or a change in
            //       file creating time note-worthy? If so, do we simply record it as a local change
            //       to our DB's metadata or do we take not of it as 'this file changed'?.
            // TODO: Detect a rather 'big' change: What is now a file was a directory before!!!
            //       Currently we think this would be best handled by deleting the entry and then
            //       re-adding it.
            if Self::fs_to_date_time(&metadata.last_mod_time()) != db_metadata.mod_time {
                use data_encoding::HEXUPPER;
                let hash = self.fs_access.calculate_hash(&path)?;
                let hash = HEXUPPER.encode(hash.as_ref());

                if db_metadata.hash != hash {
                    result.changed_items += 1;

                    self.db_access.modify_local_data_item(
                        &db_owner,
                        &db_metadata,
                        &Self::fs_to_date_time(&metadata.creation_time()),
                        &Self::fs_to_date_time(&metadata.last_mod_time()),
                        &hash,
                    )?;
                } else {
                    // TODO: handle cases where ONLY metadata changes.
                    //       Closely related to the above question which metadata changes
                    //       are noteworthy as a 'this file changed' event.
                }
            } else if detect_bitrot {
                use data_encoding::HEXUPPER;
                let hash = self.fs_access.calculate_hash(&path)?;
                let hash = HEXUPPER.encode(hash.as_ref());

                if db_metadata.hash != hash {
                    // TODO: properly handle this by returning errors. Maybe re-trying to hash
                    //       the file in case this was simply a read issue.
                    panic!("Bitrot detected!")
                }
            }
        } else {
            // We have no local entry for the target file in our DB, register it as a new file.
            result.new_items += 1;

            use data_encoding::HEXUPPER;
            let hash = self.fs_access.calculate_hash(&path)?;
            let hash = HEXUPPER.encode(hash.as_ref());

            self.db_access.create_local_data_item(
                &path,
                Self::fs_to_date_time(&metadata.creation_time()),
                Self::fs_to_date_time(&metadata.last_mod_time()),
                true,
                &hash,
            )?;
        }

        Ok(result)
    }

    fn perform_scan(
        &self,
        dir_path: &Path,
        dir_metadata: &virtual_fs::Metadata,
    ) -> Result<ScanResult> {
        // We keep track of 'scan events' to have a rough output on a run of the scan function.
        let mut scan_result = ScanResult::new();
        let data_store = self.db_access.get_this_data_store()?;

        // Index the currently scanned dir (e.g. add it to the DB if it does not exist).
        scan_result =
            scan_result.combine(&self.index_dir(&dir_path, &dir_metadata, &data_store)?);

        // Next, we index each file present on disk in this directory.
        // This is the 'positive' part of the scan operation, i.e. we add anything that is on
        // disk and not in the DB, as well as anything that has changed on dis.
        let items = self.fs_access.index(dir_path)?;
        let mut item_names = HashSet::with_capacity(items.len());
        for item in &items {
            item_names.insert(item.relative_path.clone());

            if item.issues.is_empty() {
                let item_metadata = item.metadata.as_ref().unwrap();
                match item_metadata.file_type() {
                    virtual_fs::FileType::File => {
                        let file_scan_result = self.index_file(
                            &item.relative_path,
                            &item_metadata,
                            &data_store,
                            false,
                        )?;
                        scan_result = scan_result.combine(&file_scan_result);
                    }
                    virtual_fs::FileType::Link => {
                        // Todo: Properly collect un-handled links to the caller.
                        eprintln!("Skipping Link {:?}...", item.relative_path);
                    }
                    virtual_fs::FileType::Dir => {
                        let sub_dir_result =
                            self.perform_scan(&item.relative_path, item_metadata)?;
                        scan_result = scan_result.combine(&sub_dir_result);
                    }
                }
            } else {
                // TODO: Properly collect issues and report them to the caller.
                eprintln!(
                    "Issues with data item {:?}: {:?}",
                    item.relative_path, item.issues
                );
            }
        }

        // Lastly we perform the 'negative' operation of the scan process:
        // We load all known entries of the directory and see if there are any that are
        // no longer present on disk, thus signaling a deletion.
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

    pub fn combine(&self, other: &Self) -> Self {
        Self {
            indexed_items: self.indexed_items + other.indexed_items,
            changed_items: self.changed_items + other.changed_items,
            new_items: self.new_items + other.new_items,
            deleted_items: self.deleted_items + other.deleted_items,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_interaction::virtual_fs::FS;

    #[test]
    fn create_data_store() {
        let test_dir = tempfile::tempdir().unwrap();
        let data_store =
            DefaultDataStore::create(test_dir.path(), "XYZ-123", "XYZ", "local-data-store")
                .unwrap();

        let data_set = data_store.db_access.get_data_set().unwrap();
        assert_eq!(data_set.unique_name, "XYZ-123");
        assert_eq!(data_set.human_name, "XYZ");

        let this_data_store = data_store.db_access.get_this_data_store().unwrap();
        assert_eq!(
            this_data_store.path_on_device,
            test_dir.path().canonicalize().unwrap().to_str().unwrap()
        );
        assert_eq!(this_data_store.human_name, "local-data-store");
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
        let data_store_1 =
            DataStore::create_with_fs("", "XYZ", "XYZ", "local-data-store", in_memory_fs.clone())
                .unwrap();

        // Initial data set
        in_memory_fs.create_dir("sUb-1").unwrap();
        in_memory_fs.create_dir("sUb-1/sub-1-1").unwrap();
        in_memory_fs.create_dir("sUb-2").unwrap();

        in_memory_fs.create_file("file-1").unwrap();
        in_memory_fs.create_file("file-2").unwrap();
        in_memory_fs.create_file("sUb-1/file-1").unwrap();

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
        assert_eq!(data_store_1.local_version().unwrap(), 7);

        // Detect new and changed files
        in_memory_fs.create_file("file-3").unwrap();
        in_memory_fs
            .test_set_file_content("file-1", Vec::from("hello"))
            .unwrap();
        in_memory_fs.test_increase_file_mod_time("file-1").unwrap();

        let changes = data_store_1.perform_full_scan().unwrap();
        assert_eq!(
            changes,
            ScanResult {
                indexed_items: 8,
                changed_items: 1,
                new_items: 1,
                deleted_items: 0
            }
        );
        assert_eq!(data_store_1.local_version().unwrap(), 9);

        // Detect deleted files and directories
        in_memory_fs.remove_file("file-1").unwrap();
        in_memory_fs.remove_file("sUb-1/file-1").unwrap();
        in_memory_fs.remove_dir("sUb-1/sub-1-1").unwrap();
        in_memory_fs.remove_dir("sUb-1").unwrap();

        let changes = data_store_1.perform_full_scan().unwrap();
        assert_eq!(
            changes,
            ScanResult {
                indexed_items: 4,
                changed_items: 0,
                new_items: 0,
                deleted_items: 4
            }
        );
        assert_eq!(data_store_1.local_version().unwrap(), 9);
    }
}
