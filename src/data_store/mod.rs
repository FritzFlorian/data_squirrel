use chrono::NaiveDateTime;
use fs_interaction::relative_path::RelativePath;
use std::collections::HashSet;
use std::path::Path;

use crate::fs_interaction;
use crate::fs_interaction::virtual_fs;
use crate::fs_interaction::FSInteraction;
use crate::metadata_db;
use crate::metadata_db::MetadataDB;

mod synchronization_messages;
use self::synchronization_messages::*;

#[derive(Debug)]
pub enum DataStoreError {
    DataStoreNotSetup,
    FSInteractionError {
        source: fs_interaction::FSInteractionError,
    },
    MetadataDBError {
        source: metadata_db::MetadataDBError,
    },
    UnexpectedState {
        source: &'static str,
    },
    SyncError {
        message: &'static str,
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
    /// Same as open_with_fs, but uses the default FS abstraction (OS native calls).
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_fs(&path, FS::default())
    }
    /// Opens a data_store at a given path on the local disk.
    /// Makes sure that the required metadata directories and database are present.
    ///
    /// Returns errors if the data_store is already opened or does not exist.
    pub fn open_with_fs(path: &Path, fs: FS) -> Result<Self> {
        let fs_interaction = FSInteraction::open_with_fs(&path, fs)?;
        let metadata_db = MetadataDB::open(fs_interaction.metadata_db_path().to_str().unwrap())?;

        Ok(Self {
            fs_access: fs_interaction,
            db_access: metadata_db,
        })
    }

    /// Same as create_with_fs, but uses the default FS abstraction (OS native FS calls).
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
    /// Creates a new data_store at the given path on disk.
    /// Requires to be connected to a data_set by a unique identifier.
    /// Can be initialized with different FS abstractions (e.g. for testing).
    ///
    /// Returns errors if e.g. the data_store already exists.
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
            time: 0,
        })?;

        Ok(Self {
            fs_access: fs_interaction,
            db_access: metadata_db,
        })
    }

    /// The local, logical time maintained in this data_store.
    pub fn local_time(&self) -> Result<i64> {
        Ok(self.db_access.get_this_data_store()?.time)
    }

    /// Re-indexes the data stored in this data_store.
    ///
    /// Traverses the data directory and performs the following actions for the metadata DB:
    /// 1) Adds new files (not previously in the DB)
    /// 2) Updates modified files (their content AND modification times changed)
    /// 3) Removes missing files (previously in the DB, but not on disk anymore)
    /// 4) optionally alerts about bit-rot (content changed, but modification times did not)
    ///
    /// While doing these actions at all times the modification times in the DB are kept up to date,
    /// i.e. the local time counter is kept and attached to new or changed files.
    pub fn perform_full_scan(&self) -> Result<ScanResult> {
        let root_path = RelativePath::from_path("");
        let root_metadata = self.fs_access.metadata(&root_path)?;

        self.perform_scan(&root_path, &root_metadata)
    }

    pub fn request_synchronization(&self, sync_request: SyncRequest) -> Result<SyncResponse> {
        let local_item = self
            .db_access
            .get_local_data_item(&sync_request.item_path)?;
        let remote_sync_time = self
            .db_access
            .named_to_id_version_vector(&sync_request.dir_sync_time)?;

        if local_item.is_deletion() {
            Ok(SyncResponse {
                item_path: sync_request.item_path.clone(),
                sync_time: self
                    .db_access
                    .id_to_named_version_vector(&local_item.sync_time())?,
                action: SyncResponseAction::UpdateRequired(SyncUpdateContent::Deletion),
            })
        } else if local_item.mod_time() <= &remote_sync_time {
            Ok(SyncResponse {
                item_path: sync_request.item_path.clone(),
                sync_time: self
                    .db_access
                    .id_to_named_version_vector(&local_item.sync_time())?,
                action: SyncResponseAction::UpToDate,
            })
        } else {
            let update_content = match &local_item.content {
                metadata_db::ItemType::FILE {
                    mod_time, metadata, ..
                } => SyncUpdateContent::File {
                    mod_time: self.db_access.id_to_named_version_vector(&mod_time)?,
                    metadata: metadata.as_ref().unwrap().clone(),
                },
                metadata_db::ItemType::FOLDER {
                    mod_time, metadata, ..
                } => {
                    let child_items = self
                        .db_access
                        .get_local_child_data_items(&sync_request.item_path)?;
                    SyncUpdateContent::Folder {
                        mod_time: self.db_access.id_to_named_version_vector(&mod_time)?,
                        metadata: metadata.as_ref().unwrap().clone(),
                        child_items: child_items
                            .into_iter()
                            .map(|item| item.path_component)
                            .collect(),
                    }
                }
                metadata_db::ItemType::DELETION { .. } => panic!("We should never reach this!"),
            };

            Ok(SyncResponse {
                item_path: sync_request.item_path.clone(),
                sync_time: self
                    .db_access
                    .id_to_named_version_vector(&local_item.sync_time())?,
                action: SyncResponseAction::UpdateRequired(update_content),
            })
        }
    }

    pub fn synchronize_from_other_store(
        &self,
        from_other: &Self,
        path: &RelativePath,
    ) -> Result<()> {
        // This is a first version of a synchronization between two data stores that skips
        // most of the message-passing required to abstract this to a remote sync.
        // We still try to keep distinct interaction points between the stores, i.e. we try to
        // never directly read properties of the other store but only call methods on it.
        let local_data_set = self.db_access.get_data_set()?;
        let remote_data_set = from_other.get_data_set()?;

        if local_data_set.unique_name != remote_data_set.unique_name {
            return Err(DataStoreError::SyncError {
                message: "Must only sync matching data_sets!",
            });
        }

        // TODO: pull this step into 'preparation' phase, together with a translation table
        //       for remote to local data-store ID's to not send the full identifiers all the time.
        // STEP 0) Preparation: make sure we know all data_stores that other knows off.
        let other_data_stores = from_other.get_data_stores()?;
        for data_store in other_data_stores {
            // TODO: properly handle updates of data stores, we currently simply ignore the
            //       error for duplicated data_stores in the DB.
            self.db_access
                .create_data_store(&metadata_db::data_store::InsertFull {
                    data_set_id: local_data_set.id,

                    unique_name: &data_store.unique_name,
                    human_name: &data_store.human_name,

                    is_this_store: false,
                    creation_date: &data_store.creation_date,
                    path_on_device: &data_store.path_on_device,
                    location_note: &data_store.location_note,

                    time: 0,
                })
                .ok();
        }

        // STEP 1) Perform the synchronization request to the other data_store.
        let local_item = self.db_access.get_local_data_item(&path)?;
        let sync_request = SyncRequest {
            item_path: path.clone(),
            dir_sync_time: self
                .db_access
                .id_to_named_version_vector(&local_item.sync_time())?,
        };
        let sync_response = from_other.request_synchronization(sync_request)?;

        // STEP 2) Use the response in combination with our local knowledge to perform the actual
        //         synchronization actions (e.g. report conflicts).
        match &sync_response.action {
            SyncResponseAction::UpToDate => {
                // TODO: Update all sync times (for this item and all child items).
            }
            SyncResponseAction::UpdateRequired(sync_content) => {
                match sync_content {
                    SyncUpdateContent::Deletion => {
                        // if local_creation <= remote_deletion_sync
                        //      if local_modification <= remote_sync
                        //          the deletion covers all changes, delete our local item
                        //      else
                        //          the other store deleted the file, but we have newer changes,
                        //          report a conflict!
                        //  else
                        //      all good, the deletion notice does not regard our file
                    }
                    SyncUpdateContent::File { .. } => {
                        // if remote_mod_time <= local_sync_time
                        //      we are up-to date, simply take the sync time
                        // else if local_mod_time <= remote_sync_time
                        //      the remote is newer and knows all our history,
                        //      copy file over to us.
                        // else
                        //      neither version dominates, report conflict
                    }
                    SyncUpdateContent::Folder { .. } => {
                        // Make sure the folder exists.
                        // In case it was a file before, it is going to be deleted
                        // (with care not to have conflicting versions).
                        // In case nothing was there before, we create the folder but DO NOT
                        // add any notices on mod's/syc's to it (it simply has the default parent
                        // sync time).

                        // if local_mod_time <= remote_sync_time
                        //      we got all changes
                        // else
                        //      for each child recurse into sync-routine

                        // AFTER all sub-items are in sync, add the sync time of the remote
                        // folder into this folder.
                    }
                }
            }
        }

        Ok(())
    }

    fn get_data_stores(&self) -> metadata_db::Result<Vec<metadata_db::DataStore>> {
        self.db_access.get_data_stores()
    }

    fn get_data_set(&self) -> metadata_db::Result<metadata_db::DataSet> {
        self.db_access.get_data_set()
    }

    pub fn sync_file_from_other(&self, _other: &Self, _path: &Path) -> Result<()> {
        // Check if we need to do anything (STEP 1).
        // TODO: This should be step 1, i.e. check if we must copy over the file at all based on
        //       information on a remote sync time.
        // TODO: In the future we should also handle partial databases, i.e. transfers that might
        //       not store all files and thus have not all sync/mod times present.
        // if other.mod <= this.sync -> do nothing and return
        // TODO: Once we determine we want to transfer it, pack it up into a 'sendable unit'.

        // We actually need to perform some syncing (STEP 2).
        // TODO: This should be step 2, i.e. take the previous package and apply it to the 'remote'
        //       target store that we (previously) decided that wants our item.
        // if is directory -> would need to recurs into it
        // TODO: The recursing into the directory can be decided on a remote sending side only
        //       with the sync times and local modification times.
        // if this.dose_not_exists
        {
            // if other.creating_time <= this.sync -> independent creating, we should copy the file
            // else -> conflict, we deleted a file that was modified in the other store
        }
        // if this.does_exists
        {
            // if this.mod <= other.sync -> we should copy the file, it is derived from the local file
            // else -> conflict, concurrent modifications to the file
        }

        // TODO: The actual copy of the file contents should be step 3, as there might not always
        //       be the need to copy over the whole file in advance.

        // In any case, update the mod time to match the new version and set the sync
        // time to be the element wise maximum of the previous sync times.
        // Run algorithm to keep the database consistent (mod and sync times of parent items).

        Ok(())
    }

    ///////////////////////////////////
    // 'private' helpers start here
    ///////////////////////////////////

    fn fs_to_date_time(fs_time: &filetime::FileTime) -> NaiveDateTime {
        NaiveDateTime::from_timestamp(fs_time.unix_seconds(), fs_time.nanoseconds())
    }

    fn index_dir(
        &self,
        path: &RelativePath,
        metadata: &virtual_fs::Metadata,
        data_store: &metadata_db::DataStore,
    ) -> Result<ScanResult> {
        let mut result = ScanResult::new();
        result.indexed_items += 1;

        let dir_creation_time = Self::fs_to_date_time(&metadata.creation_time());
        let dir_mod_time = Self::fs_to_date_time(&metadata.last_mod_time());

        let db_item = self.db_access.get_data_item(&data_store, &path)?;
        match db_item {
            metadata_db::Item {
                content: metadata_db::ItemType::FILE { .. },
                ..
            } => {
                // Delete existing file
                result.deleted_items += 1;
                self.db_access.delete_local_data_item(&path)?;
                // Create new dir entry
                result.new_items += 1;
                self.db_access.update_local_data_item(
                    &path,
                    dir_creation_time,
                    dir_mod_time,
                    false,
                    "",
                )?;
            }
            metadata_db::Item {
                content: metadata_db::ItemType::FOLDER { metadata, .. },
                ..
            } => {
                // Simply update the relevant metadata if it is out of sync.
                let metadata = metadata.unwrap();
                if metadata.mod_time != dir_mod_time
                    || metadata.creation_time != dir_creation_time
                    || metadata.case_sensitive_name != path.name()
                {
                    result.changed_items += 1;
                    self.db_access.update_local_data_item(
                        &path,
                        dir_creation_time,
                        dir_mod_time,
                        false,
                        "",
                    )?;
                }
            }
            metadata_db::Item {
                content: metadata_db::ItemType::DELETION { .. },
                ..
            } => {
                // Create new dir entry
                result.new_items += 1;
                self.db_access.update_local_data_item(
                    &path,
                    dir_creation_time,
                    dir_mod_time,
                    false,
                    "",
                )?;
            }
        }

        Ok(result)
    }

    fn index_file(
        &self,
        path: &RelativePath,
        metadata: &virtual_fs::Metadata,
        data_store: &metadata_db::DataStore,
        detect_bitrot: bool,
    ) -> Result<ScanResult> {
        let mut result = ScanResult::new();
        result.indexed_items += 1;

        let file_creation_time = Self::fs_to_date_time(&metadata.creation_time());
        let file_mod_time = Self::fs_to_date_time(&metadata.last_mod_time());

        let db_item = self.db_access.get_data_item(&data_store, path)?;
        match db_item {
            metadata_db::Item {
                content: metadata_db::ItemType::FILE { metadata, .. },
                ..
            } => {
                let metadata = metadata.unwrap();
                // We got an existing entry, see if it requires updating.
                if metadata.creation_time != file_creation_time
                    || metadata.mod_time != file_mod_time
                    || metadata.case_sensitive_name != path.name()
                {
                    use data_encoding::HEXUPPER;
                    let hash = self.fs_access.calculate_hash(&path)?;
                    let hash = HEXUPPER.encode(hash.as_ref());

                    result.changed_items += 1;
                    self.db_access.update_local_data_item(
                        &path,
                        file_creation_time,
                        file_mod_time,
                        true,
                        &hash,
                    )?;
                } else if detect_bitrot {
                    use data_encoding::HEXUPPER;
                    let hash = self.fs_access.calculate_hash(&path)?;
                    let hash = HEXUPPER.encode(hash.as_ref());

                    if metadata.hash != hash {
                        // TODO: properly handle this by returning errors. Maybe re-trying to hash
                        //       the file in case this was simply a read issue.
                        panic!("Bitrot detected!")
                    }
                }
            }
            metadata_db::Item {
                content: metadata_db::ItemType::FOLDER { .. },
                ..
            } => {
                // FIXME: Handle if a folder is changed to be a file.
                panic!("Changing folders to files is not supported!");
            }
            metadata_db::Item {
                content: metadata_db::ItemType::DELETION { .. },
                ..
            } => {
                // We have no local entry for the target file in our DB, register it as a new file.
                result.new_items += 1;

                use data_encoding::HEXUPPER;
                let hash = self.fs_access.calculate_hash(&path)?;
                let hash = HEXUPPER.encode(hash.as_ref());

                self.db_access.update_local_data_item(
                    &path,
                    file_creation_time,
                    file_mod_time,
                    true,
                    &hash,
                )?;
            }
        }

        Ok(result)
    }

    fn perform_scan(
        &self,
        dir_path: &RelativePath,
        dir_metadata: &virtual_fs::Metadata,
    ) -> Result<ScanResult> {
        // We keep track of 'scan events' to have a rough output on a run of the scan function.
        let mut scan_result = ScanResult::new();
        let data_store = self.db_access.get_this_data_store()?;

        // Index the currently scanned dir (e.g. add it to the DB if it does not exist).
        // (We exclude the root directory, as we never collect metadata on it).
        if dir_path.get_path_components().len() > 1 {
            let dir_scan_result = &self.index_dir(&dir_path, &dir_metadata, &data_store)?;
            scan_result = scan_result.combine(dir_scan_result);
        }

        // Next, we index each file present on disk in this directory.
        // This is the 'positive' part of the scan operation, i.e. we add anything that is on
        // disk and not in the DB, as well as anything that has changed on dis.
        let items = self.fs_access.index(dir_path)?;
        let mut lower_case_entries = HashSet::with_capacity(items.len());
        for item in &items {
            lower_case_entries.insert(item.relative_path.name().to_lowercase());

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
        let child_items = self
            .db_access
            .get_child_data_items(&data_store, &dir_path)?;
        for child_item in child_items.iter() {
            if !lower_case_entries.contains(&child_item.path_component) {
                let child_item_path = dir_path.join(child_item.path_component.clone());
                scan_result.deleted_items +=
                    self.db_access.delete_local_data_item(&child_item_path)?;
            }
        }

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
                indexed_items: 6,
                changed_items: 0,
                new_items: 6,
                deleted_items: 0
            }
        );
        assert_eq!(data_store_1.local_time().unwrap(), 7);

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
                indexed_items: 7,
                changed_items: 1,
                new_items: 1,
                deleted_items: 0
            }
        );
        assert_eq!(data_store_1.local_time().unwrap(), 9);

        // Detect deleted files and directories
        in_memory_fs.remove_file("file-1").unwrap();
        in_memory_fs.remove_file("sUb-1/file-1").unwrap();
        in_memory_fs.remove_dir("sUb-1/sub-1-1").unwrap();
        in_memory_fs.remove_dir("sUb-1").unwrap();

        let changes = data_store_1.perform_full_scan().unwrap();
        assert_eq!(
            changes,
            ScanResult {
                indexed_items: 3,
                changed_items: 0,
                new_items: 0,
                deleted_items: 4
            }
        );
        assert_eq!(data_store_1.local_time().unwrap(), 13);

        // Re-add some
        in_memory_fs.create_file("file-1").unwrap();
        in_memory_fs.create_dir("sUb-1").unwrap();
        let changes = data_store_1.perform_full_scan().unwrap();
        assert_eq!(
            changes,
            ScanResult {
                indexed_items: 5,
                changed_items: 0,
                new_items: 2,
                deleted_items: 0
            }
        );
        assert_eq!(data_store_1.local_time().unwrap(), 15);

        // Changes in capitalization should be recognized as metadata changes
        in_memory_fs.remove_file("file-1").unwrap();
        in_memory_fs.remove_dir("sUb-1").unwrap();

        in_memory_fs.create_file("FILE-1").unwrap();
        in_memory_fs.create_dir("SUB-1").unwrap();
        let changes = data_store_1.perform_full_scan().unwrap();
        assert_eq!(
            changes,
            ScanResult {
                indexed_items: 5,
                changed_items: 2,
                new_items: 0,
                deleted_items: 0
            }
        );
        assert_eq!(data_store_1.local_time().unwrap(), 17);
    }
}
