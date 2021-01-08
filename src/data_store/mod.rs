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
use filetime::FileTime;
use version_vector::VersionVector;

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

    /// Ask the data store to synchronize a single item.
    /// The store will answer with all necessary information for the caller to perform the sync.
    pub fn request_sync(&self, sync_request: ExtSyncRequest) -> Result<ExtSyncResponse> {
        // We 'translate' the external representation of vector times and other
        // content that is dependent on local database id's to easily work with it.
        let int_sync_request = sync_request.internalize(&self.db_access)?;
        let int_sync_response = self.request_sync_int(int_sync_request)?;

        Ok(int_sync_response.externalize(&self.db_access)?)
    }
    pub fn request_sync_int(&self, sync_request: IntSyncRequest) -> Result<IntSyncResponse> {
        let local_item = self
            .db_access
            .get_local_data_item(&sync_request.item_path)?;

        if local_item.is_deletion() {
            Ok(IntSyncResponse {
                sync_time: local_item.sync_time,
                action: IntSyncAction::UpdateRequired(IntSyncContent::Deletion),
            })
        } else if local_item.mod_time() <= &sync_request.item_sync_time {
            Ok(IntSyncResponse {
                sync_time: local_item.sync_time,
                action: IntSyncAction::UpToDate,
            })
        } else {
            // The actual interesting case where a substantial update is required.
            match local_item.content {
                metadata_db::ItemType::FILE {
                    metadata: local_metadata,
                    creation_time: local_creation_time,
                    last_mod_time: local_last_mod_time,
                } => Ok(IntSyncResponse {
                    sync_time: local_item.sync_time,
                    action: IntSyncAction::UpdateRequired(IntSyncContent::File {
                        last_mod_time: local_last_mod_time,
                        creation_time: local_creation_time,
                        fs_metadata: local_metadata,
                    }),
                }),
                metadata_db::ItemType::FOLDER {
                    last_mod_time: local_last_mod_time,
                    metadata: local_metadata,
                    creation_time: local_creation_time,
                    ..
                } => {
                    let child_item_names = self
                        .db_access
                        .get_local_child_data_items(&sync_request.item_path)?
                        .into_iter()
                        .map(|item| item.path_component)
                        .collect();

                    Ok(IntSyncResponse {
                        sync_time: local_item.sync_time,
                        action: IntSyncAction::UpdateRequired(IntSyncContent::Folder {
                            last_mod_time: local_last_mod_time,
                            creation_time: local_creation_time,
                            fs_metadata: local_metadata,
                            child_items: child_item_names,
                        }),
                    })
                }
                metadata_db::ItemType::DELETION { .. } => panic!("We should never reach this!"),
            }
        }
    }

    // Synchronizes in the direction from_other -> self, i.e. self will contain all changes done
    // in from_other after the operation completes successfully.
    pub fn sync_from_other_store(&self, from_other: &Self, path: &RelativePath) -> Result<()> {
        // This is a first version of a synchronization between two data stores that skips
        // most of the message-passing required to abstract this to a remote sync.
        // We still try to keep distinct interaction points between the stores, i.e. we try to
        // never directly read properties of the other store but only call methods on it.
        // Later on, we can pull these method calls out into a message-exchange protocol.

        let local_data_set = self.db_access.get_data_set()?;
        let remote_data_set = from_other.get_data_set()?;

        if local_data_set.unique_name != remote_data_set.unique_name {
            return Err(DataStoreError::SyncError {
                message: "Must only sync matching data_sets!",
            });
        }

        // TODO: pull this step into 'preparation' phase, together with a translation table
        //       for remote to local data-store ID's to not send the full identifiers all the time.
        // STEP 0) Preparation: make sure we know all data_stores that other knows off
        //         (and the other way around)
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
        let local_data_stores = self.get_data_stores()?;
        for data_store in local_data_stores {
            // TODO: properly handle updates of data stores, we currently simply ignore the
            //       error for duplicated data_stores in the DB.
            from_other
                .db_access
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
        let sync_request = IntSyncRequest {
            item_path: path.clone(),
            item_sync_time: local_item.sync_time.clone(),
        };
        let sync_request = sync_request.externalize(&self.db_access)?;

        let sync_response = from_other.request_sync(sync_request)?;
        let sync_response = sync_response.internalize(&self.db_access)?;

        // STEP 2) Use the response in combination with our local knowledge to perform the actual
        //         synchronization actions (e.g. report conflicts).
        match sync_response.action {
            IntSyncAction::UpToDate => {
                // If we are up-to-date it is rather simple, we integrate the knowledge that
                // of the other device on 'how up to date' the directory is and we are done.
                self.db_access
                    .max_sync_times_recursive(&path, &sync_response.sync_time)?;
            }
            IntSyncAction::UpdateRequired(sync_content) => {
                match sync_content {
                    IntSyncContent::Deletion => {
                        if local_item.is_deletion() {
                            // Both agree that the file should be deleted. Ignore any potential
                            // conflicts, just settle and be happy that we agree on the state.
                            self.db_access
                                .max_sync_times_recursive(&path, &sync_response.sync_time)?;
                        } else if local_item.creation_time() <= &sync_response.sync_time {
                            // The remote deletion notice is targeting our local file/folder.
                            if local_item.mod_time() <= &sync_response.sync_time {
                                // The remote deletion notice knows of all our changes.
                                // Delete the actual item on disk...
                                if local_item.is_file() {
                                    self.fs_access.delete_file(&path)?;
                                } else {
                                    self.fs_access.delete_directory(&path)?;
                                }

                                // ...and insert the appropriate deletion notice into our local db.
                                let target_item = metadata_db::DBItem {
                                    path_component: path.name().to_owned(),
                                    sync_time: sync_response.sync_time.clone(),

                                    content: metadata_db::ItemType::DELETION,
                                };
                                self.db_access.sync_local_data_item(&path, &target_item)?;
                            } else {
                                panic!("Detected sync-conflict!");
                            }
                        } else {
                            // The deletion notice does not 'target' our file, i.e. it does
                            // not know about our local file, as the local file was created
                            // logically independent of the other copy.
                            // Just do nothing more than take up the target sync time.
                            self.db_access
                                .max_sync_times_recursive(&path, &sync_response.sync_time)?;
                        }
                    }
                    IntSyncContent::File {
                        last_mod_time: response_last_mod_time,
                        fs_metadata: response_metadata,
                        creation_time: response_creation_time,
                    } => {
                        if local_item.is_deletion()
                            && response_creation_time <= local_item.sync_time
                        {
                            panic!("Detected sync-conflict!");
                        } else if local_item.is_deletion()
                            || local_item.mod_time() <= &sync_response.sync_time
                        {
                            // The remote is newer and knows all of our local changes,
                            // thus we can safely take the remote file version.

                            // ...download file.
                            let tmp_file_path = self.download_file(&from_other, &path)?;
                            self.fs_access.set_metadata(
                                &tmp_file_path,
                                FileTime::from_unix_time(
                                    response_metadata.creation_time.timestamp(),
                                    response_metadata.creation_time.timestamp_subsec_nanos(),
                                ),
                                false,
                            )?;

                            // ...replace local.
                            match &local_item.content {
                                metadata_db::ItemType::FILE { .. } => {
                                    self.fs_access.delete_file(&path)?
                                }
                                metadata_db::ItemType::FOLDER { .. } => {
                                    self.fs_access.delete_directory(&path)?
                                }
                                metadata_db::ItemType::DELETION { .. } => (), // Nothing to do,
                            }
                            self.fs_access
                                .rename_file_or_directory(&tmp_file_path, &path)?;

                            // Insert the appropriate file item into our local db.
                            let target_item = metadata_db::DBItem {
                                path_component: path.name().to_owned(),
                                sync_time: sync_response.sync_time,
                                content: metadata_db::ItemType::FILE {
                                    metadata: response_metadata,
                                    creation_time: response_creation_time,
                                    last_mod_time: response_last_mod_time,
                                },
                            };
                            self.db_access.sync_local_data_item(&path, &target_item)?;
                        } else {
                            panic!("Detected sync-conflict!");
                        }
                    }
                    IntSyncContent::Folder {
                        last_mod_time: response_last_mod_time,
                        creation_time: response_creation_time,
                        fs_metadata: response_metadata,
                        child_items: response_child_items,
                    } => {
                        if local_item.is_deletion()
                            && response_creation_time <= local_item.sync_time
                        {
                            panic!("Detected sync-conflict!");
                        } else {
                            // Make sure the folder exists.

                            // In case it was a file before, it is going to be deleted.
                            if local_item.is_file() {
                                self.fs_access.delete_file(&path)?;
                            }

                            // In case nothing was there before, we create the folder but DO NOT
                            // add any notices on mod's/syc's to it (will be done AFTER the sync).
                            if !local_item.is_folder() {
                                self.fs_access.create_dir(&path)?;
                                self.fs_access.set_metadata(
                                    &path,
                                    FileTime::from_unix_time(
                                        response_metadata.creation_time.timestamp(),
                                        response_metadata.creation_time.timestamp_subsec_nanos(),
                                    ),
                                    false,
                                )?;

                                // FIXME: Handle conflicts between folders, deletions and files.
                                //        Also ook into how is the creator of a file when
                                //        we perform a sync!
                                let folder_before_sync = metadata_db::DBItem {
                                    path_component: path.name().to_owned(),
                                    sync_time: local_item.sync_time,
                                    content: metadata_db::ItemType::FOLDER {
                                        metadata: response_metadata.clone(),
                                        creation_time: response_creation_time.clone(),
                                        last_mod_time: response_creation_time.clone(),
                                        mod_time: VersionVector::new(),
                                    },
                                };
                                self.db_access
                                    .sync_local_data_item(&path, &folder_before_sync)?;
                            }

                            // Recurse into items present on the other store...
                            let mut visited_items =
                                HashSet::with_capacity(response_child_items.len());
                            for remote_child_item in response_child_items {
                                visited_items.insert(remote_child_item.clone());

                                self.sync_from_other_store(
                                    &from_other,
                                    &path.join(remote_child_item),
                                )?;
                            }
                            // ...and also into local items (these should simply get deleted,
                            // but we can optimize this later on after the basic works).
                            for local_child in self.db_access.get_local_child_data_items(&path)? {
                                if !visited_items.contains(&local_child.path_component) {
                                    self.sync_from_other_store(
                                        &from_other,
                                        &path.join(local_child.path_component),
                                    )?;
                                }
                            }
                        }
                        // AFTER all sub-items are in sync, add the sync time of the remote
                        // folder into this folder.
                        // FIXME: it is simply not good to differentiate between root and
                        //        non-root folders in our code...
                        if path.get_path_components().len() > 1 {
                            let folder_after_sync = metadata_db::DBItem {
                                path_component: path.name().to_owned(),
                                sync_time: sync_response.sync_time,
                                content: metadata_db::ItemType::FOLDER {
                                    metadata: response_metadata,
                                    creation_time: response_creation_time,
                                    last_mod_time: response_last_mod_time,
                                    mod_time: VersionVector::new(),
                                },
                            };
                            self.db_access
                                .sync_local_data_item(&path, &folder_after_sync)?;
                        } else {
                            // FIXME: Remove this 'hack' for the root item
                            self.db_access
                                .max_sync_times_recursive(&path, &sync_response.sync_time)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn download_file(&self, other: &Self, path: &RelativePath) -> Result<RelativePath> {
        use data_encoding::HEXUPPER;
        use ring::digest::{Context, SHA256};

        let mut context = Context::new(&SHA256);
        for path_component in path.get_path_components() {
            context.update(path_component.as_bytes());
        }
        let hash = context.finish();
        let path_hash = HEXUPPER.encode(hash.as_ref());

        let target_local_path = self.fs_access.pending_files_relative().join_mut(path_hash);

        // TODO: This should later on be further abstracted to allow actual downloads/streaming.
        let stream_from_other = other.fs_access.read_file(&path)?;

        self.fs_access.create_file(&target_local_path)?;
        self.fs_access
            .write_file(&target_local_path, stream_from_other)?;

        Ok(target_local_path)
    }

    fn get_data_stores(&self) -> metadata_db::Result<Vec<metadata_db::DataStore>> {
        self.db_access.get_data_stores()
    }

    fn get_data_set(&self) -> metadata_db::Result<metadata_db::DataSet> {
        self.db_access.get_data_set()
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
    ) -> Result<ScanResult> {
        let mut result = ScanResult::new();
        result.indexed_items += 1;

        let dir_creation_time = Self::fs_to_date_time(&metadata.creation_time());
        let dir_mod_time = Self::fs_to_date_time(&metadata.last_mod_time());

        let db_item = self.db_access.get_local_data_item(&path)?;
        match db_item {
            metadata_db::DBItem {
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
            metadata_db::DBItem {
                content: metadata_db::ItemType::FOLDER { metadata, .. },
                ..
            } => {
                // Simply update the relevant metadata if it is out of sync.
                if metadata.mod_time != dir_mod_time || metadata.case_sensitive_name != path.name()
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
            metadata_db::DBItem {
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
        detect_bitrot: bool,
    ) -> Result<ScanResult> {
        let mut result = ScanResult::new();
        result.indexed_items += 1;

        let file_creation_time = Self::fs_to_date_time(&metadata.creation_time());
        let file_mod_time = Self::fs_to_date_time(&metadata.last_mod_time());

        let db_item = self.db_access.get_local_data_item(path)?;
        match db_item {
            metadata_db::DBItem {
                content: metadata_db::ItemType::FILE { metadata, .. },
                ..
            } => {
                // We got an existing entry, see if it requires updating.
                if metadata.mod_time != file_mod_time || metadata.case_sensitive_name != path.name()
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
            metadata_db::DBItem {
                content: metadata_db::ItemType::FOLDER { .. },
                ..
            } => {
                // FIXME: Handle if a folder is changed to be a file.
                panic!("Changing folders to files is not supported!");
            }
            metadata_db::DBItem {
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

        // Index the currently scanned dir (e.g. add it to the DB if it does not exist).
        // (We exclude the root directory, as we never collect metadata on it).
        if dir_path.get_path_components().len() > 1 {
            let dir_scan_result = &self.index_dir(&dir_path, &dir_metadata)?;
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
                        let file_scan_result =
                            self.index_file(&item.relative_path, &item_metadata, false)?;
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
        let child_items = self.db_access.get_local_child_data_items(&dir_path)?;
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
        in_memory_fs.create_dir("sUb-1", false).unwrap();
        in_memory_fs.create_dir("sUb-1/sub-1-1", false).unwrap();
        in_memory_fs.create_dir("sUb-2", false).unwrap();

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
        assert_eq!(data_store_1.local_time().unwrap(), 6);

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
        assert_eq!(data_store_1.local_time().unwrap(), 8);

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
        assert_eq!(data_store_1.local_time().unwrap(), 12);

        // Re-add some
        in_memory_fs.create_file("file-1").unwrap();
        in_memory_fs.create_dir("sUb-1", false).unwrap();
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
        assert_eq!(data_store_1.local_time().unwrap(), 14);

        // Changes in capitalization should be recognized as metadata changes
        in_memory_fs.remove_file("file-1").unwrap();
        in_memory_fs.remove_dir("sUb-1").unwrap();

        in_memory_fs.create_file("FILE-1").unwrap();
        in_memory_fs.create_dir("SUB-1", false).unwrap();
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
        assert_eq!(data_store_1.local_time().unwrap(), 16);
    }

    #[test]
    fn unidirectional_sync() {
        let fs_1 = virtual_fs::InMemoryFS::new();
        let data_store_1 =
            DataStore::create_with_fs("", "XYZ", "XYZ", "source-data-store", fs_1.clone()).unwrap();
        let fs_2 = virtual_fs::InMemoryFS::new();
        let data_store_2 =
            DataStore::create_with_fs("", "XYZ", "XYZ", "dest-data-store", fs_2.clone()).unwrap();

        // Initial Data Set - Local Data Store
        fs_1.create_dir("sub-1", false).unwrap();
        fs_1.create_dir("sub-1/sub-1-1", false).unwrap();
        fs_1.create_dir("sub-2", false).unwrap();
        fs_1.create_file("file-1").unwrap();
        fs_1.create_file("file-2").unwrap();
        fs_1.create_file("sub-1/file-1").unwrap();

        // Index it and sync it to the remote data store
        data_store_1.perform_full_scan().unwrap();
        data_store_2.perform_full_scan().unwrap();
        data_store_2
            .sync_from_other_store(&data_store_1, &RelativePath::from_path(""))
            .unwrap();

        // We should have the files on the second store
        let root_dir_entries = fs_2.list_dir("").unwrap();
        assert_eq!(root_dir_entries.len(), 5);
        assert!(root_dir_entries
            .iter()
            .any(|item| item.file_name == "sub-1"));
        assert!(root_dir_entries
            .iter()
            .any(|item| item.file_name == "sub-2"));
        assert!(root_dir_entries
            .iter()
            .any(|item| item.file_name == "file-1"));
        assert!(root_dir_entries
            .iter()
            .any(|item| item.file_name == "file-2"));
        let changes = data_store_2.perform_full_scan().unwrap();
        assert_eq!(
            changes,
            ScanResult {
                indexed_items: 6,
                changed_items: 0,
                new_items: 0,
                deleted_items: 0
            }
        );

        // Lets do some non-conflicting changes in both stores
        fs_2.test_set_file_content("file-2", "testing".to_owned().into_bytes())
            .unwrap();
        fs_2.test_increase_file_mod_time("file-2").unwrap();

        fs_1.create_file("file-3").unwrap();
        fs_1.remove_file("file-1").unwrap();

        // Fully scan and sync them
        data_store_1.perform_full_scan().unwrap();
        data_store_2.perform_full_scan().unwrap();
        data_store_2
            .sync_from_other_store(&data_store_1, &RelativePath::from_path(""))
            .unwrap();
        data_store_1
            .sync_from_other_store(&data_store_2, &RelativePath::from_path(""))
            .unwrap();

        // The contents should now match without any conflicts
        let root_dir_entries_1 = fs_1.list_dir("").unwrap();
        let root_dir_entries_2 = fs_2.list_dir("").unwrap();
        assert_eq!(root_dir_entries_1.len(), 5);
        assert!(root_dir_entries_1
            .iter()
            .any(|item| item.file_name == "sub-1"));
        assert!(root_dir_entries_1
            .iter()
            .any(|item| item.file_name == "sub-2"));
        assert!(root_dir_entries_1
            .iter()
            .any(|item| item.file_name == "file-2"));
        assert!(root_dir_entries_1
            .iter()
            .any(|item| item.file_name == "file-3"));
        assert_eq!(root_dir_entries_2.len(), 5);
        assert!(root_dir_entries_2
            .iter()
            .any(|item| item.file_name == "sub-1"));
        assert!(root_dir_entries_2
            .iter()
            .any(|item| item.file_name == "sub-2"));
        assert!(root_dir_entries_2
            .iter()
            .any(|item| item.file_name == "file-2"));
        assert!(root_dir_entries_2
            .iter()
            .any(|item| item.file_name == "file-3"));
    }
}
