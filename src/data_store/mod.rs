use chrono::NaiveDateTime;
use filetime::FileTime;
use std::collections::HashSet;
use std::path::Path;

use crate::fs_interaction::relative_path::RelativePath;
use crate::fs_interaction::virtual_fs;
use crate::fs_interaction::FSInteraction;
use crate::metadata_db;
use crate::metadata_db::MetadataDB;
use crate::version_vector::VersionVector;

mod synchronization_messages;
use self::synchronization_messages::*;
mod scan_result;
pub use self::scan_result::ScanResult;
mod errors;
pub use self::errors::*;

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
        Ok(self.db_access.get_local_data_store()?.time)
    }

    // The unique name of the data set. Must equal the unique name of any sync partner.
    pub fn data_set_name(&self) -> Result<String> {
        Ok(self.db_access.get_data_set()?.unique_name)
    }

    // The unique name of this local data store. Must be unique throughout all sync partners.
    pub fn local_data_store_name(&self) -> Result<String> {
        Ok(self.db_access.get_local_data_store()?.unique_name)
    }

    // The human readable description of this local data store.
    pub fn local_data_store_desc(&self) -> Result<String> {
        Ok(self.db_access.get_local_data_store()?.human_name)
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

    /// Includes the data stores given into the local database and returns a list of all
    /// stores known after the operation.
    /// This should be done before a item or folder is synced to make sure both data stores
    /// know about the same data stores related to the given data set.
    pub fn sync_data_store_list(&self, sync_handshake: SyncHandshake) -> Result<SyncHandshake> {
        let local_data_set = self.get_data_set()?;
        if local_data_set.unique_name != sync_handshake.data_set_name {
            return Err(DataStoreError::SyncError {
                message: "Must only sync matching data_sets!",
            });
        }

        for remote_data_store in sync_handshake.data_stores {
            let local_data_store = self
                .db_access
                .get_data_store(&remote_data_store.unique_name)?;
            if local_data_store.is_none() {
                self.db_access
                    .create_data_store(&metadata_db::data_store::InsertFull {
                        data_set_id: local_data_set.id,
                        unique_name: &remote_data_store.unique_name,
                        human_name: &remote_data_store.human_name,
                        creation_date: &remote_data_store.creation_date,
                        path_on_device: &remote_data_store.path_on_device,
                        location_note: &remote_data_store.location_note,
                        is_this_store: false,
                        time: remote_data_store.time,
                    })?;
            }
        }

        Ok(SyncHandshake {
            data_set_name: local_data_set.unique_name,
            data_stores: self.db_access.get_data_stores()?,
        })
    }

    /// Ask the data store to synchronize a single item.
    /// The store will answer with all necessary information for the caller to perform the sync.
    pub fn sync_item(
        &self,
        sync_request: ExtSyncRequest,
        mapper: &DataStoreIDMapper,
    ) -> Result<ExtSyncResponse> {
        // We 'translate' the external representation of vector times and other
        // content that is dependent on local database id's to easily work with it.
        let int_sync_request = sync_request.internalize(&mapper);
        let int_sync_response = self.sync_item_internal(int_sync_request)?;

        Ok(int_sync_response.externalize(&mapper))
    }
    pub fn sync_item_internal(&self, sync_request: IntSyncRequest) -> Result<IntSyncResponse> {
        if !self.does_disk_item_match_db_item(&sync_request.item_path)? {
            panic!("Must not sync if disk content is not correctly indexed in DB.");
        }

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
                        .map(|item| item.path.name().to_owned())
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
        // Step 0) Handshake so both stores know about the same data_stores and can map their
        //         data base ID's to each others local view.
        let (local_mapper, remote_mapper) = self.sync_data_store_lists(&from_other)?;

        // Perform Actual Synchronization
        self.sync_from_other_store_recursive(&from_other, &path, &local_mapper, &remote_mapper)
    }

    fn sync_data_store_lists(
        &self,
        remote: &Self,
    ) -> Result<(DataStoreIDMapper, DataStoreIDMapper)> {
        let local_data_set = self.db_access.get_data_set()?;
        let local_sync_handshake = SyncHandshake {
            data_set_name: local_data_set.unique_name.clone(),
            data_stores: self.db_access.get_data_stores()?,
        };
        let remote_data_set = remote.get_data_set()?;
        let remote_sync_handshake = SyncHandshake {
            data_set_name: remote_data_set.unique_name,
            data_stores: remote.db_access.get_data_stores()?,
        };

        let local_response = remote.sync_data_store_list(local_sync_handshake)?;
        let remote_response = self.sync_data_store_list(remote_sync_handshake)?;
        let local_mapper = DataStoreIDMapper::create_mapper(&self.db_access, local_response)?;
        let remote_mapper = DataStoreIDMapper::create_mapper(&remote.db_access, remote_response)?;

        Ok((local_mapper, remote_mapper))
    }

    fn sync_from_other_store_recursive(
        &self,
        from_other: &Self,
        path: &RelativePath,
        local_mapper: &DataStoreIDMapper,
        remote_mapper: &DataStoreIDMapper,
    ) -> Result<()> {
        // STEP 1) Perform the synchronization request to the other data_store.
        let local_item = self.db_access.get_local_data_item(&path)?;
        let localized_path = path
            .clone()
            .parent_mut()
            .join_mut(local_item.path.name().to_owned());
        let sync_request = IntSyncRequest {
            item_path: path.clone(),
            item_sync_time: local_item.sync_time.clone(),
        };
        let sync_request = sync_request.externalize(&local_mapper);

        let sync_response = from_other.sync_item(sync_request, &remote_mapper)?;
        let sync_response = sync_response.internalize(&local_mapper);

        // STEP 2) Use the response in combination with our local knowledge to perform the actual
        //         synchronization actions (e.g. report conflicts).
        match sync_response.action {
            IntSyncAction::UpToDate => {
                // If we are up-to-date it is rather simple, we integrate the knowledge that
                // of the other device on 'how up to date' the directory is and we are done.
                let mut target_item = local_item;
                target_item.sync_time.max(&sync_response.sync_time);
                self.db_access
                    .sync_local_data_item(&localized_path, &target_item)?;
            }
            IntSyncAction::UpdateRequired(sync_content) => {
                if !self.does_disk_item_match_db_item(&localized_path)? {
                    panic!("Must not sync if disk content is not correctly indexed in DB.");
                }

                match sync_content {
                    IntSyncContent::Deletion => {
                        if local_item.is_deletion() {
                            // Both agree that the file should be deleted. Ignore any potential
                            // conflicts, just settle and be happy that we agree on the state.
                            let mut target_item = local_item;
                            target_item.sync_time.max(&sync_response.sync_time);
                            self.db_access
                                .sync_local_data_item(&localized_path, &target_item)?;
                        } else if local_item.creation_time() <= &sync_response.sync_time {
                            // The remote deletion notice is targeting our local file/folder.
                            if local_item.mod_time() <= &sync_response.sync_time {
                                // The remote deletion notice knows of all our changes.
                                // Delete the actual item on disk...
                                if local_item.is_file() {
                                    self.fs_access.delete_file(&localized_path)?;
                                } else {
                                    self.fs_access.delete_directory(&localized_path)?;
                                }

                                // ...and insert the appropriate deletion notice into our local db.
                                let target_item = metadata_db::DBItem {
                                    path: localized_path.clone(),
                                    sync_time: sync_response.sync_time.clone(),

                                    content: metadata_db::ItemType::DELETION,
                                };
                                self.db_access
                                    .sync_local_data_item(&localized_path, &target_item)?;
                            } else {
                                panic!("Detected sync-conflict!");
                            }
                        } else {
                            // The deletion notice does not 'target' our file, i.e. it does
                            // not know about our local file, as the local file was created
                            // logically independent of the other copy.
                            // Just do nothing more than take up the target sync time.
                            let mut target_item = local_item;
                            target_item.sync_time.max(&sync_response.sync_time);
                            self.db_access
                                .sync_local_data_item(&localized_path, &target_item)?;
                        }
                    }
                    IntSyncContent::File {
                        last_mod_time: remote_last_mod_time,
                        fs_metadata: remote_fs_metadata,
                        creation_time: remote_creation_time,
                    } => {
                        let remote_path = localized_path
                            .parent()
                            .join_mut(remote_fs_metadata.case_sensitive_name.clone());

                        if local_item.is_deletion() && remote_creation_time <= local_item.sync_time
                        {
                            // We know of the other file in our history and have deleted it.
                            // At the same time there is new data for this item on the remote...
                            panic!(
                                "Detected sync-conflict: Remote has changes on an item that was deleted locally!"
                            );
                        }
                        if !local_item.is_deletion()
                            && !(local_item.mod_time() <= &sync_response.sync_time)
                        {
                            // The remote has a new change, but does not know everything about
                            // our local changes...
                            panic!("Detected sync-conflict: Remote has changed an item concurrently to this data store!");
                        }

                        // ...download file.
                        let tmp_file_path = self.download_file(&from_other, &localized_path)?;
                        self.fs_access.set_metadata(
                            &tmp_file_path,
                            FileTime::from_unix_time(
                                remote_fs_metadata.mod_time.timestamp(),
                                remote_fs_metadata.mod_time.timestamp_subsec_nanos(),
                            ),
                            false,
                        )?;

                        // ...remove local file/folder with same name.
                        match &local_item.content {
                            metadata_db::ItemType::FILE { .. } => {
                                self.fs_access.delete_file(&localized_path)?
                            }
                            metadata_db::ItemType::FOLDER { .. } => {
                                self.fs_access.delete_directory(&localized_path)?
                            }
                            metadata_db::ItemType::DELETION { .. } => (), // Nothing to do,
                        }
                        // ... move the downloaded file over it.
                        self.fs_access
                            .rename_file_or_directory(&tmp_file_path, &remote_path)?;

                        // Insert the appropriate file item into our local db.
                        let target_item = metadata_db::DBItem {
                            path: localized_path.clone(),
                            sync_time: sync_response.sync_time,
                            content: metadata_db::ItemType::FILE {
                                metadata: remote_fs_metadata,
                                creation_time: remote_creation_time,
                                last_mod_time: remote_last_mod_time,
                            },
                        };
                        self.db_access
                            .sync_local_data_item(&localized_path, &target_item)?;
                    }
                    IntSyncContent::Folder {
                        last_mod_time: remote_last_mod_time,
                        creation_time: remote_creation_time,
                        fs_metadata: remote_fs_metadata,
                        child_items: remote_child_items,
                    } => {
                        let remote_path = localized_path
                            .parent()
                            .join_mut(remote_fs_metadata.case_sensitive_name.clone());

                        if local_item.is_deletion() && remote_creation_time <= local_item.sync_time
                        {
                            // We know of the other file in our history and have deleted it.
                            // At the same time there is new data for this item on the remote...
                            panic!(
                                "Detected sync-conflict: Remote has changes on an item that was deleted locally!"
                            );
                        }
                        if local_item.is_file()
                            && !(local_item.mod_time() <= &sync_response.sync_time)
                        {
                            // The remote has a new change, but does not know everything about
                            // our local changes...
                            panic!("Detected sync-conflict: Remote has changed an item concurrently to this data store!");
                        }

                        // Make sure the folder exists.
                        // In case it was a file before, it is going to be deleted.
                        if local_item.is_file() {
                            self.fs_access.delete_file(&localized_path)?;
                        }

                        // In case nothing was there before, we create the folder but DO NOT
                        // add any notices on mod's/syc's to it (will be done AFTER the sync).
                        if !local_item.is_folder() {
                            self.fs_access.create_dir(&remote_path)?;
                            self.fs_access.set_metadata(
                                &remote_path,
                                FileTime::from_unix_time(
                                    remote_fs_metadata.mod_time.timestamp(),
                                    remote_fs_metadata.mod_time.timestamp_subsec_nanos(),
                                ),
                                false,
                            )?;

                            let folder_before_sync = metadata_db::DBItem {
                                path: remote_path.clone(),
                                sync_time: local_item.sync_time.clone(),
                                content: metadata_db::ItemType::FOLDER {
                                    metadata: remote_fs_metadata.clone(),
                                    creation_time: remote_creation_time.clone(),
                                    last_mod_time: remote_creation_time.clone(),
                                    mod_time: VersionVector::new(),
                                },
                            };
                            self.db_access
                                .sync_local_data_item(&localized_path, &folder_before_sync)?;
                        }

                        // Recurse into items present on the other store...
                        let mut visited_items = HashSet::with_capacity(remote_child_items.len());
                        for remote_child_item in remote_child_items {
                            visited_items.insert(remote_child_item.to_lowercase());

                            self.sync_from_other_store_recursive(
                                &from_other,
                                &localized_path.join(remote_child_item),
                                &local_mapper,
                                &remote_mapper,
                            )?;
                        }
                        // ...and also into local items (these should simply get deleted,
                        // but we can optimize this later on after the basic works).
                        for local_child in
                            self.db_access.get_local_child_data_items(&localized_path)?
                        {
                            if !visited_items.contains(&local_child.path.name().to_lowercase()) {
                                self.sync_from_other_store_recursive(
                                    &from_other,
                                    &local_child.path,
                                    &local_mapper,
                                    &remote_mapper,
                                )?;
                            }
                        }

                        // AFTER all sub-items are in sync, add the sync time of the remote
                        // folder into this folder.
                        let folder_after_sync = metadata_db::DBItem {
                            path: remote_path.clone(),
                            sync_time: sync_response.sync_time,
                            content: metadata_db::ItemType::FOLDER {
                                metadata: remote_fs_metadata,
                                creation_time: remote_creation_time,
                                last_mod_time: remote_last_mod_time,
                                mod_time: VersionVector::new(),
                            },
                        };
                        // ...and make sure the local folder metadata (i.e. its name) is correct
                        // by renaming our local folder if required.
                        if local_item.is_folder() && local_item.path.name() != remote_path.name() {
                            self.fs_access
                                .rename_file_or_directory(&local_item.path, &remote_path)?;
                        }
                        self.db_access
                            .sync_local_data_item(&localized_path, &folder_after_sync)?;
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
        let other_db_item = other.db_access.get_local_data_item(&path)?;
        let stream_from_other = other.fs_access.read_file(&other_db_item.path)?;

        self.fs_access.create_file(&target_local_path)?;
        self.fs_access
            .write_file(&target_local_path, stream_from_other)?;

        Ok(target_local_path)
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

    /// Checks if the item on the given path is up to date in the database.
    /// If anything differs between the DB and disk content, false is returned.
    fn does_disk_item_match_db_item(&self, path: &RelativePath) -> Result<bool> {
        if path.path_component_number() <= 1 {
            // Skip root path
            return Ok(true);
        }
        let db_entry = self.db_access.get_local_data_item(&path)?;
        let db_path = &db_entry.path;

        let folder_content = self.fs_access.index(&db_path.parent());
        if folder_content.is_err() {
            if folder_content.as_ref().err().unwrap().is_io_not_found() {
                return Ok(db_entry.is_deletion());
            }
            if folder_content.as_ref().err().unwrap().is_io_no_directory() {
                return Ok(db_entry.is_deletion());
            }
            println!("We really do not want to be here...");
        }

        let folder_content = folder_content?;
        if db_entry.is_deletion() {
            let has_item_on_disk = folder_content.into_iter().any(|item| {
                item.relative_path.name().to_lowercase() == db_path.name().to_lowercase()
            });
            return Ok(!has_item_on_disk);
        }

        let matching_disk_entry = folder_content.into_iter().find(|item| {
            item.issues.is_empty()
                && item.relative_path.name() == db_entry.metadata().case_sensitive_name
        });
        if matching_disk_entry.is_none() {
            return Ok(false);
        }
        let disk_entry = matching_disk_entry.unwrap();

        let disk_metadata = disk_entry.metadata.unwrap();
        if Self::fs_to_date_time(&disk_metadata.last_mod_time()) != db_entry.metadata().mod_time {
            return Ok(false);
        }
        if disk_metadata.is_file() != db_entry.is_file()
            || disk_metadata.is_dir() != db_entry.is_folder()
        {
            return Ok(false);
        }

        if disk_metadata.is_file() {
            let hash = self.fs_access.calculate_hash(&db_path);
            if hash.is_err() || hash.unwrap() != db_entry.metadata().hash {
                return Ok(false);
            }
        }

        Ok(true)
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
        match db_item.content {
            metadata_db::ItemType::FILE { .. } => {
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
            metadata_db::ItemType::FOLDER { metadata, .. } => {
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
            metadata_db::ItemType::DELETION { .. } => {
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
        match db_item.content {
            metadata_db::ItemType::FILE { metadata, .. } => {
                // We got an existing entry, see if it requires updating.
                if metadata.mod_time != file_mod_time || metadata.case_sensitive_name != path.name()
                {
                    result.changed_items += 1;

                    let hash = self.fs_access.calculate_hash(&path)?;
                    self.db_access.update_local_data_item(
                        &path,
                        file_creation_time,
                        file_mod_time,
                        true,
                        &hash,
                    )?;
                } else if detect_bitrot {
                    let hash = self.fs_access.calculate_hash(&path)?;

                    if metadata.hash != hash {
                        // TODO: properly handle this by returning errors. Maybe re-trying to hash
                        //       the file in case this was simply a read issue.
                        panic!("Bitrot detected!")
                    }
                }
            }
            metadata_db::ItemType::FOLDER { .. } => {
                // FIXME: Handle if a folder is changed to be a file.
                panic!("Changing folders to files is not supported!");
            }
            metadata_db::ItemType::DELETION { .. } => {
                // We have no local entry for the target file in our DB, register it as a new file.
                result.new_items += 1;

                let hash = self.fs_access.calculate_hash(&path)?;
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
            if !lower_case_entries.contains(&child_item.path.name().to_lowercase()) {
                let child_item_path = child_item.path.clone();
                scan_result.deleted_items +=
                    self.db_access.delete_local_data_item(&child_item_path)?;
            }
        }

        Ok(scan_result)
    }
}

#[cfg(test)]
mod tests;
