mod db_migration;

// Database schema - must be kept up to date manually
mod entity;
pub use self::entity::*;
mod item;
pub use self::item::*;
mod schema;

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sqlite::SqliteConnection;
use fs_interaction::relative_path::RelativePath;
use metadata_db::schema::metadatas::columns::owner_information_id;
use std::error::Error;
use std::fmt;
use version_vector::VersionVector;

#[derive(Debug)]
pub enum MetadataDBError {
    DBMigrationError {
        source: db_migration::MigrationError,
    },
    DBConnectionError {
        source: diesel::result::ConnectionError,
    },
    GenericSQLError {
        source: diesel::result::Error,
    },
    NotFound,
    ViolatesDBConsistency {
        message: &'static str,
    },
}
pub type Result<T> = std::result::Result<T, MetadataDBError>;

pub struct MetadataDB {
    conn: SqliteConnection,
}

impl MetadataDB {
    /// Opens the metadata db file located at the given path and performs data migrations to
    /// the current application version if required.
    pub fn open(path: &str) -> Result<MetadataDB> {
        let result = MetadataDB {
            conn: SqliteConnection::establish(path)?,
        };

        result.default_db_settings()?;
        result.upgrade_db()?;

        Ok(result)
    }

    /// Creates and returns the data set stored in the open MetadataDB.
    /// Currently, exactly one data set can be stored in one database.
    pub fn create_data_set(&self, unique_name_p: &str) -> Result<DataSet> {
        use self::schema::data_sets::dsl::*;

        Ok(self.conn.transaction(|| {
            if data_sets.first::<DataSet>(&self.conn).optional()?.is_some() {
                return Err(MetadataDBError::ViolatesDBConsistency {
                    message: "Must only have ONE data_set per database!",
                });
            }

            diesel::insert_into(data_sets)
                .values(data_set::FromUniqueName {
                    unique_name: unique_name_p,
                })
                .execute(&self.conn)?;

            let data_set = data_sets.first::<DataSet>(&self.conn)?;
            Ok(data_set)
        })?)
    }

    /// Returns the data set stored in the open MetadataDB.
    pub fn get_data_set(&self) -> Result<DataSet> {
        use self::schema::data_sets::dsl::*;

        Ok(data_sets.first::<DataSet>(&self.conn)?)
    }

    /// Updates the human readable name of the data set stored in this MetadataDB.
    pub fn update_data_set_name(&self, human_name_p: &str) -> Result<()> {
        use self::schema::data_sets::dsl::*;

        diesel::update(data_sets)
            .set(human_name.eq(human_name_p))
            .execute(&self.conn)?;

        Ok(())
    }

    /// List all data stores managed by the open MetadataDB.
    /// At most one of them must be the local data set (marked with 'is_this_data_store == true').
    pub fn get_data_stores(&self) -> Result<Vec<DataStore>> {
        use self::schema::data_stores::dsl::*;
        // We currently only allow EXACTLY ONE data_set, thus we do not need to join here.
        let result = data_stores.load(&self.conn)?;
        Ok(result)
    }

    /// Creates a new data store in the open MetadataDB.
    /// At most one data store must be the local one and this methods reports an consistency
    /// error if violated.
    pub fn create_data_store(&self, new_store: &data_store::InsertFull) -> Result<DataStore> {
        use self::schema::data_items;
        use self::schema::data_stores;
        use self::schema::owner_informations;
        use diesel::dsl::*;

        let result = self.conn.transaction(|| {
            // Check DB consistency (for only ONE local data store)
            if new_store.is_this_store {
                let this_store_already_exists = select(exists(
                    data_stores::table.filter(data_stores::is_this_store.eq(true)),
                ))
                .get_result(&self.conn)?;
                if this_store_already_exists {
                    return Err(MetadataDBError::ViolatesDBConsistency {
                        message: "Must only have one data_store marked as local store!",
                    });
                }
            }

            // Insert new entry
            diesel::insert_into(data_stores::table)
                .values(new_store)
                .execute(&self.conn)?;

            let result = data_stores::table
                .filter(data_stores::unique_name.eq(&new_store.unique_name))
                .first::<DataStore>(&self.conn)?;

            // Ensure that we always have a root-directory in the local data store
            // (This simplifies A LOT of functions, as we spare the special case for no parent dir).
            if new_store.is_this_store {
                diesel::insert_into(data_items::table)
                    .values(data_item::InsertFull {
                        path_component: "",
                        parent_item_id: None,
                    })
                    .execute(&self.conn)?;
                let root_data_item = data_items::table
                    .filter(data_items::parent_item_id.is_null())
                    .first::<DataItem>(&self.conn)?;
                diesel::insert_into(owner_informations::table)
                    .values(owner_information::InsertFull {
                        data_store_id: result.id,
                        data_item_id: root_data_item.id,

                        is_file: false,
                        is_deleted: false,
                    })
                    .execute(&self.conn)?;
                // It's fine that we DO NOT assign any mod or sync time.
                // It implicitly defaults to all 0's, which is actually correct before any scan.
                self.increase_local_time()?;
            }

            Ok(result)
        })?;

        Ok(result)
    }

    /// Returns the local data store of the open MetadataDB.
    pub fn get_this_data_store(&self) -> Result<DataStore> {
        use self::schema::data_stores::dsl::*;

        Ok(data_stores
            .filter(is_this_store.eq(true))
            .first::<DataStore>(&self.conn)?)
    }

    // Queries a local data item from the DB
    // (same as get_data_item but always from the local data store).
    pub fn get_local_data_item(&self, path: &RelativePath) -> Result<Item> {
        let local_data_store = self.get_this_data_store()?;
        self.get_data_item(&local_data_store, &path)
    }

    /// Queries a data item from the DB and returns it.
    /// Data items must always exist, as there is at least a deletion notice for everything.
    pub fn get_data_item(&self, for_data_store: &DataStore, path: &RelativePath) -> Result<Item> {
        // Any operation involving consistency of sync-time stamps and/or parent-child relations
        // between items in the database requires a consistent view of the invariants held.
        self.conn.transaction::<_, MetadataDBError, _>(|| {
            let mut path_items = self.load_data_items_on_path(&for_data_store, &path)?;

            // Sync times can increase down the chain of data_items.
            // Mod times (for now) are stored completely in the data_item.
            let local_data_store = self.get_this_data_store()?;
            let local_time = local_data_store.time;

            let mut final_sync_time = VersionVector::<i64>::new();
            final_sync_time[&local_data_store.id] = local_time;

            for path_item in path_items.iter_mut() {
                self.load_sync_time_for_item(path_item)?;
                final_sync_time.max(&path_item.sync_time.as_ref().unwrap());
            }

            if path_items.len() == path.get_path_components().len() {
                // The item has an actual entry in the db, inspect it further.
                let mut target_item = path_items.pop().unwrap();

                self.load_mod_time_for_item(&mut target_item)?;
                Ok(self.internal_to_external_item(&target_item, &final_sync_time)?)
            } else {
                // The item has no more entry in the db, thus we 'create' a deletion notice.
                Ok(Item {
                    path_component: path.name().to_lowercase(),
                    content: ItemType::DELETION {
                        sync_time: final_sync_time,
                    },
                })
            }
        })
    }

    fn load_data_items_on_path(
        &self,
        for_data_store: &DataStore,
        path: &RelativePath,
    ) -> Result<Vec<ItemInternal>> {
        use self::schema::data_items;
        use self::schema::metadatas;
        use self::schema::owner_informations;

        // We handle all path's in lower case in here!
        let path = path.to_lower_case();

        // Note: Maybe re-work with 'WITH RECURSIVE' queries directly in sqlite.
        //       Wait for actual performance issues before trying to do this.
        let mut result = Vec::<ItemInternal>::with_capacity(path.get_path_components().len());

        for path_component in path.get_path_components() {
            // TODO: Cut down on this duplication.
            let parent_data_item_id = result.last().map(|item| item.data_item.id);
            if let Some(parent_data_item_id) = parent_data_item_id {
                let component_db_item = data_items::table
                    .filter(data_items::path_component.eq(path_component))
                    .filter(data_items::parent_item_id.eq(Some(parent_data_item_id)))
                    .inner_join(owner_informations::table.left_join(metadatas::table))
                    .filter(owner_informations::data_store_id.eq(for_data_store.id))
                    .first::<(DataItem, (OwnerInformation, Option<Metadata>))>(&self.conn)
                    .optional()?;

                if let Some((item, (owner_information, metadata))) = component_db_item {
                    let current_item =
                        ItemInternal::from_join_tuple(item, owner_information, metadata);
                    result.push(current_item);
                } else {
                    break;
                }
            } else {
                let component_db_item = data_items::table
                    .filter(data_items::path_component.eq(path_component))
                    .filter(data_items::parent_item_id.is_null()) // ...can not compare to NULL
                    .inner_join(owner_informations::table.left_join(metadatas::table))
                    .filter(owner_informations::data_store_id.eq(for_data_store.id))
                    .first::<(DataItem, (OwnerInformation, Option<Metadata>))>(&self.conn)
                    .optional()?;

                if let Some((item, (owner_information, metadata))) = component_db_item {
                    let current_item =
                        ItemInternal::from_join_tuple(item, owner_information, metadata);
                    result.push(current_item);
                } else {
                    break;
                }
            };
        }

        Ok(result)
    }
    fn load_sync_time_for_item(&self, data_item: &mut ItemInternal) -> Result<()> {
        use self::schema::sync_times;

        let sync_time_entries: Vec<SyncTime> = sync_times::table
            .filter(sync_times::owner_information_id.eq(data_item.owner_info.id))
            .load::<SyncTime>(&self.conn)?;

        let mut result_vector = VersionVector::<i64>::new();
        for sync_time in sync_time_entries {
            result_vector[&sync_time.data_store_id] = sync_time.time;
        }

        data_item.sync_time = Some(result_vector);

        Ok(())
    }
    fn load_mod_time_for_item(&self, data_item: &mut ItemInternal) -> Result<()> {
        use self::schema::mod_times;

        let mod_time_entries: Vec<ModTime> = mod_times::table
            .filter(mod_times::owner_information_id.eq(data_item.owner_info.id))
            .load::<ModTime>(&self.conn)?;

        let mut result_vector = VersionVector::<i64>::new();
        for mod_time in mod_time_entries {
            result_vector[&mod_time.data_store_id] = mod_time.time;
        }

        data_item.mod_time = Some(result_vector);

        Ok(())
    }
    fn internal_to_external_item(
        &self,
        item: &ItemInternal,
        parent_directory_sync_time: &VersionVector<i64>,
    ) -> Result<Item> {
        let mut item_sync_time = parent_directory_sync_time.clone();
        item_sync_time.max(&item.sync_time.as_ref().unwrap());

        if item.owner_info.is_deleted {
            Ok(Item {
                path_component: item.data_item.path_component.to_owned(),
                content: ItemType::DELETION {
                    sync_time: item_sync_time,
                },
            })
        } else if item.owner_info.is_file {
            Ok(Item {
                path_component: item.data_item.path_component.to_owned(),
                content: ItemType::FILE {
                    metadata: item.metadata.clone(),
                    mod_time: item.mod_time.as_ref().unwrap().clone(),
                    sync_time: item_sync_time,
                },
            })
        } else {
            Ok(Item {
                path_component: item.data_item.path_component.to_owned(),
                content: ItemType::FOLDER {
                    metadata: item.metadata.clone(),
                    mod_time: item.mod_time.as_ref().unwrap().clone(),
                    sync_time: item_sync_time,
                },
            })
        }
    }

    /// Queries all child items of a given DB item.
    pub fn get_child_data_items(
        &self,
        for_data_store: &DataStore,
        dir_path: &RelativePath,
    ) -> Result<Vec<Item>> {
        use self::schema::data_items;
        use self::schema::metadatas;
        use self::schema::owner_informations;

        // Any operation involving consistency of sync-time stamps and/or parent-child relations
        // between items in the database requires a consistent view of the invariants held.
        self.conn.transaction::<_, MetadataDBError, _>(|| {
            let mut dir_path_items = self.load_data_items_on_path(&for_data_store, &dir_path)?;

            if dir_path_items.len() == dir_path.get_path_components().len() {
                // The parent directory exists, go and inspect it further.

                // Sync times can increase down the chain of data_items.
                // Mod times (for now) are stored completely in the data_items.
                let local_data_store = self.get_this_data_store()?;
                let local_time = local_data_store.time;

                let mut dir_sync_time = VersionVector::<i64>::new();
                dir_sync_time[&local_data_store.id] = local_time;

                for path_item in dir_path_items.iter_mut() {
                    self.load_sync_time_for_item(path_item)?;
                    dir_sync_time.max(&path_item.sync_time.as_ref().unwrap());
                }

                let dir_item = dir_path_items.last().unwrap();

                let dir_entries = data_items::table
                    .filter(data_items::parent_item_id.eq(dir_item.data_item.id))
                    .inner_join(owner_informations::table.left_join(metadatas::table))
                    .filter(owner_informations::data_store_id.eq(for_data_store.id))
                    .load::<(DataItem, (OwnerInformation, Option<Metadata>))>(&self.conn)?;

                dir_entries
                    .into_iter()
                    .map(|(item, (owner_information, metadata))| {
                        let mut internal_item =
                            ItemInternal::from_join_tuple(item, owner_information, metadata);
                        self.load_sync_time_for_item(&mut internal_item)?;
                        self.load_mod_time_for_item(&mut internal_item)?;
                        Ok(self.internal_to_external_item(&internal_item, &dir_sync_time)?)
                    })
                    .collect()
            } else {
                // The parent path is not in the DB, thus we have no child items.
                Ok(vec![])
            }
        })
    }

    /// Modifies a data item for the local data store (making sure versions stay consistent).
    /// The method implicitly assigns the appropriate creation information and time stamps.
    /// The method implicitly assigns an appropriate update time to the item.
    pub fn update_local_data_item(
        &self,
        path: &RelativePath,
        creation_time: chrono::NaiveDateTime,
        mod_time: chrono::NaiveDateTime,
        is_file: bool,
        hash: &str,
    ) -> Result<()> {
        use self::schema::metadatas;
        use self::schema::owner_informations;

        // Any operation involving consistency of sync-time stamps and/or parent-child relations
        // between items in the database requires a consistent view of the invariants held.
        self.conn.transaction::<_, MetadataDBError, _>(|| {
            // We insert an item, bump the data stores version and mark all events with the version.
            let local_data_store = self.get_this_data_store()?;
            let new_time = local_data_store.time;
            self.increase_local_time()?;

            // Look for parent item.
            let parent_dir_path = path.parent();
            let dir_path_items =
                self.load_data_items_on_path(&local_data_store, &parent_dir_path)?;

            if dir_path_items.len() != parent_dir_path.get_path_components().len() {
                // Something went wrong, we can not update an item that has no parent.
                Err(MetadataDBError::ViolatesDBConsistency {
                    message: "Must not insert data_item without existing parent item (i.e. no file without a parent folder)!"
                })
            } else {
                let parent_dir_item = dir_path_items.last().unwrap();
                let lower_case_name = path.name().to_lowercase();

                // FIXME: Do not allow non-deleted child items in folders that are deleted
                if !parent_dir_item.owner_info.is_file {
                    let new_data_item = self.ensure_data_item_exists(&lower_case_name, &parent_dir_item.data_item)?;

                    // Associate owner information with it (...or update an existing one, e.g.
                    // for a previously deleted item that still requires a deletion notice in the DB).
                    let existing_owner_information = owner_informations::table
                        .filter(owner_informations::data_item_id.eq(new_data_item.id))
                        .filter(owner_informations::data_store_id.eq(local_data_store.id))
                        .first::<OwnerInformation>(&self.conn)
                        .optional()?;
                    let new_owner_info = if let Some(owner_info) = existing_owner_information {
                        if !owner_info.is_deleted && owner_info.is_file != is_file {
                            return Err(MetadataDBError::ViolatesDBConsistency {
                                message: "Must not change types of entries in the DB. Delete and re-create them instead!",
                            })
                        }
                        if owner_info.is_deleted {
                            // Register the change in deletion_status
                            diesel::update(owner_informations::table)
                                .filter(owner_informations::id.eq(owner_info.id))
                                .set(owner_informations::is_deleted.eq(false))
                                .execute(&self.conn)?;
                        }

                        owner_info
                    } else {
                        diesel::insert_into(owner_informations::table)
                            .values(owner_information::InsertFull {
                                data_item_id: new_data_item.id,
                                data_store_id: local_data_store.id,

                                is_file: is_file,
                                is_deleted: false,
                            })
                            .execute(&self.conn)?;

                        owner_informations::table
                            .filter(owner_informations::data_item_id.eq(new_data_item.id))
                            .filter(owner_informations::data_store_id.eq(local_data_store.id))
                            .first::<OwnerInformation>(&self.conn)?
                    };

                    // Also update the new item's modification time to match its creation time.
                    // This gives the item a 'proper' modification event to be used in later comparisons.
                    self.add_mod_event(&new_owner_info, &local_data_store, new_time)?;


                    // Associate Metadata with the given entry (...or update an existing one, e.g.
                    // for a previously deleted item that still requires a deletion notice in the DB).
                    let existing_metadata = metadatas::table
                        .filter(metadatas::owner_information_id.eq(new_owner_info.id))
                        .first::<Metadata>(&self.conn)
                        .optional()?;
                    if let Some(metadata) = existing_metadata{
                        // Update existing entry
                        diesel::update(metadatas::table)
                            .filter(metadatas::id.eq(metadata.id))
                            .set(metadata::UpdateMetadata{
                                case_sensitive_name: path.name(),
                                creation_time: &creation_time,
                                mod_time: &mod_time,
                                hash: &hash,
                            }).execute(&self.conn)?;
                    } else {
                        // Create new entry
                        // Insert metadata item (or update existing ones)
                        diesel::insert_into(metadatas::table)
                            .values(metadata::InsertFull {
                                owner_information_id: new_owner_info.id,

                                creator_store_id: local_data_store.id,
                                creator_store_time: new_time,

                                case_sensitive_name: path.name(),
                                creation_time: creation_time,
                                mod_time: mod_time,
                                hash: hash,
                            })
                            .execute(&self.conn)?;
                    };

                    Ok(())
                } else {
                    // Something went wrong, files can not hold child-files (only folders can).
                    Err(MetadataDBError::ViolatesDBConsistency {
                        message: "Must not insert data_item that has a file as a parent!"
                    })
                }
            }

        })
    }

    /// Syncs a local data item, i.e. updating its metadata, sync- and mod time.
    /// The method implicitly keeps invariant in the DB, e.g. sets sync time to be
    /// max(argument, current) and to update parent entries.
    pub fn sync_local_data_item(&self, path: &RelativePath, target_item: &Item) -> Result<()> {
        use self::schema::metadatas;
        use self::schema::mod_times;
        use self::schema::owner_informations;

        // Any operation involving consistency of sync-time stamps and/or parent-child relations
        // between items in the database requires a consistent view of the invariants held.
        self.conn.transaction::<_, MetadataDBError, _>(|| {
            let local_data_store = self.get_this_data_store()?;

            // Look for parent item.
            let mut items_path_to_target=
                self.load_data_items_on_path(&local_data_store, &path)?;

            if items_path_to_target.len() < path.get_path_components().len() - 1 {
                // Something went wrong, we can not update an item that has no parent.
                // We enforce this invariant even on syncs (at least announce the parent folder).
                Err(MetadataDBError::ViolatesDBConsistency {
                    message: "Must not insert data_item without existing parent item (i.e. no file without a parent folder)!"
                })
            } else {
                let mut current_item_sync_time = VersionVector::new();
                for mut parent_item in items_path_to_target.iter_mut() {
                    self.load_sync_time_for_item(&mut parent_item)?;
                    current_item_sync_time.max(&parent_item.sync_time.as_ref().unwrap());
                }

                let parent_dir_item = &items_path_to_target[items_path_to_target.len() - 2];
                let lower_case_name = path.name().to_lowercase();

                // FIXME: Do not allow non-deleted child items in folders that are deleted
                if !parent_dir_item.owner_info.is_file {
                    let new_data_item = self.ensure_data_item_exists(&lower_case_name, &parent_dir_item.data_item)?;

                    // Associate owner information with it (...or update an existing one, e.g.
                    // for a previously deleted item that still requires a deletion notice in the DB).
                    let existing_owner_information = owner_informations::table
                        .filter(owner_informations::data_item_id.eq(new_data_item.id))
                        .filter(owner_informations::data_store_id.eq(local_data_store.id))
                        .first::<OwnerInformation>(&self.conn)
                        .optional()?;
                    let new_owner_info = if let Some(owner_info) = existing_owner_information {
                        let existing_item = items_path_to_target.last().unwrap();

                        let item_will_be_deleted = !owner_info.is_deleted && target_item.is_deletion();
                        let item_no_longer_folder = !owner_info.is_file && !target_item.is_folder();
                        if  item_will_be_deleted || item_no_longer_folder {
                            // In case a previous folder now is none-anymore, we need to clean out
                            // all its children from the DB (completely remove them).
                            self.delete_children_recursive(&existing_item, true)?;
                        }

                        // Remove un-needed entries for deleted items.
                        if target_item.is_deletion() {
                            // No need for modification times of deleted items.
                            diesel::delete(mod_times::table)
                                .filter(mod_times::owner_information_id.eq(existing_item.owner_info.id))
                                .execute(&self.conn)?;
                            // No need for metadata of deleted items.
                            diesel::delete(metadatas::table)
                                .filter(metadatas::owner_information_id.eq(existing_item.owner_info.id))
                                .execute(&self.conn)?;
                        }

                        // Everything is ready to simply be 'synced up' with the target item.
                        // This will also e.g. correctly setup the deletion status/folder status.
                        diesel::update(owner_informations::table)
                            .filter(owner_informations::id.eq(owner_info.id))
                            .set((
                                owner_informations::is_file.eq(target_item.is_file()),
                                owner_informations::is_deleted.eq(target_item.is_deletion())
                            )).execute(&self.conn)?;

                        owner_info
                    } else {
                        // Nothing existed locally, just create the item as desired.
                        diesel::insert_into(owner_informations::table)
                            .values(owner_information::InsertFull {
                                data_item_id: new_data_item.id,
                                data_store_id: local_data_store.id,

                                is_file: target_item.is_file(),
                                is_deleted: target_item.is_deletion(),
                            })
                            .execute(&self.conn)?;

                        owner_informations::table
                            .filter(owner_informations::data_item_id.eq(new_data_item.id))
                            .filter(owner_informations::data_store_id.eq(local_data_store.id))
                            .first::<OwnerInformation>(&self.conn)?
                    };

                    // Simply set the mod_time entries.
                    if !target_item.is_deletion() {
                        self.update_mod_times(&new_owner_info, &target_item.mod_time(), true)?;
                        self.update_parent_mod_times(&new_owner_info, true)?;
                    }
                    // For sync times we ALWAYS see what we already have in the DB, i.e. max it with
                    // the given sync time to not loose information.
                    let mut target_sync_time = current_item_sync_time;
                    target_sync_time.max(&target_item.sync_time());
                    self.update_sync_times(&new_owner_info, &target_sync_time, false)?;


                    // Associate the metadata to the target if it's no deletion.
                    if !target_item.is_deletion() {
                        if let Some(metadata) = &target_item.metadata() {
                            diesel::replace_into(metadatas::table)
                                .values(metadata::InsertFull {
                                    owner_information_id: new_owner_info.id,

                                    creator_store_id: metadata.creator_store_id,
                                    creator_store_time: metadata.creator_store_time,

                                    case_sensitive_name: path.name(),
                                    creation_time: metadata.creation_time,
                                    mod_time: metadata.mod_time,
                                    hash: &metadata.hash,
                                })
                                .execute(&self.conn)?;
                        } else {
                            diesel::delete(metadatas::table.filter(metadatas::owner_information_id.eq(new_owner_info.id))).execute(&self.conn)?;
                        }

                    }

                    Ok(())
                } else {
                    // Something went wrong, files can not hold child-files (only folders can).
                    // Again, we also enforce this during syncs. The sync algorithm itself should
                    // make sure this does not happen.
                    Err(MetadataDBError::ViolatesDBConsistency {
                        message: "Must not insert data_item that has a file as a parent!"
                    })
                }
            }

        })
    }

    fn ensure_data_item_exists(
        &self,
        lower_case_name: &str,
        parent: &DataItem,
    ) -> Result<DataItem> {
        use self::schema::data_items;

        // Insert new data_item (...or keep existing one).
        let existing_data_item = data_items::table
            .filter(data_items::path_component.eq(lower_case_name))
            .filter(data_items::parent_item_id.eq(parent.id))
            .first::<DataItem>(&self.conn)
            .optional()?;
        let result_data_item = if let Some(data_item) = existing_data_item {
            data_item
        } else {
            diesel::insert_into(data_items::table)
                .values(data_item::InsertFull {
                    parent_item_id: Some(parent.id),
                    path_component: &lower_case_name,
                })
                .execute(&self.conn)?;

            data_items::table
                .filter(data_items::path_component.eq(lower_case_name))
                .filter(data_items::parent_item_id.eq(parent.id))
                .first::<DataItem>(&self.conn)?
        };

        Ok(result_data_item)
    }

    pub fn delete_local_data_item(&self, path: &RelativePath) -> Result<usize> {
        // Any operation involving consistency of sync-time stamps and/or parent-child relations
        // between items in the database requires a consistent view of the invariants held.
        self.conn.transaction::<_, MetadataDBError, _>(|| {
            // We insert an item, bump the data stores version and mark all events with the version.
            let local_data_store = self.get_this_data_store()?;

            // Look for the item.
            let path_items = self.load_data_items_on_path(&local_data_store, &path)?;

            if path_items.len() != path.get_path_components().len() {
                // We have no parent item, i.e. this is already deleted.
                Ok(0)
            } else {
                let deleted = self.delete_local_data_item_recursive(&path_items.last().unwrap())?;
                Ok(deleted)
            }
        })
    }
    fn delete_local_data_item_recursive(&self, item: &ItemInternal) -> Result<usize> {
        use self::schema::data_items;
        use self::schema::metadatas;
        use self::schema::mod_times;
        use self::schema::owner_informations;

        let mut deleted = 0;

        // Make sure to delete children of folders recursively
        if !item.owner_info.is_file {
            let dir_entries = data_items::table
                .filter(data_items::parent_item_id.eq(item.data_item.id))
                .inner_join(owner_informations::table.left_join(metadatas::table))
                .filter(owner_informations::data_store_id.eq(item.owner_info.data_store_id))
                .load::<(DataItem, (OwnerInformation, Option<Metadata>))>(&self.conn)?;

            for (item, (owner_information, metadata)) in dir_entries {
                let dir_entry = ItemInternal::from_join_tuple(item, owner_information, metadata);
                deleted += self.delete_local_data_item_recursive(&dir_entry)?;
            }
        }

        // Update Owner Info to be deleted
        if !item.owner_info.is_deleted {
            // Register the change in deletion_status
            diesel::update(owner_informations::table)
                .filter(owner_informations::id.eq(item.owner_info.id))
                .set(owner_informations::is_deleted.eq(true))
                .execute(&self.conn)?;

            let local_data_store = self.get_this_data_store()?;
            let new_time = local_data_store.time;
            self.increase_local_time()?;
            self.add_mod_event(&item.owner_info, &local_data_store, new_time)?;

            deleted += 1;
        }

        // TODO: Pull this into clean-up procedure (as we e.g. might miss some of these clean ups
        //       during a sync anyways and they should never cause issues besides wasted storage).
        // Remove un-needed entries
        // No need for modification times of deleted items.
        diesel::delete(mod_times::table)
            .filter(mod_times::owner_information_id.eq(item.owner_info.id))
            .execute(&self.conn)?;
        // No need for metadata of deleted items.
        diesel::delete(metadatas::table)
            .filter(metadatas::owner_information_id.eq(item.owner_info.id))
            .execute(&self.conn)?;

        Ok(deleted)
    }

    fn delete_children_recursive(&self, item: &ItemInternal, is_top_item: bool) -> Result<()> {
        use self::schema::data_items;
        use self::schema::metadatas;
        use self::schema::owner_informations;

        // Make sure to delete children of folders recursively
        if !item.owner_info.is_file {
            let dir_entries = data_items::table
                .filter(data_items::parent_item_id.eq(item.data_item.id))
                .inner_join(owner_informations::table.left_join(metadatas::table))
                .filter(owner_informations::data_store_id.eq(item.owner_info.data_store_id))
                .load::<(DataItem, (OwnerInformation, Option<Metadata>))>(&self.conn)?;

            for (item, (owner_information, metadata)) in dir_entries {
                let dir_entry = ItemInternal::from_join_tuple(item, owner_information, metadata);
                self.delete_children_recursive(&dir_entry, false)?;
            }
        }

        if !is_top_item {
            // For child items we remove everything from the DB (as it would be cleaned up anyways).
            diesel::delete(owner_informations::table.find(item.owner_info.id))
                .execute(&self.conn)?;
            // Mod times, sync times and metadata should be deleted by cascade rules.
        }

        Ok(())
    }

    /// Converts a version vector indexed by data_store unique names to an local representation,
    /// indexed by database ID's. Operation can be reversed using id_to_named_version_vector(...).
    pub fn named_to_id_version_vector(
        &self,
        named_vector: &VersionVector<String>,
    ) -> Result<VersionVector<i64>> {
        use self::schema::data_stores;

        let mut result = VersionVector::new();
        for (data_store_name, time) in named_vector.iter() {
            // TODO: Special Error Type in case we do not know the other repo!
            let data_store_id = data_stores::table
                .select(data_stores::id)
                .filter(data_stores::unique_name.eq(data_store_name))
                .first::<i64>(&self.conn)?;
            result[&data_store_id] = *time;
        }

        Ok(result)
    }

    /// Converts a id vector indexed by local data_store DB Id's to an universial representation,
    /// indexed by data_set names. Operation can be reversed using named_to_id_version_vector(...).
    pub fn id_to_named_version_vector(
        &self,
        id_vector: &VersionVector<i64>,
    ) -> Result<VersionVector<String>> {
        use self::schema::data_stores;

        let mut result = VersionVector::new();
        for (data_store_id, time) in id_vector.iter() {
            let data_store_id = data_stores::table
                .select(data_stores::unique_name)
                .find(data_store_id)
                .first::<String>(&self.conn)?;
            result[&data_store_id] = *time;
        }

        Ok(result)
    }

    /// Updates the modification time of the given item (via its owner information) to
    /// include the given modification done by a the given data store at the given time stamp
    /// (i.e. it sets the item's modification time to MAX{current_mod_vector, given_mod_event}).
    ///
    /// Makes sure that all parent items are updated appropriately (i.e. all parent items
    /// stick to the DB invariant mod_time = MAX{child mod times}).
    fn add_mod_event(
        &self,
        owner_information: &OwnerInformation,
        modifying_data_store: &DataStore,
        modification_time: i64,
    ) -> Result<()> {
        let mut mod_vector = VersionVector::<i64>::new();
        mod_vector[&modifying_data_store.id] = modification_time;

        self.conn.transaction(|| {
            self.update_mod_times(&owner_information, &mod_vector, false)?;
            self.update_parent_mod_times(&owner_information, false)
        })?;

        Ok(())
    }

    /// Updates the modification times of an DB entry by replacing all
    /// given vector time entries (represented by their data_store id).
    fn update_mod_times(
        &self,
        owner_information: &OwnerInformation,
        new_mod_times: &VersionVector<i64>,
        delete_existing: bool,
    ) -> Result<()> {
        use self::schema::mod_times;

        if delete_existing {
            diesel::delete(
                mod_times::table.filter(mod_times::owner_information_id.eq(owner_information.id)),
            )
            .execute(&self.conn)?;
        }

        let new_db_entries: Vec<_> = new_mod_times
            .iter()
            .map(|(data_store_id, time)| mod_time::InsertFull {
                owner_information_id: owner_information.id,
                data_store_id: data_store_id.clone(),
                time: time.clone(),
            })
            .collect();

        diesel::replace_into(mod_times::table)
            .values(new_db_entries)
            .execute(&self.conn)?;

        Ok(())
    }

    /// Updates the sync times of an DB entry by replacing all
    /// given vector time entries (represented by their data_store id).
    fn update_sync_times(
        &self,
        owner_information: &OwnerInformation,
        new_sync_times: &VersionVector<i64>,
        delete_existing: bool,
    ) -> Result<()> {
        use self::schema::sync_times;

        if delete_existing {
            diesel::delete(
                sync_times::table.filter(sync_times::owner_information_id.eq(owner_information.id)),
            )
            .execute(&self.conn)?;
        }

        let new_db_entries: Vec<_> = new_sync_times
            .iter()
            .map(|(data_store_id, time)| sync_time::InsertFull {
                owner_information_id: owner_information.id,
                data_store_id: data_store_id.clone(),
                time: time.clone(),
            })
            .collect();

        diesel::replace_into(sync_times::table)
            .values(new_db_entries)
            .execute(&self.conn)?;

        Ok(())
    }

    /// Queries the modification time vector for a given OwnerInformation
    /// (and thus indirectly for the associated data_item).
    ///
    /// Note: The modification time vector represents (local data_store_id -> time) pairs,
    ///       for  exchange with other data_stores it must be 'translated' to a vector version
    ///       where times are identified by ('unique-str' -> time) pairs.
    pub fn get_mod_times(
        &self,
        owner_information: &OwnerInformation,
    ) -> Result<VersionVector<i64>> {
        use self::schema::mod_times;

        let mod_times: Vec<ModTime> = mod_times::table
            .filter(mod_times::owner_information_id.eq(owner_information.id))
            .load(&self.conn)?;

        let mut result = VersionVector::new();
        for mod_time in &mod_times {
            result[&mod_time.data_store_id] = mod_time.time;
        }

        Ok(result)
    }

    /// Queries the synchronization time vector for a given OwnerInformation
    /// (and thus indirectly for the associated data_item).
    ///
    /// Note: The synchronization time vector represents (local data_store_id -> time) pairs,
    ///       for  exchange with other data_stores it must be 'translated' to a vector version
    ///       where times are identified by ('unique-str' -> time) pairs.
    pub fn get_sync_times(
        &self,
        owner_information: &OwnerInformation,
    ) -> Result<VersionVector<i64>> {
        use self::schema::sync_times;

        let sync_times: Vec<ModTime> = sync_times::table
            .filter(sync_times::owner_information_id.eq(owner_information.id))
            .load(&self.conn)?;

        let mut result = VersionVector::new();
        for sync_time in &sync_times {
            // FIXME: The sync times are not as simple. We need to iterate up the parent chain...
            result[&sync_time.data_store_id] = sync_time.time;
        }

        Ok(result)
    }

    /// Updates the modification times of all the given owner_information's parent
    /// data_items to include the given time in their MAX(children) modification time.
    fn update_parent_mod_times(
        &self,
        owner_information: &OwnerInformation,
        delete_existing: bool,
    ) -> Result<()> {
        use self::schema::data_items;
        use self::schema::owner_informations;

        let mut current_item_id = owner_information.data_item_id;
        let mut current_mod_vector = self.get_mod_times(owner_information)?;
        loop {
            let parent_item_id = data_items::table
                .find(current_item_id)
                .select(data_items::parent_item_id)
                .first::<Option<i64>>(&self.conn)?;
            if parent_item_id.is_none() {
                // Reached root level
                break;
            }
            let parent_item_id = parent_item_id.unwrap();

            let parent_owner_information = owner_informations::table
                .filter(owner_informations::data_item_id.eq(parent_item_id))
                .filter(owner_informations::data_store_id.eq(owner_information.data_store_id))
                .first::<OwnerInformation>(&self.conn)?;
            let parent_mod_vector = self.get_mod_times(&parent_owner_information)?;

            if current_mod_vector == parent_mod_vector {
                // We have nothing new to add to the maximum mod time, do not hit the DB further.
                break;
            }
            current_mod_vector.max(&parent_mod_vector);
            self.update_mod_times(
                &parent_owner_information,
                &current_mod_vector,
                delete_existing,
            )?;

            // Recurse up one directory
            current_item_id = parent_item_id;
        }

        Ok(())
    }

    /// Helper that increases the version of the local data store.
    /// Frequently used when working with data items.
    fn increase_local_time(&self) -> Result<()> {
        use self::schema::data_stores;

        diesel::update(data_stores::table)
            .filter(data_stores::is_this_store.eq(true))
            .set(data_stores::time.eq(data_stores::time + 1))
            .execute(&self.conn)?;

        Ok(())
    }

    fn upgrade_db(&self) -> db_migration::Result<()> {
        self.conn
            .transaction(|| db_migration::upgrade_db(&self.conn))?;

        Ok(())
    }

    fn default_db_settings(&self) -> Result<()> {
        sql_query("PRAGMA locking_mode = EXCLUSIVE").execute(&self.conn)?;
        sql_query("PRAGMA journal_mode = WAL").execute(&self.conn)?;
        sql_query("PRAGMA foreign_keys = 1").execute(&self.conn)?;
        sql_query("PRAGMA cache_size = -64000").execute(&self.conn)?;

        Ok(())
    }
}

// Error Boilerplate (Error display, conversion and source)
impl fmt::Display for MetadataDBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error During Metadata Interaction({:?})", self)
    }
}
impl From<db_migration::MigrationError> for MetadataDBError {
    fn from(error: db_migration::MigrationError) -> Self {
        Self::DBMigrationError { source: error }
    }
}
impl From<diesel::result::Error> for MetadataDBError {
    fn from(error: diesel::result::Error) -> Self {
        match error {
            diesel::result::Error::NotFound => Self::NotFound,
            error => Self::GenericSQLError { source: error },
        }
    }
}
impl From<diesel::result::ConnectionError> for MetadataDBError {
    fn from(error: diesel::result::ConnectionError) -> Self {
        Self::DBConnectionError { source: error }
    }
}
impl Error for MetadataDBError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DBMigrationError { ref source } => Some(source),
            Self::DBConnectionError { ref source } => Some(source),
            Self::GenericSQLError { ref source } => Some(source),
            Self::ViolatesDBConsistency { .. } => None,
            Self::NotFound => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDateTime;

    fn open_metadata_store() -> MetadataDB {
        MetadataDB::open(":memory:").unwrap()
    }

    fn insert_sample_data_set(metadata_store: &MetadataDB) -> (DataSet, DataStore) {
        let data_set = metadata_store.create_data_set("abc").unwrap();
        let data_store = insert_data_store(&metadata_store, &data_set, "abc", true);

        (data_set, data_store)
    }

    fn insert_data_store(
        metadata_store: &MetadataDB,
        data_set: &DataSet,
        unique_name: &str,
        this_store: bool,
    ) -> DataStore {
        metadata_store
            .create_data_store(&data_store::InsertFull {
                data_set_id: data_set.id,
                unique_name: &unique_name,
                human_name: &"",
                is_this_store: this_store,
                time: 0,

                creation_date: &NaiveDateTime::from_timestamp(0, 0),
                path_on_device: &"/",
                location_note: &"",
            })
            .unwrap()
    }

    fn insert_data_item(metadata_store: &MetadataDB, name: &str, is_file: bool) {
        metadata_store
            .update_local_data_item(
                &RelativePath::from_path(name),
                NaiveDateTime::from_timestamp(0, 0),
                NaiveDateTime::from_timestamp(0, 0),
                is_file,
                "",
            )
            .unwrap();
    }
    fn delete_data_item(metadata_store: &MetadataDB, name: &str) {
        metadata_store
            .delete_local_data_item(&RelativePath::from_path(name))
            .unwrap();
    }
    fn assert_mod_time(metadata_store: &MetadataDB, name: &str, key: i64, value: i64) {
        let item = metadata_store
            .get_data_item(
                &metadata_store.get_this_data_store().unwrap(),
                &RelativePath::from_path(name),
            )
            .unwrap();
        match item {
            Item {
                content: ItemType::FILE { mod_time, .. },
                ..
            } => assert_eq!(mod_time[&key], value),
            Item {
                content: ItemType::FOLDER { mod_time, .. },
                ..
            } => assert_eq!(mod_time[&key], value),
            Item {
                content: ItemType::DELETION { .. },
                ..
            } => panic!("Must not check mod times on deletions"),
        };
    }

    #[test]
    fn insert_and_query_data_set() {
        let metadata_store = open_metadata_store();

        assert!(metadata_store.get_data_set().is_err());

        metadata_store.create_data_set("abc").unwrap();
        let data_set = metadata_store.get_data_set().unwrap();
        assert_eq!(data_set.unique_name, "abc");
        assert_eq!(data_set.human_name, "");

        metadata_store.update_data_set_name("testing").unwrap();
        let data_set = metadata_store.get_data_set().unwrap();
        assert_eq!(data_set.unique_name, "abc");
        assert_eq!(data_set.human_name, "testing");
    }

    #[test]
    fn enforces_single_data_set() {
        let metadata_store = open_metadata_store();

        metadata_store.create_data_set("abc").unwrap();
        match metadata_store.create_data_set("xyz") {
            Err(MetadataDBError::ViolatesDBConsistency { .. }) => (),
            _ => panic!("Must not have more than one data_set in DB!"),
        }
    }

    #[test]
    fn correctly_enter_data_items() {
        let metadata_store = open_metadata_store();
        let (_data_set, data_store) = insert_sample_data_set(&metadata_store);

        // Individual inserts have correct mod times
        assert_mod_time(&metadata_store, "", data_store.id, 0);

        insert_data_item(&metadata_store, "sub", false);
        assert_mod_time(&metadata_store, "sub", data_store.id, 1);

        insert_data_item(&metadata_store, "sub/folder", false);
        assert_mod_time(&metadata_store, "sub/folder", data_store.id, 2);

        insert_data_item(&metadata_store, "sub/folder/file", false);
        assert_mod_time(&metadata_store, "sub/folder/file", data_store.id, 3);

        // Parent folders get updated correctly
        assert_mod_time(&metadata_store, "", data_store.id, 3);
        assert_mod_time(&metadata_store, "sub", data_store.id, 3);
        assert_mod_time(&metadata_store, "sub/folder", data_store.id, 3);

        // The database is invariant on capitalization when searching or inserting items
        assert_mod_time(&metadata_store, "", data_store.id, 3);
        assert_mod_time(&metadata_store, "sUb", data_store.id, 3);
        assert_mod_time(&metadata_store, "sub/FolDer", data_store.id, 3);

        insert_data_item(&metadata_store, "sUb", false);
        assert_mod_time(&metadata_store, "sub", data_store.id, 4);

        // Check if child queries work
        let children = metadata_store
            .get_child_data_items(&data_store, &RelativePath::from_path(""))
            .unwrap();
        assert_eq!(children.len(), 1);
        match &children[0] {
            Item {
                path_component: name,
                content: ItemType::FOLDER { .. },
            } => assert_eq!(name, "sub"),
            _ => panic!("Must return the correct child item!"),
        }

        // Delete items (partially, we did not 'clean up' deletion notices jet).
        delete_data_item(&metadata_store, "sub/folder/file");
        delete_data_item(&metadata_store, "sub/folder");
        delete_data_item(&metadata_store, "sub");
        let children = metadata_store
            .get_child_data_items(&data_store, &RelativePath::from_path(""))
            .unwrap();
        assert_eq!(children.len(), 1);
        match &children[0] {
            Item {
                path_component: name,
                content: ItemType::DELETION { .. },
            } => assert_eq!(name, "sub"),
            _ => panic!("Must return the correct child item!"),
        }

        // Create new files 'over' an previous deletion notice.
        insert_data_item(&metadata_store, "SUB", false);
        assert_mod_time(&metadata_store, "sub", data_store.id, 8);

        // TODO: Clean up deletion notices and re-query child items!
    }

    #[test]
    fn correctly_inserts_synced_data_items() {
        // We use our usual local, sample data set and store and create an additional remote one.
        let metadata_store = open_metadata_store();
        let (data_set, local_data_store) = insert_sample_data_set(&metadata_store);
        let remote_data_store = insert_data_store(&metadata_store, &data_set, "remote", false);

        // Insert some sample items (/sub/folder/file)
        insert_data_item(&metadata_store, "sub", false);
        insert_data_item(&metadata_store, "sub/folder", false);
        insert_data_item(&metadata_store, "sub/folder/file", true);

        // Let's query an item, change it and re-synchronize it into our local db
        let mut file_item = metadata_store
            .get_local_data_item(&RelativePath::from_path("sub/folder/file"))
            .unwrap();

        // ...this should be as if the second store overwrites the local one with a new version.
        let new_mod_time = VersionVector::from_initial_values(vec![(&remote_data_store.id, 42)]);
        let new_sync_time = VersionVector::from_initial_values(vec![(&remote_data_store.id, 1024)]);
        file_item.content = ItemType::FILE {
            metadata: file_item.metadata().clone(),
            mod_time: new_mod_time,
            sync_time: new_sync_time,
        };

        metadata_store
            .sync_local_data_item(&RelativePath::from_path("sub/folder/file"), &file_item)
            .unwrap();

        // Check if the synced item looks right.
        let file_item_after_update = metadata_store
            .get_local_data_item(&RelativePath::from_path("sub/folder/file"))
            .unwrap();
        assert_eq!(file_item_after_update.sync_time()[&local_data_store.id], 4);
        assert_eq!(
            file_item_after_update.sync_time()[&remote_data_store.id],
            1024
        );
        assert_eq!(file_item_after_update.mod_time()[&local_data_store.id], 0);
        assert_eq!(file_item_after_update.mod_time()[&remote_data_store.id], 42);

        // Try a more complicated case where we change a folder to be a file
        let mut folder_item = metadata_store
            .get_local_data_item(&RelativePath::from_path("sub/folder"))
            .unwrap();

        let new_sync_time = VersionVector::from_initial_values(vec![(&remote_data_store.id, 2048)]);
        folder_item.content = ItemType::FILE {
            metadata: folder_item.metadata().clone(),
            mod_time: folder_item.mod_time().clone(),
            sync_time: new_sync_time,
        };

        metadata_store
            .sync_local_data_item(&RelativePath::from_path("sub/folder"), &folder_item)
            .unwrap();

        // We expect the file below to be implicitly deleted and have the appropriate sync time.
        let file_item_after_update = metadata_store
            .get_local_data_item(&RelativePath::from_path("sub/folder/file"))
            .unwrap();
        assert!(file_item_after_update.is_deletion());
        assert_eq!(
            file_item_after_update.sync_time()[&remote_data_store.id],
            2048
        );

        // Another interesting case is if we receive a single deletion notice.
        let mut root_item = metadata_store
            .get_local_data_item(&RelativePath::from_path(""))
            .unwrap();

        let new_sync_time = VersionVector::from_initial_values(vec![(&remote_data_store.id, 4096)]);
        root_item.content = ItemType::DELETION {
            sync_time: new_sync_time,
        };

        metadata_store
            .sync_local_data_item(&RelativePath::from_path("sub/folder"), &root_item)
            .unwrap();

        let root_item_after_update = metadata_store
            .get_local_data_item(&RelativePath::from_path("sub/folder/file"))
            .unwrap();
        assert!(root_item_after_update.is_deletion());
        assert_eq!(
            root_item_after_update.sync_time()[&remote_data_store.id],
            4096,
        );
        let file_item_after_update = metadata_store
            .get_local_data_item(&RelativePath::from_path("sub/folder/file"))
            .unwrap();
        assert!(file_item_after_update.is_deletion());
        assert_eq!(
            file_item_after_update.sync_time()[&remote_data_store.id],
            4096,
        );

        // TODO: Add more comprehensive tests for sync actions.
        //       We postpone this after we implement some of the actual sync logic.
    }

    #[test]
    fn convert_from_and_to_named_version_vectors() {
        let metadata_store = open_metadata_store();

        // Create sample data stores
        let data_set = metadata_store.create_data_set("abc").unwrap();
        let data_store_a = metadata_store
            .create_data_store(&data_store::InsertFull {
                data_set_id: data_set.id,
                unique_name: &"a",
                human_name: &"a",
                is_this_store: true,
                time: 0,

                creation_date: &NaiveDateTime::from_timestamp(0, 0),
                path_on_device: &"/",
                location_note: &"",
            })
            .unwrap();
        let data_store_b = metadata_store
            .create_data_store(&data_store::InsertFull {
                data_set_id: data_set.id,
                unique_name: &"b",
                human_name: &"b",
                is_this_store: false,
                time: 0,

                creation_date: &NaiveDateTime::from_timestamp(0, 0),
                path_on_device: &"/",
                location_note: &"",
            })
            .unwrap();

        let mut named_vector_1 = VersionVector::<String>::new();
        named_vector_1[&String::from("a")] = 1;
        named_vector_1[&String::from("b")] = 2;

        let id_vector_1 = metadata_store
            .named_to_id_version_vector(&named_vector_1)
            .unwrap();
        assert_eq!(id_vector_1[&data_store_a.id], 1);
        assert_eq!(id_vector_1[&data_store_b.id], 2);

        let named_vector_1_copy = metadata_store
            .id_to_named_version_vector(&id_vector_1)
            .unwrap();
        assert_eq!(named_vector_1, named_vector_1_copy);
    }
}
