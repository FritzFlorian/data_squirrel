// Database schema - must be kept up to date manually
mod schema;
use self::schema::*;
pub mod entity;
pub use self::entity::*;
// External representation of the DB.
mod db_item;
pub use self::db_item::*;

mod errors;
pub use self::errors::*;
mod db_migration;

use crate::fs_interaction::relative_path::RelativePath;
use crate::version_vector::VersionVector;

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sqlite::SqliteConnection;

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
        // We currently only allow EXACTLY ONE data_set, thus we do not need to join here.
        let result = data_stores::table.load(&self.conn)?;
        Ok(result)
    }

    /// Searches for the given data store and returns it if it exists.
    pub fn get_data_store(&self, unique_name: &str) -> Result<Option<DataStore>> {
        // We currently only allow EXACTLY ONE data_set, thus we do not need to join here.
        let result = data_stores::table
            .filter(data_stores::unique_name.eq(unique_name))
            .first::<DataStore>(&self.conn)
            .optional()?;
        Ok(result)
    }

    /// Creates a new data store in the open MetadataDB.
    /// At most one data store must be the local one and this methods reports an consistency
    /// error if violated.
    pub fn create_data_store(&self, data_store: &data_store::InsertFull) -> Result<DataStore> {
        use diesel::dsl::*;

        let result = self.conn.transaction(|| {
            // Check DB consistency (for only ONE local data store)
            if data_store.is_this_store {
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

            diesel::insert_into(data_stores::table)
                .values(data_store)
                .execute(&self.conn)?;
            let inserted_data_store = data_stores::table
                .filter(data_stores::unique_name.eq(&data_store.unique_name))
                .first::<DataStore>(&self.conn)?;

            // Ensure that we always have a root-directory in the local data store
            // (This simplifies A LOT of functions, as we spare the special case for no parent dir).
            if data_store.is_this_store {
                self.create_root_item(&inserted_data_store)?;
            }

            Ok(inserted_data_store)
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

    /// Queries a data item from the DB and returns it.
    /// Data items must always exist, as there is at least a deletion notice for everything.
    pub fn get_local_data_item(&self, path: &RelativePath) -> Result<DBItem> {
        // Any operation involving consistency of sync-time stamps and/or parent-child relations
        // between items in the database requires a consistent view of the invariants held.
        self.conn.transaction::<_, MetadataDBError, _>(|| {
            let local_data_store = self.get_this_data_store()?;
            let mut path_items = self.load_data_items_on_path(&local_data_store, &path)?;

            if path_items.len() == path.get_path_components().len() {
                // The item has an actual entry in the db, inspect it further.
                let target_item = path_items.pop().unwrap();
                Ok(DBItem::from_internal_item(target_item))
            } else {
                // The item has no more entry in the db, thus we 'create' a deletion notice.
                let last_db_entry = path_items.pop().unwrap();
                Ok(DBItem {
                    path_component: path.name().to_lowercase(),
                    sync_time: last_db_entry.sync_time.unwrap(),

                    content: ItemType::DELETION,
                })
            }
        })
    }

    /// Queries all child items of a given path that are present in the DB.
    pub fn get_local_child_data_items(&self, dir_path: &RelativePath) -> Result<Vec<DBItem>> {
        // Any operation involving consistency of sync-time stamps and/or parent-child relations
        // between items in the database requires a consistent view of the invariants held.
        self.conn.transaction::<_, MetadataDBError, _>(|| {
            let local_data_store = self.get_this_data_store()?;
            let dir_path_items = self.load_data_items_on_path(&local_data_store, &dir_path)?;

            if dir_path_items.len() == dir_path.get_path_components().len() {
                // The parent directory exists, go and inspect it further.
                // The last item in the chain of DB entries is the desired folder item.
                let dir_item = dir_path_items.last().unwrap();

                // Query its content/children.
                self.load_child_items(&dir_item)?
                    .into_iter()
                    .map(|internal_item| Ok(DBItem::from_internal_item(internal_item)))
                    .collect()
            } else {
                // The parent path is not in the DB, thus we have no child items.
                Ok(vec![])
            }
        })
    }

    /// LOCAL DATA STORE EVENT, i.e. this is used to record changes of local data_items on disk.
    ///
    /// Modifies a data item for the local data store (making sure versions stay consistent).
    /// The method implicitly assigns the appropriate creation information and time stamps.
    /// The method implicitly assigns an appropriate last modification time to the item.
    pub fn update_local_data_item(
        &self,
        path: &RelativePath,
        creation_time: chrono::NaiveDateTime,
        mod_time: chrono::NaiveDateTime,
        is_file: bool,
        hash: &str,
    ) -> Result<()> {
        self.conn.transaction::<_, MetadataDBError, _>(|| {
            // We insert an item, bump the data stores version and mark all events with the version.
            self.increase_local_time()?;
            let local_data_store = self.get_this_data_store()?;
            let new_time = local_data_store.time;

            // Load all existing items on the given path.
            let path_items =
                self.load_data_items_on_path(&local_data_store, &path)?;

            // We are especially interested in the parent directory and a potentially existing item.
            let (parent_dir_item, existing_item) =
                Self::extract_parent_dir_and_item(path_items, path.path_component_number())?;

            if parent_dir_item.item.is_file {
                return Err(MetadataDBError::ViolatesDBConsistency {
                    message: "Must not insert data_item that has a file as a parent!"
                });
            }
            if parent_dir_item.item.is_deleted {
                return Err(MetadataDBError::ViolatesDBConsistency {
                    message: "Must not try to modify a local item that has a deleted parent folder!"
                })
            }

            let item = if let Some(mut existing_item) = existing_item {
                if !existing_item.item.is_deleted && existing_item.item.is_file != is_file {
                    return Err(MetadataDBError::ViolatesDBConsistency {
                        message: "Must not change types of entries in the DB. Delete and re-create them instead!",
                    })
                }

                // ...update it to reflect the change.
                diesel::update(items::table)
                    .filter(items::id.eq(existing_item.item.id))
                    .set((
                        items::is_deleted.eq(false),
                        items::is_file.eq(is_file)
                    ))
                    .execute(&self.conn)?;
                existing_item.item.is_deleted = false;
                existing_item.item.is_file = is_file;

                existing_item.item
            } else {
                let path_component =
                    self.ensure_path_exists(&path.name(), &parent_dir_item.path_component)?;

                diesel::insert_into(items::table)
                    .values(item::InsertFull {
                        path_component_id: path_component.id,
                        data_store_id: local_data_store.id,

                        is_file: is_file,
                        is_deleted: false,
                    })
                    .execute(&self.conn)?;

                items::table
                    .filter(items::path_component_id.eq(path_component.id))
                    .filter(items::data_store_id.eq(local_data_store.id))
                    .first::<Item>(&self.conn)?
            };

            // Associate Metadata with the given entry (...or update an existing one, e.g.
            // for a previously deleted item that still requires a deletion notice in the DB).

            // FS Metadata can always be overwritten.
            diesel::replace_into(file_system_metadatas::table)
                .values(file_system_metadata::InsertFull {
                    id: item.id,

                    case_sensitive_name: path.name(),
                    creation_time: creation_time,
                    mod_time: mod_time,
                    hash: &hash,
                }).execute(&self.conn)?;

            // Mod Metadata must not be replaced if it exists!
            // We simply bump the mod time in this case.
            let existing_mod_metadata = mod_metadatas::table.find(item.id).first::<ModMetadata>(&self.conn).optional()?;
            if existing_mod_metadata.is_none() {
                diesel::insert_into(mod_metadatas::table).values(mod_metadata::InsertFull{
                    id: item.id,

                    creator_store_id: local_data_store.id,
                    creator_store_time: new_time,

                    last_mod_store_id: local_data_store.id,
                    last_mod_store_time: new_time,
                }).execute(&self.conn)?;
            }

            // Add the modification event (both changes and newly created items require mod events).
            self.add_mod_event(&item, local_data_store.id, new_time)?;

            Ok(())
        })
    }

    /// LOCAL DATA STORE EVENT, i.e. this is used to record changes of local data_items on disk.
    ///
    /// Marks the given data item (and all its child items) as being deleted.
    /// This keeps their entries in the DB, but converts them to deletion notices.
    ///
    /// Correctly adds modification time stamps to the affected parent folders.
    pub fn delete_local_data_item(&self, path: &RelativePath) -> Result<usize> {
        // Any operation involving consistency of sync-time stamps and/or parent-child relations
        // between items in the database requires a consistent view of the invariants held.
        self.conn.transaction::<_, MetadataDBError, _>(|| {
            // We insert an item, bump the data stores version and mark all events with the version.
            let local_data_store = self.get_this_data_store()?;

            // Look for the item.
            let mut path_items = self.load_data_items_on_path(&local_data_store, &path)?;

            if path_items.len() != path.get_path_components().len() {
                // We have no parent item, i.e. this is already deleted.
                Ok(0)
            } else {
                let item = path_items.pop().unwrap();
                let parent = path_items.pop().unwrap();

                let deleted = self.delete_local_data_item_recursive(&parent, &item)?;
                Ok(deleted)
            }
        })
    }

    /// Syncs a local data item, i.e. updating its metadata, sync- and mod time.
    /// The method implicitly keeps invariants in the DB, e.g. sets sync time to be
    /// max(argument, current) and to update parent entries.
    ///
    /// MUST only do sensible sync operations and will throw ViolatesDBConsistency Errors
    /// otherwise. For example, it never makes sense to update the full modification vector of an
    /// item, as this vector MUST be explicitly be generated from its child items.
    pub fn sync_local_data_item(&self, path: &RelativePath, target_item: &DBItem) -> Result<()> {
        self.conn.transaction::<_, MetadataDBError, _>(|| {
            let local_data_store = self.get_this_data_store()?;

            // Look for existing items on this path.
            let path_items =
                self.load_data_items_on_path(&local_data_store, &path)?;

            // We are especially interested in the parent directory and a potentially existing item.
            let (parent_dir_item, existing_item) =
                Self::extract_parent_dir_and_item(path_items, path.path_component_number())?;

            if parent_dir_item.item.is_deleted {
                return Err(MetadataDBError::ViolatesDBConsistency {
                    message: "Must not insert data_item below an deleted db entry (i.e. no file without an existing parent folder)!"
                });
            }
            if parent_dir_item.item.is_file {
                return Err(MetadataDBError::ViolatesDBConsistency {
                    message: "Must not insert data_item that has a file as a parent!"
                });
            }

            // Associate item with the path (...or update an existing one, e.g.
            // for a previously deleted item that still requires a deletion notice in the DB).
            let item = if let Some(existing_item) = &existing_item {
                let item_will_be_deleted = !existing_item.item.is_deleted && target_item.is_deletion();
                let item_no_longer_folder = !existing_item.item.is_file && !target_item.is_folder();

                if  item_will_be_deleted || item_no_longer_folder {
                    // In case a previous folder now is none-anymore, we need to clean out
                    // all its children from the DB (completely remove them).
                    self.delete_db_entries_recursive(&existing_item.item, true)?;
                }

                // Remove un-needed entries for deleted items.
                if target_item.is_deletion() {
                    diesel::delete(mod_metadatas::table)
                        .filter(mod_metadatas::id.eq(existing_item.item.id))
                        .execute(&self.conn)?;
                    diesel::delete(file_system_metadatas::table)
                        .filter(file_system_metadatas::id.eq(existing_item.item.id))
                        .execute(&self.conn)?;
                }

                // Everything is ready to simply be 'synced up' with the target item.
                // This will also e.g. correctly setup the deletion status/folder status.
                diesel::update(items::table)
                    .filter(items::id.eq(existing_item.item.id))
                    .set((
                        items::is_file.eq(target_item.is_file()),
                        items::is_deleted.eq(target_item.is_deletion()),
                    )).execute(&self.conn)?;
                let mut result_item = existing_item.item.clone();
                result_item.is_file = target_item.is_file();
                result_item.is_deleted = target_item.is_deletion();

                result_item
            } else {
                let path_component =
                    self.ensure_path_exists(&path.name(), &parent_dir_item.path_component)?;

                // Just create a new item with the correct values.
                diesel::insert_into(items::table)
                    .values(item::InsertFull {
                        path_component_id: path_component.id,
                        data_store_id: local_data_store.id,

                        is_file: target_item.is_file(),
                        is_deleted: target_item.is_deletion(),
                    })
                    .execute(&self.conn)?;

                let new_item = items::table
                    .filter(items::path_component_id.eq(path_component.id))
                    .filter(items::data_store_id.eq(local_data_store.id))
                    .first::<Item>(&self.conn)?;

                new_item
            };

            if !target_item.is_deletion() {
                // FS Metadata can always be overwritten safely.
                diesel::replace_into(file_system_metadatas::table)
                    .values(file_system_metadata::InsertFull {
                        id: item.id,

                        case_sensitive_name: &target_item.metadata().case_sensitive_name,
                        creation_time: target_item.metadata().creation_time,
                        mod_time: target_item.metadata().mod_time,
                        hash: &target_item.metadata().hash,
                    }).execute(&self.conn)?;

                // Mod Metadata is tricky, as we want to e.g. keep the mod_times associated with
                // a folder.
                let mod_metadata_exits = existing_item.is_some() && !existing_item.as_ref().unwrap().item.is_deleted;
                if mod_metadata_exits {
                    diesel::update(mod_metadatas::table.find(item.id))
                        .set(mod_metadata::UpdateCreator{
                            creator_store_id: target_item.creation_store_id(),
                            creator_store_time: target_item.creation_store_time(),
                        }).execute(&self.conn)?;
                } else {
                    diesel::insert_into(mod_metadatas::table)
                        .values(mod_metadata::InsertFull {
                            id: item.id,

                            creator_store_id: target_item.creation_store_id(),
                            creator_store_time: target_item.creation_store_time(),

                            last_mod_store_id: target_item.last_mod_store_id(),
                            last_mod_store_time: target_item.last_mod_store_time(),
                        }).execute(&self.conn)?;
                }

                // Simply set the last_mod_time and let it bump the parent items mod times.
                // We never directly sync the mod_times (max in folders), these should always be
                // implicitly set by child items being updated.
                self.add_mod_event(&item, target_item.last_mod_store_id(), target_item.last_mod_store_time())?;
            }

            // ALL items in the db hold a sync time, thus always update it.
            // Sync times MUST always increase, i.e. we never loose information on a sync operation.
            let mut target_sync_time = if let Some(existing_item) = existing_item{
                existing_item.sync_time.unwrap()
            } else {
                parent_dir_item.sync_time.unwrap()
            };
            target_sync_time.max(&target_item.sync_time);
            self.update_sync_times(&item, &target_sync_time)?;

            Ok(())
        })
    }

    /// Load all existing internal items on the given path.
    ///
    /// If there are less items than the given path, the procedure simply stops and returns an
    /// incomplete list. It is the callers responsibility to check for this.
    /// Often used with extract_parent_dir_and_item to make sure the path was complete in the DB.
    ///
    /// Automatically loads sync and mod times if present.
    fn load_data_items_on_path(
        &self,
        for_data_store: &DataStore,
        path: &RelativePath,
    ) -> Result<Vec<DBItemInternal>> {
        // We handle all path's in lower case in here!
        let path = path.to_lower_case();

        // Required for sync time compression in the DB.
        let mut current_sync_time = VersionVector::<i64>::new();
        current_sync_time[&for_data_store.id] = for_data_store.time;

        // Note: Maybe re-work with 'WITH RECURSIVE' queries directly in sqlite.
        //       Wait for actual performance issues before trying to do this.
        let mut result = Vec::<DBItemInternal>::with_capacity(path.get_path_components().len());

        for path_component in path.get_path_components() {
            let parent_id = result.last().map(|item| item.path_component.id);
            let component_db_item = if let Some(parent_path_component_id) = parent_id {
                path_components::table
                    .filter(path_components::path_component.eq(path_component))
                    .filter(path_components::parent_component_id.eq(Some(parent_path_component_id)))
                    .inner_join(items::table)
                    .filter(items::data_store_id.eq(for_data_store.id))
                    .first::<(PathComponent, Item)>(&self.conn)
                    .optional()?
            } else {
                path_components::table
                    .filter(path_components::path_component.eq(path_component))
                    .filter(path_components::parent_component_id.is_null())
                    .inner_join(items::table)
                    .filter(items::data_store_id.eq(for_data_store.id))
                    .first::<(PathComponent, Item)>(&self.conn)
                    .optional()?
            };

            if let Some((path_component, item)) = component_db_item {
                let current_item = self.load_item(path_component, item, &current_sync_time)?;
                current_sync_time = current_item.sync_time.as_ref().unwrap().clone();

                result.push(current_item);
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// Loads all child items of the given internal db item.
    fn load_child_items(&self, parent_item: &DBItemInternal) -> Result<Vec<DBItemInternal>> {
        let dir_entries = path_components::table
            .filter(path_components::parent_component_id.eq(parent_item.path_component.id))
            .inner_join(items::table)
            .filter(items::data_store_id.eq(parent_item.item.data_store_id))
            .load::<(PathComponent, Item)>(&self.conn)?;

        let child_items: Result<Vec<_>> = dir_entries
            .into_iter()
            .map(|(path, item)| {
                let internal_item =
                    self.load_item(path, item, &parent_item.sync_time.as_ref().unwrap())?;
                Ok(internal_item)
            })
            .collect();
        child_items
    }

    // Given a vector of path items and the expected depth of the target_item on this path,
    // return it's parent directory and optionally the target_items itself.
    //
    // Returns an Error if even the parent_item does not exist.
    //
    // 'Normalizes' the root directory, i.e. it returns the root directory as the parent of the
    // root directory.
    fn extract_parent_dir_and_item(
        mut path_items: Vec<DBItemInternal>,
        target_item_depth: usize,
    ) -> Result<(DBItemInternal, Option<DBItemInternal>)> {
        if target_item_depth == 1 {
            // Special case for root directory.
            let parent_dir_item = path_items.pop().unwrap();
            let existing_item = Some(parent_dir_item.clone());

            Ok((parent_dir_item, existing_item))
        } else if path_items.len() == target_item_depth {
            let existing_item = Some(path_items.pop().unwrap());
            let parent_dir_item = path_items.pop().unwrap();

            Ok((parent_dir_item, existing_item))
        } else if path_items.len() == target_item_depth - 1 {
            let existing_item = None;
            let parent_dir_item = path_items.pop().unwrap();

            Ok((parent_dir_item, existing_item))
        } else {
            Err(MetadataDBError::ViolatesDBConsistency {
                message: "Must not insert data_item without existing parent item (i.e. no file without a parent folder)!"
            })
        }
    }

    /// Loads the remaining metadata for the given DB item (metadata entries and sync/mod times).
    /// Returns the complete internal item.
    fn load_item(
        &self,
        path_component: PathComponent,
        item: Item,
        parent_sync_time: &VersionVector<i64>,
    ) -> Result<DBItemInternal> {
        let fs_metadata = file_system_metadatas::table
            .find(item.id)
            .first::<FileSystemMetadata>(&self.conn)
            .optional()?;
        let mod_metadata = mod_metadatas::table
            .find(item.id)
            .first::<ModMetadata>(&self.conn)
            .optional()?;

        let mut current_item =
            DBItemInternal::from_db_query(path_component, item, fs_metadata, mod_metadata);

        // Always load the times from the DB in here. This way we can keep invariants on
        // DB compression (e.g. only store changed sync/mod times relative to parent)
        // inside this loading layer.
        if current_item.mod_metadata.is_some() {
            self.load_max_mod_time_for_folder(&mut current_item)?;
        }
        self.load_sync_time_for_item(&mut current_item)?;
        current_item
            .sync_time
            .as_mut()
            .unwrap()
            .max(&parent_sync_time);

        Ok(current_item)
    }

    /// Loads the sync time vector stored in the DB for this item.
    /// This only returns what is stored ON DISK. To get the final sync time of the item
    /// it must be determined by max(parent_sync, item_sync).
    fn load_sync_time_for_item(&self, data_item: &mut DBItemInternal) -> Result<()> {
        let sync_time_entries: Vec<SyncTime> = sync_times::table
            .filter(sync_times::item_id.eq(data_item.item.id))
            .load::<SyncTime>(&self.conn)?;

        let mut result_vector = VersionVector::<i64>::new();
        for sync_time in sync_time_entries {
            result_vector[&sync_time.data_store_id] = sync_time.time;
        }

        data_item.sync_time = Some(result_vector);

        Ok(())
    }

    /// Loads the mod time vector stored in the DB for this item.
    /// For now this will be the full vector, but we might change this in later iterations.
    fn load_max_mod_time_for_folder(&self, data_item: &mut DBItemInternal) -> Result<()> {
        if data_item.item.is_file || data_item.item.is_deleted {
            // Skip the loading, makes only sense for folders that exist
        } else {
            let mod_time_entries: Vec<ModTime> = mod_times::table
                .filter(mod_times::mod_metadata_id.eq(data_item.mod_metadata.as_ref().unwrap().id))
                .load::<ModTime>(&self.conn)?;

            let mut result_vector = VersionVector::<i64>::new();
            for mod_time in mod_time_entries {
                result_vector[&mod_time.data_store_id] = mod_time.time;
            }

            data_item.mod_time = Some(result_vector);
        }

        Ok(())
    }

    /// Inserts the given path_component into the DB if it does not already exist.
    /// Returns the - now existing - path_component DB entry.
    fn ensure_path_exists(&self, name: &str, parent: &PathComponent) -> Result<PathComponent> {
        let name = name.to_lowercase();

        let existing_path = path_components::table
            .filter(path_components::path_component.eq(&name))
            .filter(path_components::parent_component_id.eq(parent.id))
            .first::<PathComponent>(&self.conn)
            .optional()?;
        if let Some(existing_path) = existing_path {
            return Ok(existing_path);
        }

        diesel::insert_into(path_components::table)
            .values(path_component::InsertFull {
                parent_component_id: Some(parent.id),
                path_component: &name,
            })
            .execute(&self.conn)?;

        let new_path = path_components::table
            .filter(path_components::path_component.eq(&name))
            .filter(path_components::parent_component_id.eq(parent.id))
            .first::<PathComponent>(&self.conn)?;
        Ok(new_path)
    }

    /// Marks the given item and all its child items as deleted.
    /// This leaves their entries in the DB in the form of deletion notices.
    ///
    /// Correctly adds modification time stamps to the affected parent folders.
    fn delete_local_data_item_recursive(
        &self,
        parent: &DBItemInternal,
        item: &DBItemInternal,
    ) -> Result<usize> {
        let mut deleted = 0;

        // Make sure to delete children of folders recursively
        if !item.item.is_file {
            let dir_entries = self.load_child_items(&item)?;

            for dir_entry in dir_entries {
                deleted += self.delete_local_data_item_recursive(&item, &dir_entry)?;
            }
        }

        // Update Owner Info to be deleted
        if !item.item.is_deleted {
            // Register the change in deletion_status
            diesel::update(items::table)
                .filter(items::id.eq(item.item.id))
                .set(items::is_deleted.eq(true))
                .execute(&self.conn)?;

            // Push the parent folders last mod time
            self.increase_local_time()?;
            let local_data_store = self.get_this_data_store()?;
            let new_time = local_data_store.time;
            self.add_mod_event(&parent.item, local_data_store.id, new_time)?;

            deleted += 1;
        }

        // TODO: Pull this into clean-up procedure (as we e.g. might miss some of these clean ups
        //       during a sync anyways and they should never cause issues besides wasted storage).
        // Remove un-needed entries
        // No need for modification times of deleted items.
        diesel::delete(mod_metadatas::table)
            .filter(mod_metadatas::id.eq(item.item.id))
            .execute(&self.conn)?;
        // No need for metadata of deleted items.
        diesel::delete(file_system_metadatas::table)
            .filter(file_system_metadatas::id.eq(item.item.id))
            .execute(&self.conn)?;

        Ok(deleted)
    }

    /// Deletes all child DB entries of the given item.
    /// If passed delete_given_item == true: Also deletes the given item from the DB.
    /// If passed delete_given_item == false: Only deletes the child items from the DB.
    fn delete_db_entries_recursive(&self, item: &Item, delete_given_item: bool) -> Result<()> {
        // Make sure to delete children of folders recursively
        if !item.is_file {
            let dir_entries = path_components::table
                .filter(path_components::parent_component_id.eq(item.path_component_id))
                .inner_join(items::table)
                .filter(items::data_store_id.eq(item.data_store_id))
                .load::<(PathComponent, Item)>(&self.conn)?;

            for (_path, item) in dir_entries {
                self.delete_db_entries_recursive(&item, false)?;
            }
        }

        if !delete_given_item {
            // For child items we remove everything from the DB (as it would be cleaned up anyways).
            // Mod times, sync times and metadata should be deleted by cascade rules.
            diesel::delete(items::table.find(item.id)).execute(&self.conn)?;
        }

        Ok(())
    }

    /// Updates the modification time of the given item (via its owner information) to
    /// include the given modification done by a the given data store at the given time stamp
    /// (i.e. it sets the item's modification time to MAX{current_mod_vector, given_mod_event}).
    ///
    /// Makes sure that all parent items are updated appropriately (i.e. all parent items
    /// stick to the DB invariant mod_time = MAX{child mod times}).
    fn add_mod_event(
        &self,
        item: &Item,
        modifying_data_store_id: i64,
        modification_time: i64,
    ) -> Result<()> {
        self.conn.transaction::<_, MetadataDBError, _>(|| {
            let changes = diesel::update(mod_metadatas::table.find(item.id))
                .set(mod_metadata::UpdateLastMod {
                    last_mod_store_id: modifying_data_store_id,
                    last_mod_store_time: modification_time,
                })
                .execute(&self.conn)?;
            assert_eq!(
                changes, 1,
                "Must not add modification event for non existing mod_metadata!"
            );

            self.bump_path_mod_times(&item, modifying_data_store_id, modification_time)?;

            Ok(())
        })?;

        Ok(())
    }
    /// Updates the modification times of all the given item's parent
    /// items to include the given time in their MAX(children) modification time.
    fn bump_path_mod_times(
        &self,
        item: &Item,
        modifying_data_store_id: i64,
        modification_time: i64,
    ) -> Result<()> {
        let mut current_item = item.clone();
        loop {
            let current_mod_vector = self.get_mod_times(&current_item)?;

            let mut new_mod_vector = current_mod_vector;
            new_mod_vector[&modifying_data_store_id] =
                std::cmp::max(modification_time, new_mod_vector[&modifying_data_store_id]);
            if !current_item.is_file {
                self.update_mod_times(&current_item, &new_mod_vector)?;
            }

            let parent_path_component_id = path_components::table
                .find(current_item.path_component_id)
                .select(path_components::parent_component_id)
                .first::<Option<i64>>(&self.conn)?;
            if parent_path_component_id.is_none() {
                break; // Reached root level
            }
            let parent_path_id = parent_path_component_id.unwrap();

            let parent_item = items::table
                .filter(items::path_component_id.eq(parent_path_id))
                .filter(items::data_store_id.eq(item.data_store_id))
                .first::<Item>(&self.conn)?;
            current_item = parent_item;
        }

        Ok(())
    }
    /// Queries the modification time vector for a given item
    pub fn get_mod_times(&self, item: &Item) -> Result<VersionVector<i64>> {
        let mod_times: Vec<ModTime> = mod_times::table
            .filter(mod_times::mod_metadata_id.eq(item.id))
            .load(&self.conn)?;

        let mut result = VersionVector::new();
        for mod_time in &mod_times {
            result[&mod_time.data_store_id] = mod_time.time;
        }

        Ok(result)
    }
    /// Updates the modification times of an DB entry by replacing all
    /// given vector time entries (represented by their data_store id).
    fn update_mod_times(&self, item: &Item, new_mod_times: &VersionVector<i64>) -> Result<()> {
        let new_db_entries: Vec<_> = new_mod_times
            .iter()
            .map(|(data_store_id, time)| mod_time::InsertFull {
                mod_metadata_id: item.id,
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
    /// existing entries with the given vector entries.
    ///
    /// DOES NOT remove non-mentioned entries.
    fn update_sync_times(&self, item: &Item, new_sync_times: &VersionVector<i64>) -> Result<()> {
        let new_db_entries: Vec<_> = new_sync_times
            .iter()
            .map(|(data_store_id, time)| sync_time::InsertFull {
                item_id: item.id,
                data_store_id: *data_store_id,
                time: *time,
            })
            .collect();

        diesel::replace_into(sync_times::table)
            .values(new_db_entries)
            .execute(&self.conn)?;

        Ok(())
    }

    /// Inserts a root item for the given data store.
    /// This includes associated metadata entries.
    fn create_root_item(&self, data_store: &DataStore) -> Result<()> {
        let root_path = path_components::table
            .filter(path_components::parent_component_id.is_null())
            .first::<PathComponent>(&self.conn)
            .optional()?;
        let root_path = if let Some(root_path) = root_path {
            root_path
        } else {
            diesel::insert_into(path_components::table)
                .values(path_component::InsertFull {
                    path_component: "",
                    parent_component_id: None,
                })
                .execute(&self.conn)?;
            path_components::table
                .filter(path_components::parent_component_id.is_null())
                .first::<PathComponent>(&self.conn)?
        };

        diesel::insert_into(items::table)
            .values(item::InsertFull {
                data_store_id: data_store.id,
                path_component_id: root_path.id,

                is_file: false,
                is_deleted: false,
            })
            .execute(&self.conn)?;
        let root_item = items::table
            .filter(items::path_component_id.eq(root_path.id))
            .filter(items::data_store_id.eq(data_store.id))
            .first::<Item>(&self.conn)?;

        diesel::insert_into(mod_metadatas::table)
            .values(mod_metadata::InsertFull {
                id: root_item.id,

                creator_store_id: data_store.id,
                creator_store_time: 0,

                last_mod_store_id: data_store.id,
                last_mod_store_time: 0,
            })
            .execute(&self.conn)?;
        diesel::insert_into(file_system_metadatas::table)
            .values(file_system_metadata::InsertFull {
                id: root_item.id,

                case_sensitive_name: "",
                creation_time: chrono::NaiveDateTime::from_timestamp(0, 0),
                mod_time: chrono::NaiveDateTime::from_timestamp(0, 0),
                hash: "",
            })
            .execute(&self.conn)?;

        Ok(())
    }

    /// Helper that increases the version of the local data store.
    /// Frequently used when working with data items.
    fn increase_local_time(&self) -> Result<()> {
        diesel::update(data_stores::table)
            .filter(data_stores::is_this_store.eq(true))
            .set(data_stores::time.eq(data_stores::time + 1))
            .execute(&self.conn)?;

        Ok(())
    }

    /// Upgrades the DB to the most recent schema version.
    fn upgrade_db(&self) -> db_migration::Result<()> {
        self.conn
            .transaction(|| db_migration::upgrade_db(&self.conn))?;

        Ok(())
    }

    /// Changes the connection DB settings to our default usage pattern.
    fn default_db_settings(&self) -> Result<()> {
        sql_query("PRAGMA locking_mode = EXCLUSIVE").execute(&self.conn)?;
        sql_query("PRAGMA journal_mode = WAL").execute(&self.conn)?;
        sql_query("PRAGMA foreign_keys = 1").execute(&self.conn)?;
        sql_query("PRAGMA cache_size = -64000").execute(&self.conn)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests;
