// Database schema - must be kept up to date manually
mod schema;
use self::schema::*;
pub mod entity;
pub use self::entity::*;
// Helpers for handling more complex query scenarios without cluttering logic.
mod queries;
// External representation of the DB.
mod db_item;
pub use self::db_item::*;
mod db_inclusion_rule;
pub use self::db_inclusion_rule::*;
// Error boilerplate
mod errors;
pub use self::errors::*;
mod db_migration;

use crate::fs_interaction::relative_path::RelativePath;
use crate::version_vector::VersionVector;

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sqlite::SqliteConnection;
use std::cell::RefCell;
use std::cmp::max;

const UPDATES_UNTIL_OPTIMIZATION: usize = 10_000;

pub struct MetadataDB {
    conn: SqliteConnection,
    // Caching local data store
    local_datastore: RefCell<Option<DataStore>>,
    // Optimize the DB after a big number of inserts
    updates_since_optimization: RefCell<usize>,
    // Allow to relax/disable nested transactions
    is_bundled: RefCell<bool>,
}

impl MetadataDB {
    /// Opens the metadata db file located at the given path and performs data migrations to
    /// the current application version if required.
    pub fn open(path: &str) -> Result<MetadataDB> {
        let result = MetadataDB {
            conn: SqliteConnection::establish(path)?,

            local_datastore: RefCell::new(None),
            updates_since_optimization: RefCell::new(0),

            is_bundled: RefCell::new(false),
        };

        result.default_db_settings()?;
        result.upgrade_db()?;

        Ok(result)
    }

    /// Performs a clean-up operation on the local database, removing any redundant information.
    /// Also re-builds the DB to shrink the file size and analyze it for future queries.
    /// Should be run from time to time to decrease the DB size on disk.
    pub fn optimize_db(&self) -> Result<()> {
        self.clean_up_db()?;
        diesel::sql_query("ANALYZE").execute(&self.conn)?;
        diesel::sql_query("VACUUM").execute(&self.conn)?;

        Ok(())
    }

    /// Performs a clean-up operation on the local database, removing any redundant information.
    pub fn clean_up_db(&self) -> Result<()> {
        self.clean_up_local_sync_times()?;
        self.clean_up_deleted_items()?;
        self.clean_up_path_components()?;

        Ok(())
    }

    // Run the given function 'bundled' on the database.
    // This means, that the inner function is run inside a transaction and that we will turn off
    // any nested transactions. In other words, all actions done inside are either executed as a
    // unit or not at all.
    // As a regular transaction, rolls back all changes if an error is returned.
    pub fn run_bundled<F: FnMut() -> std::result::Result<V, E>, V, E>(
        &self,
        mut func: F,
    ) -> Result<std::result::Result<V, E>> {
        enum InnerError<V, E> {
            Inner(std::result::Result<V, E>),
            SqlError { source: diesel::result::Error },
        }
        impl<V, E> From<diesel::result::Error> for InnerError<V, E> {
            fn from(error: diesel::result::Error) -> Self {
                Self::SqlError { source: error }
            }
        }

        let inner_result = self.conn.transaction(|| {
            *self.is_bundled.borrow_mut() = true;
            let result = func();
            *self.is_bundled.borrow_mut() = false;

            // Simulate an error in case the internal function errored out.
            // This will rollback the transaction.
            if result.is_err() {
                Err(InnerError::Inner(result))
            } else {
                Ok(result)
            }
        });

        // In case we had an db error on the transaction, return that.
        // In all other cases, return the inner functions result.
        match inner_result {
            Ok(result) => Ok(result),
            Err(InnerError::Inner(result)) => Ok(result),
            Err(InnerError::SqlError { source }) => {
                Err(MetadataDBError::GenericSQLError { source })
            }
        }
    }
    fn run_transaction<F: FnMut() -> Result<R>, R>(&self, mut func: F) -> Result<R> {
        if *self.is_bundled.borrow_mut() {
            func()
        } else {
            self.conn.transaction(|| func())
        }
    }

    /// Creates and returns the data set stored in the open MetadataDB.
    /// Currently, exactly one data set can be stored in one database.
    pub fn create_data_set(&self, unique_name_p: &str) -> Result<DataSet> {
        use self::schema::data_sets::dsl::*;

        Ok(self.run_transaction(|| {
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

    /// Returns a vector of file inclusion rules for the given data store.
    /// This represents our knowledge of the remote data stores inclusion/exclusion rules.
    pub fn get_inclusion_rules(&self, data_store: &DataStore) -> Result<Vec<DBInclusionRule>> {
        let result = inclusion_rules::table
            .filter(inclusion_rules::data_store_id.eq(data_store.id))
            .load::<InclusionRule>(&self.conn)?
            .into_iter()
            .map(|db_entry| DBInclusionRule {
                rule: glob::Pattern::new(&db_entry.rule_glob).unwrap(),
                include: db_entry.include,
            })
            .collect();
        Ok(result)
    }

    /// Sets the file inclusion rules for the given data store.
    /// Validation that the rules are valid glob patterns must be performed externally!
    pub fn set_inclusion_rules(
        &self,
        data_store: &DataStore,
        rules: &Vec<DBInclusionRule>,
    ) -> Result<()> {
        self.conn.transaction(|| {
            diesel::delete(
                inclusion_rules::table.filter(inclusion_rules::data_store_id.eq(data_store.id)),
            )
            .execute(&self.conn)?;

            let new_rules: Vec<_> = rules
                .iter()
                .map(|rule| inclusion_rule::InsertFull {
                    data_store_id: data_store.id,
                    rule_glob: rule.rule.to_string(),
                    include: rule.include,
                })
                .collect();
            diesel::insert_into(inclusion_rules::table)
                .values(new_rules)
                .execute(&self.conn)?;

            // We bump our local time counter when changing these rules.
            // That way, others can notice that our database changed even without a new
            // file modification event.
            self.increase_local_time()?;

            Ok(())
        })
    }

    /// Creates a new data store in the open MetadataDB.
    /// At most one data store must be the local one and this methods reports an consistency
    /// error if violated.
    pub fn create_data_store(&self, data_store: &data_store::InsertFull) -> Result<DataStore> {
        use diesel::dsl::*;

        let result = self.run_transaction(|| {
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
                self.create_include_all_glob(&inserted_data_store)?;
            }

            Ok(inserted_data_store)
        })?;

        Ok(result)
    }

    /// Returns the local data store of the open MetadataDB.
    pub fn get_local_data_store(&self) -> Result<DataStore> {
        use self::schema::data_stores::dsl::*;

        let mut cache = self.local_datastore.borrow_mut();
        if let Some(cache_content) = cache.as_mut() {
            Ok(cache_content.clone())
        } else {
            let loaded_store = data_stores
                .filter(is_this_store.eq(true))
                .first::<DataStore>(&self.conn)?;

            *cache = Some(loaded_store.clone());

            Ok(loaded_store)
        }
    }

    /// Helper that increases the version of the local data store.
    /// Frequently used when working with data items.
    fn increase_local_time(&self) -> Result<i64> {
        // Update cached value.
        let mut data_store = self.get_local_data_store()?;
        data_store.time += 1;
        let result = data_store.time;
        *self.local_datastore.borrow_mut() = Some(data_store);

        // Update actual value.
        diesel::update(data_stores::table.filter(data_stores::is_this_store.eq(true)))
            .set(data_stores::time.eq(data_stores::time + 1))
            .execute(&self.conn)?;

        Ok(result)
    }

    /// Queries a data item from the DB and returns it.
    /// Data items must always exist, as there is at least a deletion notice for everything.
    pub fn get_local_data_item(
        &self,
        path: &RelativePath,
        load_timestamps: bool,
    ) -> Result<DBItem> {
        // Any operation involving consistency of sync-time stamps and/or parent-child relations
        // between items in the database requires a consistent view of the invariants held.
        self.run_transaction(|| {
            let local_data_store = self.get_local_data_store()?;
            let mut path_items =
                self.load_data_items_on_path(&local_data_store, &path, load_timestamps)?;

            if path_items.len() == path.get_path_components().len() {
                // The item has an actual entry in the db, inspect it further.
                let mut target_item = path_items.pop().unwrap();
                if !load_timestamps {
                    target_item.sync_time = Some(VersionVector::new());
                    target_item.mod_time = Some(VersionVector::new());
                }
                Ok(DBItem::from_internal_item(&path_items, target_item))
            } else {
                // The item has no more entry in the db, thus we 'create' a deletion notice.
                let mut last_db_entry = path_items.pop().unwrap();
                if !load_timestamps {
                    last_db_entry.sync_time = Some(VersionVector::new());
                    last_db_entry.mod_time = Some(VersionVector::new());
                }
                Ok(DBItem {
                    path: path.clone(),
                    sync_time: last_db_entry.sync_time.unwrap(),

                    content: ItemType::DELETION,
                })
            }
        })
    }

    /// Queries all item names (NOT case sensitive) present in the given dir_path.
    pub fn get_local_child_items(
        &self,
        dir_path: &RelativePath,
        load_timestamps: bool,
    ) -> Result<Vec<DBItem>> {
        self.run_transaction(|| {
            let local_data_store = self.get_local_data_store()?;
            let mut dir_path_items =
                self.load_data_items_on_path(&local_data_store, dir_path, load_timestamps)?;

            if dir_path_items.len() == dir_path.get_path_components().len() {
                // The parent directory exists, go and inspect it further.
                // The last item in the chain of DB entries is the desired folder item.
                let dir_item = dir_path_items.last_mut().unwrap();

                // Query its content/children.
                Ok(self
                    .load_child_items(&dir_item, load_timestamps)?
                    .into_iter()
                    .map(|internal_item| DBItem::from_internal_item(&dir_path_items, internal_item))
                    .collect())
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
        is_read_only: bool,
    ) -> Result<()> {
        self.run_transaction(|| {
            // We insert an item, bump the data stores version and mark all events with the version.
            let new_time = self.increase_local_time()?;
            let local_data_store = self.get_local_data_store()?;

            // Load all existing items on the given path.
            let mut path_items =
                self.load_data_items_on_path(&local_data_store, &path, true)?;
            let (parent_dir_item, existing_item) =
                Self::extract_parent_dir_and_item(&path_items, path.path_component_number())?;

            let (path_component, item) = if let Some(existing_item) = existing_item {
                if (is_file && existing_item.item.file_type == FileType::DIRECTORY) ||
                    (!is_file && existing_item.item.file_type == FileType::FILE) {
                    return Err(MetadataDBError::ViolatesDBConsistency {
                        message: "Must not change types of entries in the DB. Delete and re-create them instead!",
                    })
                }

                // ...update it to reflect the change.
                diesel::update(items::table.filter(items::id.eq(existing_item.item.id)))
                    .set(
                        items::file_type.eq(if is_file { FileType::FILE } else { FileType::DIRECTORY })
                    )
                    .execute(&self.conn)?;

                let mut item = existing_item.item.clone();
                item.file_type = if is_file { FileType::FILE } else { FileType::DIRECTORY };

                (existing_item.path_component.clone(), item)
            } else {
                let path_component =
                    self.ensure_path_exists(path.name(), Some(&parent_dir_item.path_component))?;

                diesel::insert_into(items::table)
                    .values(item::InsertFull {
                        path_component_id: path_component.id,
                        data_store_id: local_data_store.id,

                        file_type: if is_file { FileType::FILE } else { FileType::DIRECTORY },
                    })
                    .execute(&self.conn)?;

                let item = items::table
                    .filter(items::path_component_id.eq(path_component.id))
                    .filter(items::data_store_id.eq(local_data_store.id))
                    .first::<Item>(&self.conn)?;
                (path_component, item)
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

                    is_read_only: is_read_only,
                }).execute(&self.conn)?;
            let fs_metadata = file_system_metadatas::table.find(item.id).first::<FileSystemMetadata>(&self.conn)?;

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
            let mod_metadata = mod_metadatas::table.find(item.id).first::<ModMetadata>(&self.conn)?;

            if existing_item.is_some() && path_items.len() == 1 {
                // Root item, do not touch item chain.
            } else {
                if existing_item.is_some() {
                    path_items.pop();
                }
                let new_item_internal = self.load_item(path_component, item, Some(fs_metadata), Some(mod_metadata), &path_items.last().unwrap().sync_time.as_ref().unwrap())?;
                path_items.push(new_item_internal);
            }

            // Add the modification event (both changes and newly created items require mod events).
            self.add_mod_event(&path_items, local_data_store.id, new_time)?;

            self.notify_change_for_optimization()?;
            Ok(())
        })
    }

    /// LOCAL DATA STORE EVENT, i.e. this is used to record changes of local data_items on disk.
    ///
    /// Marks the given data item (and all its child items) as being deleted.
    /// This removes all child entries completely from the DB and marks the current entry as
    /// deleted (which in turn will be cleaned up if the sync times match up in the directory).
    ///
    /// Correctly adds modification time stamps to the affected parent folders.
    pub fn delete_local_data_item(&self, path: &RelativePath) -> Result<()> {
        self.run_transaction(|| {
            // We insert an item, bump the data stores version and mark all events with the version.
            let local_data_store = self.get_local_data_store()?;

            // Look for the item.
            let mut path_items = self.load_data_items_on_path(&local_data_store, &path, true)?;

            if path_items.len() != path.get_path_components().len() {
                // We have no item in the DB, i.e. this is already implicitly deleted.
                Ok(())
            } else {
                let existing_item = path_items.pop().unwrap();
                if existing_item.item.file_type != FileType::DELETED {
                    self.delete_child_db_entries(&existing_item)?;
                    diesel::update(items::table.filter(items::id.eq(existing_item.item.id)))
                        .set(items::file_type.eq(FileType::DELETED))
                        .execute(&self.conn)?;
                    self.delete_item_metadata(&existing_item)?;

                    // Push the parent folders last mod time
                    let new_time = self.increase_local_time()?;
                    let local_data_store = self.get_local_data_store()?;
                    self.add_mod_event(&path_items, local_data_store.id, new_time)?;

                    self.notify_change_for_optimization()?;
                }
                Ok(())
            }
        })
    }

    /// LOCAL DATA STORE EVENT, i.e. this is used to record changes of local data_items on disk.
    ///
    /// Marks the given data item (and all its child items) as being ignored.
    /// This deletes all child entries and marks the current entry as ignore.
    /// If the information in the folder was not already 'synced outwards' it is lost,
    /// if it was synced to another store, the other store will still keep it.
    ///
    /// Does not affect any modification times.
    pub fn ignore_local_data_item(&self, path: &RelativePath) -> Result<()> {
        self.run_transaction(|| {
            let local_data_store = self.get_local_data_store()?;

            // Look for the item.
            let path_items = self.load_data_items_on_path(&local_data_store, &path, true)?;
            let (_parent_dir_item, existing_item) =
                Self::extract_parent_dir_and_item(&path_items, path.path_component_number())?;

            if let Some(existing_item) = existing_item {
                // An entry exists. Just delete all its children and mark it ignored.
                self.delete_child_db_entries(&existing_item)?;
                diesel::update(items::table.filter(items::id.eq(existing_item.item.id)))
                    .set(items::file_type.eq(FileType::IGNORED))
                    .execute(&self.conn)?;
                // In contrast to deleted items we keep its metadata. We can still sync
                // 'only the metadata' when fetching changes to an remote. This way, the mod/sync
                // timestamps are consistent in respect ot sync=min(children) and mod=max(children).

                self.notify_change_for_optimization()?;
                Ok(())
            } else {
                Err(MetadataDBError::ViolatesDBConsistency {
                    message: "Must not ignore non-existing items!",
                })
            }
        })
    }

    /// LOCAL DATA STORE EVENT, i.e. this is used to record changes of local data_items on disk.
    ///
    /// Marks the given data item (and all its child items) as 'reset',
    /// i.e. the items are set to an initial clean state, with no information on them.
    /// This means, that for all items we will have deletion notices at time 0.
    ///
    /// Does not affect any modification times.
    pub fn reset_local_data_item(&self, path: &RelativePath) -> Result<()> {
        self.run_transaction(|| {
            let local_data_store = self.get_local_data_store()?;

            // Look for the item.
            let path_items = self.load_data_items_on_path(&local_data_store, &path, true)?;
            let (_parent_dir_item, existing_item) =
                Self::extract_parent_dir_and_item(&path_items, path.path_component_number())?;

            if let Some(existing_item) = existing_item {
                // An entry exists. Delete all its children and mark it deleted...
                self.delete_child_db_entries(&existing_item)?;
                diesel::update(items::table.filter(items::id.eq(existing_item.item.id)))
                    .set(items::file_type.eq(FileType::DELETED))
                    .execute(&self.conn)?;
                self.delete_item_metadata(&existing_item)?;
                // ...the last step is to reset the sync time of the item and all its parent items
                // down to a zero vector. This requires 'bubbling' up the change and setting
                // all other children's sync times on the way.
                self.reset_sync_time_recursive(path_items)?;

                self.notify_change_for_optimization()?;
                Ok(())
            } else {
                Err(MetadataDBError::ViolatesDBConsistency {
                    message: "Must not ignore non-existing items!",
                })
            }
        })
    }

    fn reset_sync_time_recursive(&self, mut path_items: Vec<DBItemInternal>) -> Result<()> {
        let current_item = path_items.pop();

        if let Some(current_item) = current_item {
            // Explicitly write the sync time vector to the child items, so we loose nothing
            // when resetting the parents sync time to zero.
            let child_items = self.load_child_items(&current_item, true)?;
            for child_item in child_items {
                let mut child_sync_time = child_item.sync_time.unwrap();
                child_sync_time.max(&current_item.sync_time.as_ref().unwrap());
                self.update_sync_times(&child_item.item, &child_sync_time)?;
            }

            // Recurse up, this will also set the sync time of current_item.
            self.reset_sync_time_recursive(path_items)?;

            // Now reset the sync time of current_item to 0 (deleting entries equal zero entries).
            diesel::delete(sync_times::table.filter(sync_times::item_id.eq(current_item.item.id)))
                .execute(&self.conn)?;
        }

        Ok(())
    }

    /// Syncs a local data item, i.e. updating its metadata, sync- and mod time.
    /// The method implicitly keeps invariants in the DB, e.g. sets sync time to be
    /// max(argument, current) and to update parent entries.
    ///
    /// MUST only do sensible sync operations and will throw ViolatesDBConsistency Errors
    /// otherwise. For example, it never makes sense to update the full modification vector of an
    /// item, as this vector MUST be explicitly be generated from its child items.
    pub fn sync_local_data_item(&self, path: &RelativePath, target_item: &DBItem) -> Result<()> {
        self.run_transaction(|| {
            let local_data_store = self.get_local_data_store()?;

            // Look for existing items on this path.
            let mut path_items = self.load_data_items_on_path(&local_data_store, &path, true)?;
            let (parent_dir_item, existing_item) =
                Self::extract_parent_dir_and_item(&path_items, path.path_component_number())?;

            // Associate item with the path (...or update an existing one, e.g.
            // for a previously deleted item that still requires a deletion notice in the DB).
            let (path_component, item) = if let Some(existing_item) = existing_item {
                let item_will_be_deleted =
                    existing_item.item.file_type != FileType::DELETED && target_item.is_deletion();
                let item_no_longer_folder =
                    existing_item.item.file_type == FileType::DIRECTORY && target_item.is_file();
                let item_will_be_ignored =
                    existing_item.item.file_type != FileType::IGNORED && target_item.is_ignored();

                if item_will_be_deleted || item_no_longer_folder || item_will_be_ignored {
                    // In case a previous folder now is none-anymore, we need to clean out
                    // all its children from the DB (completely remove them).
                    self.delete_child_db_entries(&existing_item)?;
                }

                // Remove un-needed metadata entries for deleted items.
                if target_item.is_deletion() {
                    diesel::delete(
                        mod_metadatas::table.filter(mod_metadatas::id.eq(existing_item.item.id)),
                    )
                    .execute(&self.conn)?;
                    diesel::delete(
                        file_system_metadatas::table
                            .filter(file_system_metadatas::id.eq(existing_item.item.id)),
                    )
                    .execute(&self.conn)?;
                }

                // Everything is ready to simply be 'synced up' with the target item.
                // This will also e.g. correctly setup the deletion status/folder status.
                diesel::update(items::table.filter(items::id.eq(existing_item.item.id)))
                    .set((items::file_type.eq(target_item.file_type()),))
                    .execute(&self.conn)?;

                let mut item = existing_item.item.clone();
                item.file_type = target_item.file_type();

                (existing_item.path_component.clone(), item)
            } else {
                let path_component =
                    self.ensure_path_exists(&path.name(), Some(&parent_dir_item.path_component))?;

                // Just create a new item with the correct values.
                diesel::insert_into(items::table)
                    .values(item::InsertFull {
                        path_component_id: path_component.id,
                        data_store_id: local_data_store.id,

                        file_type: target_item.file_type(),
                    })
                    .execute(&self.conn)?;

                let new_item = items::table
                    .filter(items::path_component_id.eq(path_component.id))
                    .filter(items::data_store_id.eq(local_data_store.id))
                    .first::<Item>(&self.conn)?;

                (path_component, new_item)
            };

            if !target_item.is_deletion() {
                // FS Metadata can always be overwritten safely.
                if !target_item.is_ignored() {
                    diesel::replace_into(file_system_metadatas::table)
                        .values(file_system_metadata::InsertFull {
                            id: item.id,

                            case_sensitive_name: &target_item.metadata().case_sensitive_name,
                            creation_time: target_item.metadata().creation_time,
                            mod_time: target_item.metadata().mod_time,
                            hash: &target_item.metadata().hash,

                            is_read_only: target_item.metadata().is_read_only,
                        })
                        .execute(&self.conn)?;
                }

                // Mod Metadata is tricky, as we want to e.g. keep the mod_times associated with
                // a folder.
                let mod_metadata_exits = existing_item.is_some()
                    && existing_item.as_ref().unwrap().item.file_type != FileType::DELETED;
                if mod_metadata_exits {
                    diesel::update(mod_metadatas::table.find(item.id))
                        .set(mod_metadata::UpdateCreator {
                            creator_store_id: target_item.creation_store_id(),
                            creator_store_time: target_item.creation_store_time(),
                        })
                        .execute(&self.conn)?;
                } else {
                    diesel::insert_into(mod_metadatas::table)
                        .values(mod_metadata::InsertFull {
                            id: item.id,

                            creator_store_id: target_item.creation_store_id(),
                            creator_store_time: target_item.creation_store_time(),

                            last_mod_store_id: target_item.last_mod_store_id(),
                            last_mod_store_time: target_item.last_mod_store_time(),
                        })
                        .execute(&self.conn)?;
                }
            }

            // ALL items in the db hold a sync time, thus always update it.
            // Sync times MUST always increase, i.e. we never loose information on a sync operation.
            let mut target_sync_time = if let Some(existing_item) = existing_item {
                existing_item.sync_time.clone().unwrap()
            } else {
                parent_dir_item.sync_time.clone().unwrap()
            };
            target_sync_time.max(&target_item.sync_time);
            self.update_sync_times(&item, &target_sync_time)?;

            if !target_item.is_deletion() {
                let fs_metadata = file_system_metadatas::table
                    .find(item.id)
                    .first::<FileSystemMetadata>(&self.conn)
                    .optional()?;
                let mod_metadata = mod_metadatas::table
                    .find(item.id)
                    .first::<ModMetadata>(&self.conn)
                    .optional()?;

                let existing_item = existing_item.cloned();
                if existing_item.is_some() && path_items.len() == 1 {
                    // Root item, do not touch item chain.
                } else {
                    if existing_item.is_some() {
                        path_items.pop();
                    }
                    let new_item_internal = self.load_item(
                        path_component,
                        item,
                        fs_metadata,
                        mod_metadata,
                        &path_items.last().unwrap().sync_time.as_ref().unwrap(),
                    )?;
                    path_items.push(new_item_internal);
                }

                // For mod times we want to add any new changes also to the parent items.
                if target_item.is_folder() || target_item.is_ignored() {
                    let target_mod_time = target_item.mod_time();
                    let local_mod_time = existing_item
                        .map(|item| item.mod_time.unwrap_or_else(VersionVector::new))
                        .unwrap_or_else(VersionVector::new);
                    for (target_store, target_time) in target_mod_time.iter() {
                        if local_mod_time[&target_store] < *target_time {
                            self.add_mod_event(
                                &path_items,
                                target_item.last_mod_store_id(),
                                target_item.last_mod_store_time(),
                            )?;
                        }
                    }
                }
                self.add_mod_event(
                    &path_items,
                    target_item.last_mod_store_id(),
                    target_item.last_mod_store_time(),
                )?;
            }

            self.notify_change_for_optimization()?;
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
        load_timestamps: bool,
    ) -> Result<Vec<DBItemInternal>> {
        // We handle all path's in lower case in here!
        let path = path.to_lower_case();

        // Required for sync time compression in the DB.
        let mut current_sync_time = VersionVector::<i64>::new();
        current_sync_time[&for_data_store.id] = for_data_store.time;

        // We tried two db layouts: one with full path's in each element and one with a
        // 'linked chain' style where each path item references its parent.
        // We tried WITH RECURSIVE queries for all styles, and find the current implementation
        // to be fastest. It has the downside of - in first tests - using about 180% of the
        // disk space the 'basic' version would (the difference will become only slimmer if we
        // add more metadata to the DB; also, the DB size will not scale up with more sync sites).
        let path_string = format!("{}/", path.get_path_components().join("/"));
        queries::ItemLoader {
            path_query: queries::AllPathComponents { path_string },
            item_query: items::table.filter(items::data_store_id.eq(for_data_store.id)),
        }
        .get_results::<queries::ItemLoaderResult>(&self.conn)?
        .into_iter()
        .map(|(path, item, fs_metadata, mod_metadata)| {
            Ok(if load_timestamps {
                let item =
                    self.load_item(path, item, fs_metadata, mod_metadata, &current_sync_time)?;
                current_sync_time = item.sync_time.as_ref().unwrap().clone();
                item
            } else {
                DBItemInternal::from_db_query(path, item, fs_metadata, mod_metadata)
            })
        })
        .collect()
    }

    /// Loads all child items of the given internal db item.
    fn load_child_items(
        &self,
        parent_item: &DBItemInternal,
        load_timestamps: bool,
    ) -> Result<Vec<DBItemInternal>> {
        let dir_entries = queries::ItemLoader {
            path_query: path_components::table
                .filter(path_components::parent_id.eq(parent_item.path_component.id)),
            item_query: items::table
                .filter(items::data_store_id.eq(parent_item.item.data_store_id)),
        }
        .get_results::<queries::ItemLoaderResult>(&self.conn)?;

        let child_items: Result<Vec<_>> = dir_entries
            .into_iter()
            .map(|(path, item, fs_metadata, mod_metadata)| {
                if load_timestamps {
                    Ok(self.load_item(
                        path,
                        item,
                        fs_metadata,
                        mod_metadata,
                        &parent_item.sync_time.as_ref().unwrap(),
                    )?)
                } else {
                    let mut item =
                        DBItemInternal::from_db_query(path, item, fs_metadata, mod_metadata);
                    item.mod_time = Some(VersionVector::new());
                    item.sync_time = Some(VersionVector::new());
                    Ok(item)
                }
            })
            .collect();
        child_items
    }

    /// Given a vector of path items and the expected depth of the target_item on this path,
    /// return it's parent directory and optionally the target_items itself.
    ///
    /// Returns an Error if even the parent_item does not exist.
    /// Returns an Error if the parent_item is no valid directory.
    ///
    /// 'Normalizes' the root directory, i.e. it returns the root directory as the parent of the
    /// root directory.
    fn extract_parent_dir_and_item(
        path_items: &Vec<DBItemInternal>,
        target_item_depth: usize,
    ) -> Result<(&DBItemInternal, Option<&DBItemInternal>)> {
        if target_item_depth == 1 {
            // Special case for root directory.
            let parent_dir_item = path_items.last().unwrap();
            let existing_item = Some(path_items.last().unwrap());

            Self::validate_items_as_parent_dir(&parent_dir_item)?;
            Ok((&parent_dir_item, existing_item))
        } else if path_items.len() == target_item_depth {
            let existing_item = Some(path_items.last().unwrap());
            let parent_dir_item = path_items.get(path_items.len() - 2).unwrap();

            Self::validate_items_as_parent_dir(&parent_dir_item)?;
            Ok((parent_dir_item, existing_item))
        } else if path_items.len() == target_item_depth - 1 {
            let existing_item = None;
            let parent_dir_item = path_items.last().unwrap();

            Self::validate_items_as_parent_dir(&parent_dir_item)?;
            Ok((parent_dir_item, existing_item))
        } else {
            Err(MetadataDBError::ViolatesDBConsistency {
                message: "Must not insert data_item without existing parent item (i.e. no file without a parent folder)!"
            })
        }
    }

    /// Makes sure that the given item is a valid parent item.
    /// This means, it must be a folder. All other cases will be rejected with a fitting error
    /// message. While this can be written shorter, the 'nice' error message will help us debugging.
    fn validate_items_as_parent_dir(parent_dir_item: &DBItemInternal) -> Result<()> {
        if parent_dir_item.item.file_type == FileType::FILE {
            return Err(MetadataDBError::ViolatesDBConsistency {
                message: "Must not try to modify a local item that has a file as a parent!",
            });
        }
        if parent_dir_item.item.file_type == FileType::DELETED {
            return Err(MetadataDBError::ViolatesDBConsistency {
                message: "Must not try to modify a local item that has a deleted parent folder!",
            });
        }
        if parent_dir_item.item.file_type == FileType::IGNORED {
            return Err(MetadataDBError::ViolatesDBConsistency {
                message: "Must not try to modify a local item that has an ignored parent folder!",
            });
        }

        Ok(())
    }

    /// Loads the remaining metadata for the given DB item (metadata entries and sync/mod times).
    /// Returns the complete internal item.
    fn load_item(
        &self,
        path_component: PathComponent,
        item: Item,
        fs_metadata: Option<FileSystemMetadata>,
        mod_metadata: Option<ModMetadata>,
        parent_sync_time: &VersionVector<i64>,
    ) -> Result<DBItemInternal> {
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
        if data_item.item.file_type == FileType::FILE
            || data_item.item.file_type == FileType::DELETED
        {
            // Skip the loading, makes only sense for items that actually have entries.
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
    fn ensure_path_exists(
        &self,
        name: &str,
        parent: Option<&PathComponent>,
    ) -> Result<PathComponent> {
        let current_path_string = if let Some(parent) = parent {
            format!("{}{}/", parent.full_path, name.to_lowercase())
        } else {
            assert_eq!(name, "", "Must not insert non empty root item!");
            "/".to_string()
        };

        let existing_path = path_components::table
            .filter(path_components::full_path.eq(&current_path_string))
            .first::<PathComponent>(&self.conn)
            .optional()?;
        if let Some(existing_path) = existing_path {
            return Ok(existing_path);
        }

        diesel::insert_into(path_components::table)
            .values((
                path_components::parent_id.eq(parent.map(|parent| parent.id)),
                path_components::full_path.eq(&current_path_string),
            ))
            .execute(&self.conn)?;

        let new_path = path_components::table
            .filter(path_components::full_path.eq(&current_path_string))
            .first::<PathComponent>(&self.conn)?;
        Ok(new_path)
    }

    /// Deletes the metadata entries associated with this DB item (removes the fs and mod metadata).
    fn delete_item_metadata(&self, db_item: &DBItemInternal) -> Result<()> {
        // No need for modification times of deleted items.
        diesel::delete(mod_metadatas::table.filter(mod_metadatas::id.eq(db_item.item.id)))
            .execute(&self.conn)?;
        // No need for metadata of deleted items.
        diesel::delete(
            file_system_metadatas::table.filter(file_system_metadatas::id.eq(db_item.item.id)),
        )
        .execute(&self.conn)?;

        Ok(())
    }

    /// Deletes all child DB entries of the given item.
    /// If passed delete_given_item == true: Also deletes the given item from the DB.
    /// If passed delete_given_item == false: Only deletes the child items from the DB.
    fn delete_child_db_entries(&self, parent_item: &DBItemInternal) -> Result<()> {
        let path_string = &parent_item.path_component.full_path;
        let db_path_components = path_components::table
            .filter(path_components::full_path.like(format!("{}%", path_string)))
            .filter(path_components::id.ne(parent_item.path_component.id))
            .select(path_components::id);

        diesel::delete(
            items::table
                .filter(items::data_store_id.eq(parent_item.item.data_store_id))
                .filter(items::path_component_id.eq_any(db_path_components)),
        )
        .execute(&self.conn)?;

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
        path_items: &Vec<DBItemInternal>,
        modifying_data_store_id: i64,
        modification_time: i64,
    ) -> Result<()> {
        let changes =
            diesel::update(mod_metadatas::table.find(path_items.last().as_ref().unwrap().item.id))
                .set(mod_metadata::UpdateLastMod {
                    last_mod_store_id: modifying_data_store_id,
                    last_mod_store_time: modification_time,
                })
                .execute(&self.conn)?;
        assert_eq!(
            changes, 1,
            "Must not add modification event for non existing mod_metadata!"
        );

        for path_item in path_items.iter().rev() {
            if path_item.item.file_type == FileType::DIRECTORY
                || path_item.item.file_type == FileType::IGNORED
            {
                let current_mod_time = mod_times::table
                    .select(mod_times::time)
                    .filter(
                        mod_times::mod_metadata_id.eq(path_item.mod_metadata.as_ref().unwrap().id),
                    )
                    .filter(mod_times::data_store_id.eq(modifying_data_store_id))
                    .first::<i64>(&self.conn)
                    .optional()?;
                if let Some(current_mod_time) = current_mod_time {
                    diesel::update(
                        mod_times::table
                            .filter(mod_times::data_store_id.eq(modifying_data_store_id))
                            .filter(
                                mod_times::mod_metadata_id.eq(path_item
                                    .mod_metadata
                                    .as_ref()
                                    .unwrap()
                                    .id),
                            ),
                    )
                    .set(mod_times::time.eq(max(current_mod_time, modification_time)))
                    .execute(&self.conn)?;
                } else {
                    diesel::insert_into(mod_times::table)
                        .values(mod_time::InsertFull {
                            mod_metadata_id: path_item.mod_metadata.as_ref().unwrap().id,
                            data_store_id: modifying_data_store_id,
                            time: modification_time,
                        })
                        .execute(&self.conn)?;
                }
            }
        }

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

    /// Queries the DB for DBItems that hold 'significant sync times'.
    /// A sync time is significant, if it has entries stored in the DB.
    /// After running `clean_up_local_sync_times` this function should only return
    /// DBItems that have changes in their sync time compared to their parent.
    pub fn find_local_significant_sync_times(
        &self,
    ) -> Result<Vec<(RelativePath, VersionVector<i64>)>> {
        #[derive(QueryableByName)]
        #[table_name = "path_components"]
        struct PathResult {
            full_path: String,
        }
        self.conn.transaction(|| {
            let significant_paths = diesel::sql_query("SELECT path_components.full_path FROM items, path_components WHERE ((SELECT COUNT(*) FROM sync_times WHERE sync_times.item_id = items.id) > 0 OR full_path = '/') AND path_components.id = items.path_component_id").load::<PathResult>(&self.conn)?;
            significant_paths
                .into_iter()
                .map(|item| RelativePath::from_path(&item.full_path[1..]))
                .map(|path| {
                    let db_item =  self.get_local_data_item(&path, true)?;
                    Ok((db_item.path, db_item.sync_time))
                })
                .collect()
        })
    }

    /// Enters the significant sync times of another data store into the local DB.
    /// Once entered, `find_sync_time` will return the correct sync time for the
    /// data store on subsequent calls respecting the signficant sync times.
    ///
    /// Essentially, this allows to transfer the knowledge of the synchronization status
    /// of other data stores into the local data store. This is the key piece of information
    /// needed to implement 'carrying' of data on devices like a laptop.
    pub fn enter_significant_sync_times_for(
        &self,
        data_store: &DataStore,
        entries: Vec<(RelativePath, VersionVector<i64>)>,
    ) -> Result<()> {
        self.conn.transaction(|| {
            assert_ne!(
                data_store.id,
                self.get_local_data_store()?.id,
                "Must not enter significant sync times for the local data store! This information is only valid for external stores."
            );
            // Delete existing entries...
            diesel::delete(items::table.filter(items::data_store_id.eq(data_store.id))).execute(&self.conn)?;
            // ...overwrite with given entries
            for (path, sync_time) in entries {
                // This search for the correct path_component is not very efficient.
                // But it will probably do for now, as we expect very few significant items.
                let mut current_path = self.ensure_path_exists("", None)?;
                for path_component in path.get_path_components().iter().skip(1) {
                    current_path = self.ensure_path_exists(path_component, Some(&current_path))?;
                }

                diesel::insert_into(items::table)
                    .values(item::InsertFull {
                        data_store_id: data_store.id,
                        path_component_id: current_path.id,
                        file_type: FileType::IGNORED
                    }).execute(&self.conn)?;
                let item = items::table
                    .filter(items::data_store_id.eq(data_store.id))
                    .filter(items::path_component_id.eq(current_path.id))
                    .first::<Item>(&self.conn)?;
                for (store_id, time) in sync_time.iter() {
                    diesel::insert_into(sync_times::table)
                        .values(sync_time::InsertFull {
                            item_id: item.id,
                            data_store_id: *store_id,
                            time: *time
                        }).execute(&self.conn)?;
                }
            }
            Ok(())
        })
    }

    /// Queries the sync time of a given item for the given data store.
    pub fn find_sync_time(
        &self,
        data_store: &DataStore,
        path: &RelativePath,
    ) -> Result<VersionVector<i64>> {
        let mut path_items = self
            .load_data_items_on_path(&data_store, &path, true)
            .unwrap();
        Ok(path_items.pop().unwrap().sync_time.unwrap())
    }

    fn clean_up_deleted_items(&self) -> Result<()> {
        // file_type = 3 is all deletions, the select after that selects only deletions with no
        // sync time entries, i.e. it deletes all 'implicit' deletions.
        diesel::sql_query("DELETE FROM items WHERE items.file_type = 3 AND (SELECT COUNT(*) FROM sync_times WHERE sync_times.item_id = items.id) = 0").execute(&self.conn)?;
        Ok(())
    }

    #[cfg(test)]
    fn count_items_in_db(&self) -> Result<i64> {
        let item_count = items::table
            .select(diesel::dsl::count(items::id))
            .first(&self.conn)?;
        Ok(item_count)
    }

    fn clean_up_path_components(&self) -> Result<()> {
        // delete all path_components that have no item using them in the DB.
        diesel::sql_query("DELETE FROM path_components WHERE (SELECT COUNT(*) FROM items WHERE items.path_component_id = path_components.id) = 0").execute(&self.conn)?;
        Ok(())
    }

    #[cfg(test)]
    fn count_path_components_in_db(&self) -> Result<i64> {
        let path_count = path_components::table
            .select(diesel::dsl::count(path_components::id))
            .first(&self.conn)?;
        Ok(path_count)
    }

    fn clean_up_local_sync_times(&self) -> Result<usize> {
        self.run_transaction(|| {
            let local_data_store = self.get_local_data_store()?;
            let root_item = self
                .load_data_items_on_path(&local_data_store, &RelativePath::from_path(""), true)?
                .pop()
                .unwrap();

            self.clean_up_sync_times(&root_item)
        })
    }

    fn clean_up_sync_times(&self, parent_item: &DBItemInternal) -> Result<usize> {
        let mut cleaned_up_items = 0;

        let parent_sync_time = parent_item.sync_time.as_ref().unwrap();
        for child_item in self.load_child_items(parent_item, true)? {
            let mut times_to_keep = Vec::new();
            for child_sync_entry in child_item.sync_time.as_ref().unwrap().iter() {
                let (data_store_id, time) = child_sync_entry;
                if parent_sync_time[&data_store_id] < *time {
                    // We found a sync time component that this item must change, i.e.
                    // it has a bigger sync time component than its parent folder.
                    times_to_keep.push(*data_store_id);
                }
            }

            // Clean up the item itself, i.e. remove non-keeper entries.
            let target_db_rows = sync_times::table
                .filter(sync_times::item_id.eq(child_item.item.id))
                .filter(diesel::dsl::not(
                    sync_times::data_store_id.eq_any(times_to_keep),
                ));
            let deleted_entries = diesel::delete(target_db_rows).execute(&self.conn)?;
            cleaned_up_items += deleted_entries;

            // Clean up the items chlid items.
            cleaned_up_items += self.clean_up_sync_times(&child_item)?;
        }

        Ok(cleaned_up_items)
    }

    /// Inserts a root item for the given data store.
    /// This includes associated metadata entries.
    fn create_root_item(&self, data_store: &DataStore) -> Result<()> {
        let root_path = self.ensure_path_exists("", None)?;

        diesel::insert_into(items::table)
            .values(item::InsertFull {
                data_store_id: data_store.id,
                path_component_id: root_path.id,

                file_type: FileType::DIRECTORY,
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

                is_read_only: false,
            })
            .execute(&self.conn)?;

        Ok(())
    }

    /// Inserts the default 'include all glob' to the inclusion rules.
    fn create_include_all_glob(&self, data_store: &DataStore) -> Result<()> {
        self.set_inclusion_rules(
            &data_store,
            &vec![DBInclusionRule {
                include: true,
                rule: glob::Pattern::new("**").unwrap(),
            }],
        )?;
        Ok(())
    }

    /// Upgrades the DB to the most recent schema version.
    fn upgrade_db(&self) -> db_migration::Result<()> {
        self.conn
            .transaction(|| db_migration::upgrade_db(&self.conn))?;

        Ok(())
    }

    /// Notes that we did some updating operation, re-optimize the DB from time to time.
    fn notify_change_for_optimization(&self) -> Result<()> {
        let mut updates = self.updates_since_optimization.borrow_mut();
        *updates += 1;

        if *updates >= UPDATES_UNTIL_OPTIMIZATION {
            *updates = 0;
            sql_query("ANALYZE").execute(&self.conn)?;
        }

        Ok(())
    }

    /// Changes the connection DB settings to our default usage pattern.
    fn default_db_settings(&self) -> Result<()> {
        sql_query("PRAGMA locking_mode = EXCLUSIVE").execute(&self.conn)?;
        sql_query("PRAGMA journal_mode = WAL").execute(&self.conn)?;
        sql_query("PRAGMA foreign_keys = 1").execute(&self.conn)?;

        // Set 'about' 512MB limit for RAM used to cache
        sql_query("PRAGMA cache_size = -512000").execute(&self.conn)?;
        sql_query("PRAGMA mmap_size = 536870912").execute(&self.conn)?;

        Ok(())
    }
}

#[cfg(test)]
pub mod tests;
