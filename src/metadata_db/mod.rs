mod db_migration;
// Database schema - must be kept up to date manually
mod entity;
pub use self::entity::*;
mod item;
pub use self::item::Item;
mod schema;

use std::error::Error;
use std::fmt;

use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sqlite::SqliteConnection;
use std::path::Path;
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
        use self::schema::data_stores::dsl::*;
        use diesel::dsl::*;

        let result = self.conn.transaction(|| {
            // Check DB consistency
            let this_store_already_exists =
                select(exists(data_stores.filter(is_this_store.eq(true))))
                    .get_result(&self.conn)?;
            if this_store_already_exists {
                return Err(MetadataDBError::ViolatesDBConsistency {
                    message: "Must only have one data_store marked as local store!",
                });
            }

            // Insert new entry
            diesel::insert_into(data_stores)
                .values(new_store)
                .execute(&self.conn)?;

            let result = data_stores
                .filter(unique_name.eq(&new_store.unique_name))
                .first::<DataStore>(&self.conn)?;
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

    /// Queries a data item from the DB and returns it as well as its metadata information.
    /// As data items might not be present in a given data store, the method might return None.
    pub fn get_data_item(&self, for_data_store: &DataStore, path: &str) -> Result<Option<Item>> {
        use self::schema::data_items;
        use self::schema::metadatas;
        use self::schema::owner_informations;

        let join =
            data_items::table.inner_join(owner_informations::table.inner_join(metadatas::table));
        let filtered = join
            .filter(data_items::path.eq(path)) // ignores case because of table definition
            .filter(owner_informations::data_store_id.eq(for_data_store.id));

        let result = filtered
            .first::<(DataItem, (OwnerInformation, Metadata))>(&self.conn)
            .optional()?;
        if let Some((item, (owner, meta))) = result {
            Ok(Some(Item::from_join_tuple(item, owner, meta)))
        } else {
            Ok(None)
        }
    }

    // Queries all child items of a given DB item.
    pub fn get_child_data_items(&self, parent_item: &Item) -> Result<Vec<Item>> {
        use self::schema::data_items;
        use self::schema::metadatas;
        use self::schema::owner_informations;

        let join =
            data_items::table.inner_join(owner_informations::table.inner_join(metadatas::table));
        let filtered = join
            .filter(data_items::parent_item_id.eq(parent_item.data_item.id))
            .filter(owner_informations::data_store_id.eq(parent_item.owner_info.data_store_id));

        let result: Vec<_> = filtered
            .load::<(DataItem, (OwnerInformation, Metadata))>(&self.conn)?
            .into_iter()
            .map(|(item, (owner, meta))| Item::from_join_tuple(item, owner, meta))
            .collect();

        Ok(result)
    }

    /// Creates a new data item for the local data store (making sure versions stay consistent).
    /// The method implicitly assigns the appropriate creation information and time stamps.
    /// The method implicitly assigns an appropriate update time to the new item.
    pub fn create_local_data_item(
        &self,
        path: &Path,
        creation_time: chrono::NaiveDateTime,
        mod_time: chrono::NaiveDateTime,
        is_file: bool,
        hash: &str,
    ) -> Result<Item> {
        use self::schema::data_items;
        use self::schema::metadatas;
        use self::schema::owner_informations;

        let path_string = path.to_str().unwrap().to_string();
        let result = self.conn.transaction::<_, MetadataDBError, _>(|| {
            // We insert an item, bump the data stores version and mark all events with the version.
            let local_data_store = self.get_this_data_store()?;
            let new_time = local_data_store.time;
            self.increase_local_time()?;

            // Look if the data_item has a parent and make sure we associate them with each other.
            let parent_item: Option<DataItem> = if let Some(parent_path) = path.parent() {
                // Deeper nested sup-directory, e.g. sub/dir/nested/ has parent sub/dir/
                Some(
                    data_items::table
                        .filter(data_items::path.eq(parent_path.to_str().unwrap()))
                        .first::<DataItem>(&self.conn)?,
                )
            } else if path_string.is_empty() {
                // Root directory has no parent directory
                None
            } else {
                // Special case of a top level direcotry, as its parent is root and not apparent in
                // the path's representation (i.e. rust will tell us that sub/ has no parent).
                Some(
                    data_items::table
                        .filter(data_items::path.eq(""))
                        .first::<DataItem>(&self.conn)?,
                )
            };

            // Insert new data_item and associated owner information.
            diesel::replace_into(data_items::table)
                .values(data_item::InsertFull {
                    data_set_id: local_data_store.data_set_id,
                    parent_item_id: parent_item.map_or(None, |item| Some(item.id)),
                    path: &path_string,
                })
                .execute(&self.conn)?;
            let new_data_item = data_items::table
                .filter(data_items::path.eq(&path_string))
                .first::<DataItem>(&self.conn)?;

            diesel::replace_into(owner_informations::table)
                .values(owner_information::InsertFull {
                    data_item_id: new_data_item.id,
                    data_store_id: local_data_store.id,
                })
                .execute(&self.conn)?;
            let new_owner_info = owner_informations::table
                .filter(owner_informations::data_item_id.eq(new_data_item.id))
                .filter(owner_informations::data_store_id.eq(local_data_store.id))
                .first::<OwnerInformation>(&self.conn)?;

            // Also update the new item's modification time to match its creation time.
            // This gives the item a 'proper' modification event to be used in later comparisons.
            self.add_mod_event(&new_owner_info, &local_data_store, new_time)?;

            // Insert metadata item.
            diesel::insert_into(metadatas::table)
                .values(metadata::InsertFull {
                    owner_information_id: new_owner_info.id,

                    creator_store_id: local_data_store.id,
                    creator_store_time: new_time,

                    is_file: is_file,
                    creation_time: creation_time,
                    mod_time: mod_time,
                    hash: hash.to_string(),
                })
                .execute(&self.conn)?;

            Ok(self
                .get_data_item(&local_data_store, path.to_str().unwrap())?
                .unwrap())
        })?;

        Ok(result)
    }

    /// Notify the DB about an changed/modified data_item.
    /// This updates the specified metadata as well as bumping the modification time of the item.
    pub fn modify_local_data_item(
        &self,
        item: &Item,
        creation_time: &NaiveDateTime,
        mod_time: &NaiveDateTime,
        hash: &str,
    ) -> Result<()> {
        use self::schema::metadatas;

        self.conn.transaction::<_, MetadataDBError, _>(|| {
            // We insert an item, bump the data stores version and mark all events with the version.
            let local_data_store = self.get_this_data_store()?;
            let new_time = local_data_store.time;
            self.increase_local_time()?;

            self.add_mod_event(&item.owner_info, &local_data_store, new_time)?;

            diesel::update(metadatas::table.find(item.metadata.id))
                .set(metadata::UpdateMetadata {
                    creation_time: &creation_time,
                    mod_time: &mod_time,
                    hash: &hash,
                })
                .execute(&self.conn)?;

            Ok(())
        })?;

        Ok(())
    }

    /// Notify the DB about a deleted data_item.
    /// Only performs minimal work to enable a deletion, might require DB cleanup afterwards.
    pub fn delete_local_data_item(&self, item: &Item) -> Result<()> {
        use self::schema::metadatas;
        use self::schema::mod_times;

        self.conn.transaction::<_, MetadataDBError, _>(|| {
            // No need for modification times of deleted items.
            diesel::delete(mod_times::table)
                .filter(mod_times::owner_information_id.eq(item.owner_info.id))
                .execute(&self.conn)?;
            // No need for metadata of deleted items.
            diesel::delete(metadatas::table)
                .filter(metadatas::owner_information_id.eq(item.owner_info.id))
                .execute(&self.conn)?;

            // Keep the owner information itself and its sync time, as it will be required
            // to perform deletions when syncing between two data_stores.
            // NOTE: a db cleanup procedure will also delete this in case it is not needed
            //       anymore as the parent directory has the same sync times.

            Ok(())
        })?;

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
        owner_information: &OwnerInformation,
        modifying_data_store: &DataStore,
        modification_time: i64,
    ) -> Result<()> {
        let mut mod_vector = VersionVector::<i64>::new();
        mod_vector[&modifying_data_store.id] = modification_time;

        self.conn.transaction(|| {
            self.update_mod_times(&owner_information, &mod_vector)?;
            self.update_parent_mod_times(&owner_information)
        })?;

        Ok(())
    }

    /// Updates the modification times of an DB entry by replacing all
    /// given vector time entries (represented by their data_store id).
    ///
    /// NOTE: Does currently only override other existing entries, but never
    ///       deletes entries not mentioned in the given version vector.
    fn update_mod_times(
        &self,
        owner_information: &OwnerInformation,
        mod_times: &VersionVector<i64>,
    ) -> Result<()> {
        use self::schema::mod_times;

        let new_db_entries: Vec<_> = mod_times
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

    /// Queries the modification time vector for a given OwnerInformation
    /// (and thus indirectly for the associated data_item).
    ///
    /// Note: The modification time vector represents (local data_store_id -> time) pairs,
    ///       for  exchange with other data_stores it must be 'translated' to a vector version
    ///       where times are identified by ('unique-str' -> time) pairs.
    fn get_mod_times(&self, owner_information: &OwnerInformation) -> Result<VersionVector<i64>> {
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

    /// Updates the modification times of all the given owner_information's parent
    /// data_items to include the given time in their MAX(children) modification time.
    fn update_parent_mod_times(&self, owner_information: &OwnerInformation) -> Result<()> {
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
            self.update_mod_times(&parent_owner_information, &current_mod_vector)?;

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
        sql_query("PRAGMA locking_mode = exclusive").execute(&self.conn)?;
        sql_query("PRAGMA journal_mode = WAL").execute(&self.conn)?;
        sql_query("PRAGMA foreign_keys = 1").execute(&self.conn)?;

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
    use std::path::PathBuf;

    fn open_metadata_store() -> MetadataDB {
        MetadataDB::open(":memory:").unwrap()
    }

    fn insert_sample_data_set(metadata_store: &MetadataDB) -> (DataSet, DataStore) {
        let data_set = metadata_store.create_data_set("abc").unwrap();
        let data_store = metadata_store
            .create_data_store(&data_store::InsertFull {
                data_set_id: data_set.id,
                unique_name: &"abc",
                human_name: &"abc",
                is_this_store: true,
                time: 0,

                creation_date: &NaiveDateTime::from_timestamp(0, 0),
                path_on_device: &"/",
                location_note: &"",
            })
            .unwrap();

        (data_set, data_store)
    }

    fn insert_data_item(metadata_store: &MetadataDB, name: &str, is_file: bool) -> Item {
        metadata_store
            .create_local_data_item(
                &PathBuf::from(name),
                NaiveDateTime::from_timestamp(0, 0),
                NaiveDateTime::from_timestamp(0, 0),
                is_file,
                "",
            )
            .unwrap()
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
        let root = insert_data_item(&metadata_store, "", false);
        assert_eq!(
            metadata_store.get_mod_times(&root.owner_info).unwrap()[&data_store.id],
            0
        );
        let sub = insert_data_item(&metadata_store, "sub", false);
        assert_eq!(
            metadata_store.get_mod_times(&sub.owner_info).unwrap()[&data_store.id],
            1
        );
        let sub_folder = insert_data_item(&metadata_store, "sub/folder", false);
        assert_eq!(
            metadata_store
                .get_mod_times(&sub_folder.owner_info)
                .unwrap()[&data_store.id],
            2
        );
        let file = insert_data_item(&metadata_store, "sub/folder/file", true);
        assert_eq!(
            metadata_store.get_mod_times(&file.owner_info).unwrap()[&data_store.id],
            3
        );

        // Parent folders get updated correctly
        assert_eq!(
            metadata_store.get_mod_times(&root.owner_info).unwrap()[&data_store.id],
            3
        );
        assert_eq!(
            metadata_store.get_mod_times(&sub.owner_info).unwrap()[&data_store.id],
            3
        );
        assert_eq!(
            metadata_store
                .get_mod_times(&sub_folder.owner_info)
                .unwrap()[&data_store.id],
            3
        );

        // Re-query an entry and check if it is correct
        let file = metadata_store
            .get_data_item(&data_store, "sub/folder/file")
            .unwrap()
            .unwrap();
        assert_eq!(file.is_file(), true);
        assert_eq!(file.path(), "sub/folder/file");
        assert_eq!(file.data_item.parent_item_id, Some(sub_folder.data_item.id));
        assert_eq!(root.data_item.parent_item_id, None);

        // Check if child queries work
        let children = metadata_store.get_child_data_items(&root).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].data_item.path, "sub");

        // Delete items
        metadata_store.delete_local_data_item(&file).unwrap();
        metadata_store.delete_local_data_item(&sub_folder).unwrap();
        metadata_store.delete_local_data_item(&sub).unwrap();
        let children = metadata_store.get_child_data_items(&root).unwrap();
        assert_eq!(children.len(), 0);
    }
}
