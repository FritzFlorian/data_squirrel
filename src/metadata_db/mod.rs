mod db_migration;
// Database schema - must be kept up to date manually
mod schema;
// Basic entity mappings on database tables (Should be mostly 1:1 copies of our schema and helpers).
pub mod data_item;
pub use self::data_item::DataItem;
pub mod data_set;
pub use self::data_set::DataSet;
pub mod data_store;
pub use self::data_store::DataStore;
pub mod metadata;
pub use self::metadata::Metadata;
pub mod owner_information;
pub use self::owner_information::OwnerInformation;

use std::error::Error;
use std::fmt;

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sqlite::SqliteConnection;

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
    pub fn open(path: &str) -> Result<MetadataDB> {
        let result = MetadataDB {
            conn: SqliteConnection::establish(path)?,
        };

        result.default_db_settings()?;
        result.upgrade_db()?;

        Ok(result)
    }

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

    pub fn get_data_set(&self) -> Result<DataSet> {
        use self::schema::data_sets::dsl::*;

        Ok(data_sets.first::<DataSet>(&self.conn)?)
    }

    pub fn update_data_set_name(&self, human_name_p: &str) -> Result<()> {
        use self::schema::data_sets::dsl::*;

        diesel::update(data_sets)
            .set(human_name.eq(human_name_p))
            .execute(&self.conn)?;

        Ok(())
    }

    pub fn get_data_stores(&self) -> Result<Vec<DataStore>> {
        use self::schema::data_stores::dsl::*;
        // We currently only allow EXACTLY ONE data_set, thus we do not need to join here.
        let result = data_stores.load(&self.conn)?;
        Ok(result)
    }

    pub fn create_data_store(&self, new_store: &DataStore) -> Result<DataStore> {
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

    pub fn get_this_data_store(&self) -> Result<DataStore> {
        use self::schema::data_stores::dsl::*;

        Ok(data_stores
            .filter(is_this_store.eq(true))
            .first::<DataStore>(&self.conn)?)
    }

    pub fn get_data_item(
        &self,
        for_data_store: &DataStore,
        path: &str,
    ) -> Result<Option<(DataItem, Metadata)>> {
        use self::schema::data_items;
        use self::schema::metadatas;
        use self::schema::owner_informations;

        let join =
            data_items::table.inner_join(owner_informations::table.inner_join(metadatas::table));
        let filtered = join
            .filter(data_items::path.like(path))
            .filter(owner_informations::data_store_id.eq(for_data_store.id));

        let result = filtered
            .first::<(DataItem, (OwnerInformation, Metadata))>(&self.conn)
            .optional()?;
        if let Some((item, (_owner, meta))) = result {
            Ok(Some((item, meta)))
        } else {
            Ok(None)
        }
    }

    pub fn create_local_data_item(&self, path: &str) -> Result<(DataItem, Metadata)> {
        let local_data_store = self.get_this_data_store()?;

        // TODO: Insert new data_item.
        // TODO: Set new data_item's mod time (read local DB version and bump it).
        // TODO: Update chain of parent data items (mod times set to MAX with new mod time).
        // TODO: Insert metadata item.

        Ok(self.get_data_item(&local_data_store, &path)?.unwrap())
    }

    pub fn modify_local_data_item(&self) -> Result<()> {
        let _local_data_store = self.get_this_data_store()?;

        // TODO: Find data_item.
        // TODO: Set data_item's mod time (read local DB version and bump it).
        // TODO: Update chain of parent data items (mod times set to MAX with new mod time).
        // TODO: Update data_item's metadata entry.

        Ok(())
    }

    fn upgrade_db(&self) -> db_migration::Result<()> {
        self.conn
            .transaction(|| db_migration::upgrade_db(&self.conn))?;

        Ok(())
    }

    fn default_db_settings(&self) -> Result<()> {
        sql_query("PRAGMA locking_mode = exclusive").execute(&self.conn)?;
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

    fn open_metadata_store() -> MetadataDB {
        MetadataDB::open(":memory:").unwrap()
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
}
