mod db_migration;
// CRUD operations/basic entity mappings on database tables
mod data_set;
pub use self::data_set::DataSet;
mod data_store;
pub use self::data_store::DataStore;

use std::error::Error;
use std::fmt;

use rusqlite;

#[derive(Debug)]
pub enum MetadataDBError {
    DBMigrationError {
        source: db_migration::MigrationError,
    },
    GenericSQLError {
        source: rusqlite::Error,
    },
    NotFound,
    ViolatesDBConsistency {
        message: &'static str,
    },
}
pub type Result<T> = std::result::Result<T, MetadataDBError>;

pub struct MetadataDB {
    connection: rusqlite::Connection,
}

impl MetadataDB {
    pub fn open(path: &str) -> Result<MetadataDB> {
        let mut result = MetadataDB {
            connection: rusqlite::Connection::open(path)?,
        };

        result.default_db_settings()?;
        result.upgrade_db()?;

        Ok(result)
    }

    pub fn create_data_set(&mut self, unique_name: &str) -> Result<DataSet> {
        let transaction = self.connection.transaction()?;

        // Make sure we only hold ONE data_set instance in our database for now.
        if DataSet::get(&transaction)?.is_some() {
            return Err(MetadataDBError::ViolatesDBConsistency {
                message: "The database may only hold exactly ONE data_store!",
            });
        }

        DataSet::create(&transaction, unique_name)?;

        let data_set = DataSet::get(&transaction)?.unwrap();

        transaction.commit()?;
        Ok(data_set)
    }

    pub fn get_data_set(&self) -> Result<DataSet> {
        if let Some(result) = DataSet::get(&self.connection)? {
            Ok(result)
        } else {
            Err(MetadataDBError::NotFound)
        }
    }

    pub fn update_data_set_name(&self, human_name: &str) -> Result<()> {
        let mut query = self.connection.prepare(
            "
            UPDATE data_set
            SET human_name = ?
        ",
        )?;
        query.execute(rusqlite::params![&human_name])?;

        Ok(())
    }

    pub fn get_data_stores(&self) -> Result<Vec<DataStore>> {
        let data_set = self.get_data_set()?;
        Ok(DataStore::get_all(&self.connection, &data_set, None)?)
    }

    pub fn create_data_store(
        &mut self,
        unique_name: &str,
        path: &str,
        is_this_store: bool,
    ) -> Result<DataStore> {
        let data_set = self.get_data_set()?;

        let transaction = self.connection.transaction()?;
        if DataStore::get(&transaction, &data_set, &unique_name)?.is_some() {
            return Err(MetadataDBError::ViolatesDBConsistency {
                message: "A data_store with the same unique_name exists!",
            });
        }

        Ok(DataStore::create(
            &transaction,
            &data_set,
            &unique_name,
            "",
            chrono::Utc::now().naive_local(),
            &path,
            "",
            is_this_store,
            0,
        )?)
    }

    fn upgrade_db(&mut self) -> db_migration::Result<()> {
        let transaction = self.connection.transaction()?;
        db_migration::upgrade_db(&transaction)?;
        transaction.commit()?;

        Ok(())
    }

    fn default_db_settings(&self) -> rusqlite::Result<()> {
        self.connection
            .pragma_update(None, "locking_mode", &"exclusive".to_string())?;
        self.connection.pragma_update(None, "foreign_keys", &1)?;

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
impl From<rusqlite::Error> for MetadataDBError {
    fn from(error: rusqlite::Error) -> Self {
        Self::GenericSQLError { source: error }
    }
}
impl Error for MetadataDBError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DBMigrationError { ref source } => Some(source),
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
        let mut metadata_store = open_metadata_store();

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
        let mut metadata_store = open_metadata_store();

        metadata_store.create_data_set("abc").unwrap();
        match metadata_store.create_data_set("xyz") {
            Err(MetadataDBError::ViolatesDBConsistency { .. }) => (),
            _ => panic!("Must not have more than one data_set in DB!"),
        }
    }
}
