mod db_migration;
// CRUD operations/basic entity mappings on database tables
mod data_set;
use self::data_set::DataSet;

use rusqlite;

#[derive(Debug)]
pub enum MetadataError {
    DBMigrationError(db_migration::MigrationError),
    GenericSQLError(rusqlite::Error),
    NotFound,
    ViolatesDBConsistency { message: &'static str },
}

impl From<db_migration::MigrationError> for MetadataError {
    fn from(error: db_migration::MigrationError) -> Self {
        Self::DBMigrationError(error)
    }
}
impl From<rusqlite::Error> for MetadataError {
    fn from(error: rusqlite::Error) -> Self {
        Self::GenericSQLError(error)
    }
}
pub type MetadataResult<T> = Result<T, MetadataError>;

pub struct MetadataStore {
    connection: rusqlite::Connection,
}

impl MetadataStore {
    pub fn open(path: &str) -> MetadataResult<MetadataStore> {
        let result = MetadataStore {
            connection: rusqlite::Connection::open(path)?,
        };

        result.default_db_settings()?;
        result.upgrade_db()?;

        Ok(result)
    }

    pub fn create_data_set(&mut self, unique_name: &str) -> MetadataResult<DataSet> {
        let transaction = self.connection.transaction()?;

        // Make sure we only hold ONE data_set instance in our database for now.
        if let Some(_) = DataSet::get(&transaction)? {
            return Err(MetadataError::ViolatesDBConsistency {
                message: "The database may only hold exactly ONE data_store!",
            });
        }

        DataSet::create(&transaction, unique_name)?;

        let data_set = DataSet::get(&transaction)?.unwrap();

        transaction.commit()?;
        Ok(data_set)
    }

    pub fn get_data_set(&self) -> MetadataResult<DataSet> {
        if let Some(result) = DataSet::get(&self.connection)? {
            Ok(result)
        } else {
            Err(MetadataError::NotFound)
        }
    }

    pub fn update_data_set(&self, data_set: &DataSet) -> MetadataResult<()> {
        data_set.update(&self.connection)?;

        Ok(())
    }

    fn upgrade_db(&self) -> db_migration::MigrationResult<()> {
        db_migration::upgrade_db(&self.connection)?;

        Ok(())
    }

    fn default_db_settings(&self) -> rusqlite::Result<()> {
        self.connection
            .pragma_update(None, "locking_mode", &"exclusive".to_string())?;
        self.connection.pragma_update(None, "foreign_keys", &1)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_metadata_store() -> MetadataStore {
        MetadataStore::open(":memory:").unwrap()
    }

    #[test]
    fn insert_and_query_data_set() {
        let mut metadata_store = open_metadata_store();

        assert!(metadata_store.get_data_set().is_err());

        metadata_store.create_data_set("abc").unwrap();
        let mut data_set = metadata_store.get_data_set().unwrap();
        assert_eq!(data_set.unique_name, "abc");
        assert_eq!(data_set.human_name, "");

        data_set.human_name = "testing".to_string();
        metadata_store.update_data_set(&data_set).unwrap();
        let data_set = metadata_store.get_data_set().unwrap();
        assert_eq!(data_set.unique_name, "abc");
        assert_eq!(data_set.human_name, "testing");
    }

    #[test]
    fn enforces_single_data_set() {
        let mut metadata_store = open_metadata_store();

        metadata_store.create_data_set("abc").unwrap();
        match metadata_store.create_data_set("xyz") {
            Err(MetadataError::ViolatesDBConsistency { .. }) => (),
            _ => panic!("Must not have more than one data_set in DB!"),
        }
    }
}
