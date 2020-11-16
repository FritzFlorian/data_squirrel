mod db_migration;

use metadata_store::db_migration::upgrade_db;
use rusqlite;

pub struct MetadataStore {
    connection: rusqlite::Connection,
}

impl MetadataStore {
    // TODO: currently only 'outline' to shut down not used errors
    pub fn open(path: &str) -> MetadataStore {
        let result = MetadataStore {
            connection: rusqlite::Connection::open(path).unwrap(),
        };

        result.default_db_settings().unwrap();
        result.upgrade_db().unwrap();

        result
    }

    fn upgrade_db(&self) -> db_migration::MigrationResult<()> {
        upgrade_db(&self.connection)?;

        Ok(())
    }

    fn default_db_settings(&self) -> rusqlite::Result<()> {
        self.connection
            .pragma_update(None, "locking_mode", &"exclusive".to_string())?;
        self.connection.pragma_update(None, "foreign_keys", &1)?;

        Ok(())
    }
}
