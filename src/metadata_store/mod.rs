mod db_migration;

use metadata_store::db_migration::upgrade_db;
use rusqlite;

pub struct MetadataStore {
    _connection: rusqlite::Connection,
}

impl MetadataStore {
    // TODO: currently only 'outline' to shut down not used errors
    pub fn open(path: &str) -> MetadataStore {
        let connection = rusqlite::Connection::open(path).unwrap();
        upgrade_db(&connection).unwrap();
        MetadataStore {
            _connection: connection,
        }
    }
}
