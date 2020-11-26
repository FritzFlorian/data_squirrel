/// Module performing database migrations to newer application/database format versions.
/// Used on an existing DB connection to upgrade it to the most recent version.
///
/// upgrade_db(&connection); // upgrades to latest DB version
mod version_001;

use rusqlite;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum MigrationError {
    ReadWriteDBVersion { source: rusqlite::Error },
    UnknownDBVersion { version: DBVersion },
    SQLError { source: rusqlite::Error },
}
pub type Result<T> = std::result::Result<T, MigrationError>;

pub type DBVersion = u32;
const REQUIRED_DB_VERSION: DBVersion = 1;
const PRAGMA_USER_VERSION: &str = "user_version";

/// Upgrades the given database connection to the REQUIRED_DB_VERSION of the
/// current application build.
///
/// As the application and therefore the database schema evolves, this routine is
/// used to step-by-step keep database files up to date with the application.
///
/// MUST be run before any other action on the database to make sure it's compatible.
pub fn upgrade_db(connection: &rusqlite::Connection) -> Result<DBVersion> {
    loop {
        let current_version = read_db_version(&connection)?;
        if current_version < REQUIRED_DB_VERSION {
            migrate_up_from(connection, current_version)?;
        } else {
            return Ok(current_version);
        }
    }
}

/// Migrates the given database connection from the DBVersion version to (version + 1).
/// Expects the database to be in the given version and updates the user_version pragma
/// to the new (version + 1) value if successful.
///
/// Does not wrap the operation in a transaction,
/// the caller is supposed to if a rollback might be required.
fn migrate_up_from(connection: &rusqlite::Connection, version: DBVersion) -> Result<()> {
    match version {
        // Just run the know migration steps as a regular functions.
        0 => version_001::migrate(&connection)?,
        // We do not know how to handle this migration.
        _ => return Err(MigrationError::UnknownDBVersion { version }),
    };

    write_db_version(&connection, version + 1)?;
    Ok(())
}

fn read_db_version(connection: &rusqlite::Connection) -> Result<DBVersion> {
    let version = connection
        .pragma_query_value(None, PRAGMA_USER_VERSION, |row| {
            let version: DBVersion = row.get(0)?;
            Ok(version)
        })
        .map_err(|source| MigrationError::ReadWriteDBVersion { source })?;

    Ok(version)
}

fn write_db_version(connection: &rusqlite::Connection, version: DBVersion) -> Result<()> {
    connection
        .pragma_update(None, PRAGMA_USER_VERSION, &version)
        .map_err(|source| MigrationError::ReadWriteDBVersion { source })?;

    Ok(())
}

// Error Boilerplate (Error display, conversion and source)
impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error During Database Migration ({:?})", self)
    }
}
impl From<rusqlite::Error> for MigrationError {
    fn from(error: rusqlite::Error) -> Self {
        Self::SQLError { source: error }
    }
}
impl Error for MigrationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ReadWriteDBVersion { ref source } => Some(source),
            Self::UnknownDBVersion { .. } => None,
            Self::SQLError { ref source } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_connection() -> rusqlite::Connection {
        rusqlite::Connection::open_in_memory().unwrap()
    }

    fn query_table_names(connection: &rusqlite::Connection) -> rusqlite::Result<Vec<String>> {
        let mut query = connection.prepare("SELECT name FROM sqlite_schema")?;
        let rows = query.query_map(rusqlite::params![], |row| {
            let name: String = row.get(0)?;
            Ok(name)
        })?;

        rows.collect()
    }

    #[test]
    fn read_and_write_db_version() {
        let connection = open_connection();

        assert_eq!(read_db_version(&connection).unwrap(), 0);
        write_db_version(&connection, 42).unwrap();
        assert_eq!(read_db_version(&connection).unwrap(), 42);
    }

    #[test]
    fn properly_upgrade_to_version_1() {
        let connection = open_connection();

        assert_eq!(read_db_version(&connection).unwrap(), 0);

        migrate_up_from(&connection, 0).unwrap();

        let table_names = query_table_names(&connection).unwrap();
        assert!(table_names.contains(&"data_set".to_string()));
        assert!(table_names.contains(&"data_store".to_string()));
        assert!(table_names.contains(&"data_item".to_string()));
        assert!(table_names.contains(&"item_metadata".to_string()));
        assert!(table_names.contains(&"owner_information".to_string()));
        assert!(table_names.contains(&"mod_time".to_string()));
        assert!(table_names.contains(&"sync_time".to_string()));

        assert_eq!(read_db_version(&connection).unwrap(), 1);
    }

    #[test]
    fn properly_upgrade_to_required_version() {
        let connection = open_connection();

        upgrade_db(&connection).unwrap();
        assert_eq!(read_db_version(&connection).unwrap(), REQUIRED_DB_VERSION);
    }
}
