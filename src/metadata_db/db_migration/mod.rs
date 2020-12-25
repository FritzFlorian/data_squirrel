/// Module performing database migrations to newer application/database format versions.
/// Used on an existing DB connection to upgrade it to the most recent version.
///
/// upgrade_db(&connection); // upgrades to latest DB version
mod version_001;
mod version_002;

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sqlite::SqliteConnection;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum MigrationError {
    ReadWriteDBVersion { source: diesel::result::Error },
    UnknownDBVersion { version: DBVersion },
    SQLError { source: diesel::result::Error },
}
pub type Result<T> = std::result::Result<T, MigrationError>;

pub type DBVersion = i32;
const REQUIRED_DB_VERSION: DBVersion = 2;

/// Upgrades the given database connection to the REQUIRED_DB_VERSION of the
/// current application build.
///
/// As the application and therefore the database schema evolves, this routine is
/// used to step-by-step keep database files up to date with the application.
///
/// MUST be run before any other action on the database to make sure it's compatible.
pub fn upgrade_db(conn: &SqliteConnection) -> Result<DBVersion> {
    loop {
        let current_version = read_db_version(&conn)?;
        if current_version < REQUIRED_DB_VERSION {
            migrate_up_from(conn, current_version)?;
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
fn migrate_up_from(conn: &SqliteConnection, version: DBVersion) -> Result<()> {
    match version {
        // Just run the know migration steps as a regular functions.
        0 => version_001::migrate(&conn)?,
        1 => version_002::migrate(&conn)?,
        // We do not know how to handle this migration.
        _ => return Err(MigrationError::UnknownDBVersion { version }),
    };

    write_db_version(&conn, version + 1)?;
    Ok(())
}

fn read_db_version(conn: &SqliteConnection) -> Result<DBVersion> {
    use diesel::sql_types::Integer;
    #[derive(Debug, QueryableByName)]
    struct Test {
        #[sql_type = "Integer"]
        user_version: DBVersion,
    }
    let version: Vec<Test> = sql_query("PRAGMA user_version")
        .load(conn)
        .map_err(|source| MigrationError::ReadWriteDBVersion { source })?;

    Ok(version.get(0).unwrap().user_version)
}

fn write_db_version(conn: &SqliteConnection, version: DBVersion) -> Result<()> {
    sql_query(format!("PRAGMA user_version = {:}", version))
        .execute(conn)
        .map_err(|source| MigrationError::ReadWriteDBVersion { source })?;

    Ok(())
}

// Error Boilerplate (Error display, conversion and source)
impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error During Database Migration ({:?})", self)
    }
}
impl From<diesel::result::Error> for MigrationError {
    fn from(error: diesel::result::Error) -> Self {
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

    fn open_connection() -> SqliteConnection {
        SqliteConnection::establish(":memory:").unwrap()
    }

    fn query_table_names(conn: &SqliteConnection) -> Vec<String> {
        use diesel::sql_types::Text;
        #[derive(Debug, QueryableByName)]
        struct Test {
            #[sql_type = "Text"]
            name: String,
        }

        let result: Vec<Test> = sql_query("SELECT name FROM sqlite_master")
            .load(conn)
            .unwrap();
        result.iter().map(|test| test.name.clone()).collect()
    }

    #[test]
    fn read_and_write_db_version() {
        let conn = open_connection();

        assert_eq!(read_db_version(&conn).unwrap(), 0);
        write_db_version(&conn, 42).unwrap();
        assert_eq!(read_db_version(&conn).unwrap(), 42);
    }

    #[test]
    fn properly_upgrade_to_version_1() {
        let conn = open_connection();

        assert_eq!(read_db_version(&conn).unwrap(), 0);

        migrate_up_from(&conn, 0).unwrap();

        let table_names = query_table_names(&conn);
        assert!(table_names.contains(&"data_sets".to_string()));
        assert!(table_names.contains(&"data_stores".to_string()));
        assert!(table_names.contains(&"data_items".to_string()));
        assert!(table_names.contains(&"metadatas".to_string()));
        assert!(table_names.contains(&"owner_informations".to_string()));
        assert!(table_names.contains(&"mod_times".to_string()));
        assert!(table_names.contains(&"sync_times".to_string()));

        assert_eq!(read_db_version(&conn).unwrap(), 1);
    }

    #[test]
    fn properly_upgrade_to_version_2() {
        let conn = open_connection();

        assert_eq!(read_db_version(&conn).unwrap(), 0);

        migrate_up_from(&conn, 0).unwrap();
        migrate_up_from(&conn, 1).unwrap();

        assert_eq!(read_db_version(&conn).unwrap(), 2);
    }

    #[test]
    fn properly_upgrade_to_required_version() {
        let conn = open_connection();

        upgrade_db(&conn).unwrap();
        assert_eq!(read_db_version(&conn).unwrap(), REQUIRED_DB_VERSION);
    }
}
