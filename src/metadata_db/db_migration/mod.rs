/// Module performing database migrations to newer application/database format versions.
/// Used on an existing DB connection to upgrade it to the most recent version.
///
/// upgrade_db(&connection); // upgrades to latest DB version
mod version_001;
mod version_002;

mod errors;
pub use self::errors::*;

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sqlite::SqliteConnection;

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

#[cfg(test)]
mod tests;
