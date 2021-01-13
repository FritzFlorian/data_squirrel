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
    assert!(table_names.contains(&"path_components".to_string()));
    assert!(table_names.contains(&"items".to_string()));
    assert!(table_names.contains(&"mod_metadatas".to_string()));
    assert!(table_names.contains(&"file_system_metadatas".to_string()));
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
