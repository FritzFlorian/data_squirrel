use super::*;

pub fn migrate(conn: &SqliteConnection) -> Result<()> {
    create_index_data_item_path(&conn)?;
    create_index_owner_information(&conn)?;
    create_index_mod_times_owner_information(&conn)?;
    create_index_sync_times_owner_information(&conn)?;

    Ok(())
}

// Creates an index to search for data items based on their path.
// This is the main search we might do in our DB and thus worth speeding up.
fn create_index_data_item_path(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE UNIQUE INDEX data_item_path_idx ON data_items(path_component, parent_item_id)",
    )
    .execute(conn)?;
    Ok(())
}

// Creates an index to search for owner informations (fast search by data_item_id and data_store_id).
fn create_index_owner_information(conn: &SqliteConnection) -> Result<()> {
    sql_query("CREATE UNIQUE INDEX owner_information_data_store ON owner_informations(data_item_id, data_store_id)").execute(conn)?;
    Ok(())
}

// Allow for quick searches of mod time entries
fn create_index_mod_times_owner_information(conn: &SqliteConnection) -> Result<()> {
    sql_query("CREATE INDEX mod_times_owner_information ON mod_times(owner_information_id)")
        .execute(conn)?;
    Ok(())
}

// Allow for quick searches of sync time entries
fn create_index_sync_times_owner_information(conn: &SqliteConnection) -> Result<()> {
    sql_query("CREATE INDEX sync_times_owner_information ON sync_times(owner_information_id)")
        .execute(conn)?;
    Ok(())
}
