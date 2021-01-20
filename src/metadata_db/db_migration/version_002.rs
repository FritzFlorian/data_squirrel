use super::*;

pub fn migrate(conn: &SqliteConnection) -> Result<()> {
    create_index_path_components(&conn)?;
    create_index_item(&conn)?;
    create_index_mod_times_mod_metadata(&conn)?;
    create_index_sync_times_item(&conn)?;

    Ok(())
}

// Creates an index to search for data items based on their path.
// This is the main search we might do in our DB and thus worth speeding up.
fn create_index_path_components(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE UNIQUE INDEX path_components_idx ON path_components(parent_component_id, path_component)",
    )
    .execute(conn)?;
    Ok(())
}

// Creates an index to search for items (fast search by path_component_id and data_store_id).
fn create_index_item(conn: &SqliteConnection) -> Result<()> {
    sql_query("CREATE UNIQUE INDEX item_path_component ON items(path_component_id, data_store_id)")
        .execute(conn)?;
    Ok(())
}

// Allow for quick searches of mod time entries
fn create_index_mod_times_mod_metadata(conn: &SqliteConnection) -> Result<()> {
    sql_query("CREATE INDEX mod_times_mod_metadatas ON mod_times(mod_metadata_id)")
        .execute(conn)?;
    Ok(())
}

// Allow for quick searches of sync time entries
fn create_index_sync_times_item(conn: &SqliteConnection) -> Result<()> {
    sql_query("CREATE INDEX sync_times_item ON sync_times(item_id)").execute(conn)?;
    Ok(())
}
