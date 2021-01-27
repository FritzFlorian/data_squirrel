use super::*;

pub fn migrate(conn: &SqliteConnection) -> Result<()> {
    create_index_path_components(&conn)?;

    Ok(())
}

// Creates an index to search for data items based on their path.
// This is the main search we might do in our DB and thus worth speeding up.
fn create_index_path_components(conn: &SqliteConnection) -> Result<()> {
    sql_query("CREATE INDEX path_components_parent_idx ON path_components(parent_id)")
        .execute(conn)?;
    Ok(())
}
