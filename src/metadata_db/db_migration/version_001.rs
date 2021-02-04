use super::*;

pub fn migrate(conn: &SqliteConnection) -> Result<()> {
    create_table_data_sets(&conn)?;
    create_table_data_stores(&conn)?;

    create_table_path_components(&conn)?;
    create_table_item(&conn)?;

    create_table_file_system_metadatas(&conn)?;
    create_table_mod_metadatas(&conn)?;

    create_table_mod_times(&conn)?;
    create_table_sync_times(&conn)?;

    Ok(())
}

// A data_set is a unique identifier for a data set being synchronized.
// There can be multiple physical copies of one logical data_set,
// all kept in sync by the software.
fn create_table_data_sets(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE data_sets(
                id              INTEGER PRIMARY KEY NOT NULL,
                unique_name     TEXT NOT NULL UNIQUE,
                human_name      TEXT NOT NULL DEFAULT ''
             )",
    )
    .execute(conn)?;

    Ok(())
}

// A data_store is a physical copy of a dataset. It lives on a users storage
// device in form of a folder that is kept in sync with other device's folders.
fn create_table_data_stores(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE data_stores(
                id                  INTEGER PRIMARY KEY NOT NULL,
                data_set_id         INTEGER NOT NULL,

                unique_name         TEXT NOT NULL,
                human_name          TEXT NOT NULL DEFAULT '',
                creation_date       TEXT NOT NULL,
                path_on_device      TEXT NOT NULL,
                location_note       TEXT NOT NULL DEFAULT '',

                is_this_store       INTEGER NOT NULL,
                time                INTEGER NOT NULL,

                UNIQUE(unique_name),
                FOREIGN KEY(data_set_id)    REFERENCES data_sets(id)
             )",
    )
    .execute(conn)?;

    Ok(())
}

// An individual path_component is an item of a folder/directory structure.
// Path_components form a tree like structure by defining their parent_item_id.
fn create_table_path_components(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE path_components(
                id                  INTEGER PRIMARY KEY NOT NULL UNIQUE,  
                parent_id           INTEGER,
                full_path           TEXT NOT NULL UNIQUE,

                FOREIGN KEY(parent_id) REFERENCES path_components(id)
            )",
    )
    .execute(conn)?;

    Ok(())
}

// An item represents actual knowledge of what is stored on the filesystem.
// Is associated a raw 'path' (in form of a path_component, which is part of a directory tree)
// with metadata required to perform synchronization.
// For the 'classic' vector pair algorithm we only need item's for our local data_store,
// however, we allow to also keep information about other data_store's items, to e.g. allow
// synchronization to non-reachable targets later on.
fn create_table_item(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE items(
                id                  INTEGER PRIMARY KEY NOT NULL,

                data_store_id       INTEGER NOT NULL,
                path_component_id   INTEGER NOT NULL,

                file_type INTEGER NOT NULL,

                UNIQUE(path_component_id, data_store_id),
                FOREIGN KEY(data_store_id)      REFERENCES data_stores(id),
                FOREIGN KEY(path_component_id)  REFERENCES path_components(id)
            )",
    )
    .execute(conn)?;

    Ok(())
}

// File system related metadata associated to a data item.
fn create_table_file_system_metadatas(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE file_system_metadatas(
                id                      INTEGER PRIMARY KEY NOT NULL,
                
                case_sensitive_name     TEXT NOT NULL,
                creation_time           TEXT NOT NULL,
                mod_time                TEXT NOT NULL,
                hash                    TEXT NOT NULL,

                is_read_only            INTEGER NOT NULL, 
    
                FOREIGN KEY(id)   REFERENCES items(id)   ON DELETE CASCADE
            )",
    )
    .execute(conn)?;

    Ok(())
}

// Metadata related to the modification of a data item.
fn create_table_mod_metadatas(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE mod_metadatas(
                id                      INTEGER PRIMARY KEY NOT NULL,
                
                creator_store_id        INTEGER NOT NULL,
                creator_store_time      INTEGER NOT NULL,

                last_mod_store_id       INTEGER NOT NULL,
                last_mod_store_time     INTEGER NOT NULL,
    
                FOREIGN KEY(id)   REFERENCES items(id)   ON DELETE CASCADE
            )",
    )
    .execute(conn)?;

    Ok(())
}

// Stores a modification time of a file from the view of a specific owner,
// i.e. it encodes the information of the form:
// "data_item from the view of owner_information has modification time stamp
//  data_store -> time (the time the data_item was modified most recently by the data_store)"
fn create_table_mod_times(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE mod_times(
                id                  INTEGER PRIMARY KEY NOT NULL,
                
                mod_metadata_id     INTEGER NOT NULL,
                
                data_store_id       INTEGER NOT NULL,
                time                INTEGER NOT NULL,

                UNIQUE(mod_metadata_id, data_store_id),
                FOREIGN KEY(mod_metadata_id)   REFERENCES mod_metadatas(id)   ON DELETE CASCADE,
                FOREIGN KEY(data_store_id)     REFERENCES data_stores(id) 
            )",
    )
    .execute(conn)?;

    Ok(())
}

// Stores a synchronization time of a file from the view of a specific owner,
// i.e. it encodes the information of the form:
// "the item has the synchronization time stamp data_store -> time
//  (the time the data_item was synchronized most recently with the data_store)"
fn create_table_sync_times(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE sync_times(
                id                  INTEGER PRIMARY KEY NOT NULL,
                
                item_id             INTEGER NOT NULL,
                
                data_store_id       INTEGER NOT NULL,
                time                INTEGER NOT NULL,

                UNIQUE(item_id, data_store_id),
                FOREIGN KEY(item_id)            REFERENCES items(id)   ON DELETE CASCADE,
                FOREIGN KEY(data_store_id)      REFERENCES data_stores(id)
            )",
    )
    .execute(conn)?;

    Ok(())
}
