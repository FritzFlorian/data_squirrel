use super::*;

pub fn migrate(conn: &SqliteConnection) -> Result<()> {
    create_table_data_sets(&conn)?;
    create_table_data_stores(&conn)?;
    create_table_data_items(&conn)?;
    create_table_item_metadatas(&conn)?;
    create_table_owner_informations(&conn)?;
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
                version             INTEGER NOT NULL,

                FOREIGN KEY(data_set_id)    REFERENCES data_set(id)
             )",
    )
    .execute(conn)?;

    Ok(())
}

// An individual data item is a folder or file contained in a data_set.
// Each data_item is uniquely identified by its initial, physical data_store it was
// created in (a pair of creator_store_id and creator_version time stamp).
//
// Data_items form a tree like structure by defining their parent_item_id.
fn create_table_data_items(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE data_items(
                id                  INTEGER PRIMARY KEY NOT NULL,
                
                creator_store_id    INTEGER NOT NULL,
                creator_version     INTEGER NOT NULL,

                parent_item_id      INTEGER,

                path                TEXT NOT NULL,
                is_file             INTEGER NOT NULL, 

                UNIQUE(creator_store_id, creator_version),
                FOREIGN KEY(creator_store_id)   REFERENCES data_store(id),
                FOREIGN KEY(parent_item_id)     REFERENCES data_item(id)
            )",
    )
    .execute(conn)?;

    Ok(())
}

// Metadata associated to a data item from the view of a specific data store.
// Usually, we will only keep information on our local data_store, as this is required for
// detecting local updates. However, in some use cases we might want to communicate other
// metadata, thus also keep it.
fn create_table_item_metadatas(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE item_metadatas(
                id                  INTEGER PRIMARY KEY NOT NULL,

                data_store_id       INTEGER NOT NULL,

                creation_time       TEXT NOT NULL,
                mod_time            TEXT NOT NULL,
                hash                TEXT NOT NULL,
    
                FOREIGN KEY(data_store_id)      REFERENCES data_store(id)
            )",
    )
    .execute(conn)?;

    Ok(())
}

// Data_items have no notion of modification/sync times, which in turn must be tailored to
// what each individual data_store knows about them.
//
// To fill this gap, the owner_information can associate this information to a data_item.
// Each owner_information represents the knowledge that we know something about this data item
// from the perspective of a given data store.
//
// For the 'classic' sync algorithm we only need our own information (i.e. the information
// associated with the data_store this database belonging to this physical location).
// For 'eagerly' sending data to remote sites it is important to also keep some information
// about them. This can very depending on the use-case (might be user configurable later on)
// and can therefore range from enough information to know that we might need to send data to a
// site up to all information about the other site.
fn create_table_owner_informations(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE owner_informations(
                id              INTEGER PRIMARY KEY NOT NULL,

                data_store_id   INTEGER NOT NULL,
                data_item_id    INTEGER NOT NULL,

                UNIQUE(data_store_id, data_item_id),
                FOREIGN KEY(data_store_id)      REFERENCES data_store(id),
                FOREIGN KEY(data_item_id)       REFERENCES data_item(id)
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
                id                      INTEGER PRIMARY KEY NOT NULL,
                
                owner_information_id    INTEGER NOT NULL,
                
                data_store_id           INTEGER NOT NULL,
                time                    INTEGER NOT NULL,

                UNIQUE(owner_information_id, data_store_id),
                FOREIGN KEY(owner_information_id)   REFERENCES owner_information(id),
                FOREIGN KEY(data_store_id)          REFERENCES data_store(id)
            )",
    )
    .execute(conn)?;

    Ok(())
}

// Stores a synchronization time of a file from the view of a specific owner,
// i.e. it encodes the information of the form:
// "data_item from the view of owner_information has synchronization time stamp
//  data_store -> time (the time the data_item was synchronized most recently with the data_store)"
fn create_table_sync_times(conn: &SqliteConnection) -> Result<()> {
    sql_query(
        "CREATE TABLE sync_times(
                id                      INTEGER PRIMARY KEY NOT NULL,
                
                owner_information_id    INTEGER NOT NULL,
                
                data_store_id           INTEGER NOT NULL,
                time                    INTEGER NOT NULL,

                UNIQUE(owner_information_id, data_store_id),
                FOREIGN KEY(owner_information_id)   REFERENCES owner_information(id),
                FOREIGN KEY(data_store_id)          REFERENCES data_store(id)
            )",
    )
    .execute(conn)?;

    Ok(())
}
