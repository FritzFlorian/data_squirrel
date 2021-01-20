use super::schema::mod_metadatas;

#[derive(Debug, Queryable, QueryableByName, Clone)]
#[table_name = "mod_metadatas"]
pub struct ModMetadata {
    pub id: i64,

    pub creator_store_id: i64,
    pub creator_store_time: i64,

    pub last_mod_store_id: i64,
    pub last_mod_store_time: i64,
}

#[derive(Insertable)]
#[table_name = "mod_metadatas"]
pub struct InsertFull {
    pub id: i64,

    pub creator_store_id: i64,
    pub creator_store_time: i64,

    pub last_mod_store_id: i64,
    pub last_mod_store_time: i64,
}

#[derive(AsChangeset)]
#[table_name = "mod_metadatas"]
pub struct UpdateCreator {
    pub creator_store_id: i64,
    pub creator_store_time: i64,
}

#[derive(AsChangeset)]
#[table_name = "mod_metadatas"]
pub struct UpdateLastMod {
    pub last_mod_store_id: i64,
    pub last_mod_store_time: i64,
}
