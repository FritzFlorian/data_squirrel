use super::schema::metadatas;

#[derive(Debug, Queryable)]
pub struct Metadata {
    pub id: i64,
    pub owner_information_id: i64,

    pub creator_store_id: i64,
    pub creator_store_time: i64,

    pub is_file: bool,
    pub creation_time: chrono::NaiveDateTime,
    pub mod_time: chrono::NaiveDateTime,
    pub hash: String,
}

#[derive(Insertable)]
#[table_name = "metadatas"]
pub struct InsertFull {
    pub owner_information_id: i64,

    pub creator_store_id: i64,
    pub creator_store_time: i64,

    pub is_file: bool,
    pub creation_time: chrono::NaiveDateTime,
    pub mod_time: chrono::NaiveDateTime,
    pub hash: String,
}

#[derive(AsChangeset)]
#[table_name = "metadatas"]
pub struct UpdateMetadata<'a> {
    pub creation_time: &'a chrono::NaiveDateTime,
    pub mod_time: &'a chrono::NaiveDateTime,
    pub hash: &'a str,
}
