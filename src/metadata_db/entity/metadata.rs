use super::schema::metadatas;

#[derive(Debug, Queryable, Clone)]
pub struct Metadata {
    pub id: i64,
    pub owner_information_id: i64,

    pub creator_store_id: i64,
    pub creator_store_time: i64,

    pub case_sensitive_name: String,
    pub creation_time: chrono::NaiveDateTime,
    pub mod_time: chrono::NaiveDateTime,
    pub hash: String,
}

#[derive(Insertable)]
#[table_name = "metadatas"]
pub struct InsertFull<'a> {
    pub owner_information_id: i64,

    pub creator_store_id: i64,
    pub creator_store_time: i64,

    pub case_sensitive_name: &'a str,
    pub creation_time: chrono::NaiveDateTime,
    pub mod_time: chrono::NaiveDateTime,
    pub hash: &'a str,
}

#[derive(AsChangeset)]
#[table_name = "metadatas"]
pub struct UpdateMetadata<'a> {
    pub case_sensitive_name: &'a str,
    pub creation_time: &'a chrono::NaiveDateTime,
    pub mod_time: &'a chrono::NaiveDateTime,
    pub hash: &'a str,
}
