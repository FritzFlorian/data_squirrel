use super::schema::metadatas;

#[derive(Debug, Queryable)]
pub struct Metadata {
    pub id: i64,

    pub owner_information_id: i64,

    pub creation_time: chrono::NaiveDateTime,
    pub mod_time: chrono::NaiveDateTime,

    pub hash: String,
}

#[derive(Insertable)]
#[table_name = "metadatas"]
pub struct InsertFull {
    pub owner_information_id: i64,

    pub creation_time: chrono::NaiveDateTime,
    pub mod_time: chrono::NaiveDateTime,

    pub hash: String,
}
