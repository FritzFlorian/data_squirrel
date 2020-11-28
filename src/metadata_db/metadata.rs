use super::schema::metadatas;

#[derive(Debug, Queryable, Insertable)]
pub struct Metadata {
    pub id: i64,

    pub owner_information_id: i64,

    pub creation_time: chrono::NaiveDateTime,
    pub mod_time: chrono::NaiveDateTime,

    pub hash: String,
}
