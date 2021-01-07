use super::schema::file_system_metadatas;

#[derive(Debug, Queryable, Clone)]
pub struct FileSystemMetadata {
    pub id: i64,

    pub case_sensitive_name: String,
    pub creation_time: chrono::NaiveDateTime,
    pub mod_time: chrono::NaiveDateTime,
    pub hash: String,
}

#[derive(Insertable)]
#[table_name = "file_system_metadatas"]
pub struct InsertFull<'a> {
    pub id: i64,

    pub case_sensitive_name: &'a str,
    pub creation_time: chrono::NaiveDateTime,
    pub mod_time: chrono::NaiveDateTime,
    pub hash: &'a str,
}

#[derive(AsChangeset)]
#[table_name = "file_system_metadatas"]
pub struct UpdateMetadata<'a> {
    pub case_sensitive_name: &'a str,
    pub creation_time: &'a chrono::NaiveDateTime,
    pub mod_time: &'a chrono::NaiveDateTime,
    pub hash: &'a str,
}
