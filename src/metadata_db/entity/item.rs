use super::schema::items;
use super::FileType;

#[derive(Debug, Queryable, QueryableByName, Clone)]
#[table_name = "items"]
pub struct Item {
    pub id: i64,

    pub data_store_id: i64,
    pub path_component_id: i64,

    pub file_type: FileType,
}

#[derive(Insertable)]
#[table_name = "items"]
pub struct InsertFull {
    pub data_store_id: i64,
    pub path_component_id: i64,

    pub file_type: FileType,
}
