use super::schema::items;

#[derive(Debug, Queryable, QueryableByName, Clone)]
#[table_name = "items"]
pub struct Item {
    pub id: i64,

    pub data_store_id: i64,
    pub path_component_id: i64,

    pub is_file: bool,
    pub is_deleted: bool,
}

#[derive(Insertable)]
#[table_name = "items"]
pub struct InsertFull {
    pub data_store_id: i64,
    pub path_component_id: i64,

    pub is_file: bool,
    pub is_deleted: bool,
}
