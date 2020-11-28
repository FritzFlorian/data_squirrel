use super::schema::data_items;

#[derive(Debug, Queryable, Insertable)]
pub struct DataItem {
    pub id: i64,

    pub creator_store_id: i64,
    pub creator_version: i64,

    pub parent_item_id: Option<i64>,

    pub path: String,
    pub is_file: bool,
}
