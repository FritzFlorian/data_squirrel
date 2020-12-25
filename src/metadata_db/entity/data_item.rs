use super::schema::data_items;

#[derive(Debug, Queryable)]
pub struct DataItem {
    pub id: i64,

    pub parent_item_id: Option<i64>,
    pub path: String,
}

#[derive(Insertable)]
#[table_name = "data_items"]
pub struct InsertFull<'a> {
    pub parent_item_id: Option<i64>,
    pub path: &'a str,
}
