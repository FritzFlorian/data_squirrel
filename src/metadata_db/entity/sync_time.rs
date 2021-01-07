use super::schema::sync_times;

#[derive(Debug, Queryable)]
pub struct SyncTime {
    pub id: i64,

    pub item_id: i64,

    pub data_store_id: i64,
    pub time: i64,
}

#[derive(Insertable)]
#[table_name = "sync_times"]
pub struct InsertFull {
    pub item_id: i64,

    pub data_store_id: i64,
    pub time: i64,
}
