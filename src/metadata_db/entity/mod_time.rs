use super::schema::mod_times;

#[derive(Debug, Queryable)]
pub struct ModTime {
    pub id: i64,

    pub owner_information_id: i64,

    pub data_store_id: i64,
    pub time: i64,
}

#[derive(Insertable)]
#[table_name = "mod_times"]
pub struct InsertFull {
    pub owner_information_id: i64,

    pub data_store_id: i64,
    pub time: i64,
}
