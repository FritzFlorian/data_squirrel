use super::schema::data_stores;

#[derive(Debug, Queryable, Clone)]
pub struct DataStore {
    pub id: i64,
    pub data_set_id: i64,

    pub unique_name: String,
    pub human_name: String,
    pub creation_date: chrono::NaiveDateTime,
    pub path_on_device: String,
    pub location_note: String,

    pub is_this_store: bool,
    pub time: i64,
}

#[derive(Insertable)]
#[table_name = "data_stores"]
pub struct InsertFull<'a> {
    pub data_set_id: i64,

    pub unique_name: &'a str,
    pub human_name: &'a str,
    pub creation_date: &'a chrono::NaiveDateTime,
    pub path_on_device: &'a str,
    pub location_note: &'a str,

    pub is_this_store: bool,
    pub time: i64,
}
