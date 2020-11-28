use super::schema::data_stores;

#[derive(Debug, Queryable, Insertable)]
pub struct DataStore {
    pub id: i64,
    pub data_set_id: i64,

    pub unique_name: String,
    pub human_name: String,
    pub creation_date: chrono::NaiveDateTime,
    pub path_on_device: String,
    pub location_note: String,
    pub is_this_store: bool,
    pub version: i64,
}
