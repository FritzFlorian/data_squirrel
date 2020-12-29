use super::schema::owner_informations;

#[derive(Debug, Queryable)]
pub struct OwnerInformation {
    pub id: i64,

    pub data_store_id: i64,
    pub data_item_id: i64,
}

#[derive(Insertable)]
#[table_name = "owner_informations"]
pub struct InsertFull {
    pub data_store_id: i64,
    pub data_item_id: i64,
}