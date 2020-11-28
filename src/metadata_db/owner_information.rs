use super::schema::owner_informations;

#[derive(Debug, Queryable, Insertable)]
pub struct OwnerInformation {
    pub id: i64,

    pub data_store_id: i64,
    pub data_item_id: i64,
}
