use super::schema::data_sets;

#[derive(Debug, Queryable, Clone)]
pub struct DataSet {
    pub id: i64,
    pub unique_name: String,
    pub human_name: String,
}

#[derive(Insertable)]
#[table_name = "data_sets"]
pub struct FromUniqueName<'a> {
    pub unique_name: &'a str,
}
