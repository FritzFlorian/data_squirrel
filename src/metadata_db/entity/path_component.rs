use super::schema::path_components;

#[derive(Debug, Queryable, QueryableByName, Clone)]
#[table_name = "path_components"]
pub struct PathComponent {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub full_path: String,
}

#[derive(Insertable)]
#[table_name = "path_components"]
pub struct InsertFull<'a> {
    pub parent_id: Option<i64>,
    pub full_path: &'a str,
}
