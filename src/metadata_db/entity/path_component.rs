use super::schema::path_components;

#[derive(Debug, Queryable)]
pub struct PathComponent {
    pub id: i64,

    pub parent_component_id: Option<i64>,
    pub path_component: String,
}

#[derive(Insertable)]
#[table_name = "path_components"]
pub struct InsertFull<'a> {
    pub parent_component_id: Option<i64>,
    pub path_component: &'a str,
}
