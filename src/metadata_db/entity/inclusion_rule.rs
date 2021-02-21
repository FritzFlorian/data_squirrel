use super::schema::inclusion_rules;

#[derive(Debug, Queryable, QueryableByName, Clone)]
#[table_name = "inclusion_rules"]
pub struct InclusionRule {
    pub id: i64,
    pub data_store_id: i64,

    pub rule_glob: String,
    pub include: bool,
}

#[derive(Insertable)]
#[table_name = "inclusion_rules"]
pub struct InsertFull {
    pub data_store_id: i64,

    pub rule_glob: String,
    pub include: bool,
}
