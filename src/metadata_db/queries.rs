use diesel::query_builder::*;
use diesel::result::QueryResult;
use diesel::sql_types::*;
use diesel::sqlite::Sqlite;

use super::*;

#[derive(Debug, Clone, QueryId)]
pub struct AllPathComponents {
    pub path_string: String,
}
impl QueryFragment<Sqlite> for AllPathComponents {
    fn walk_ast(&self, mut out: AstPass<Sqlite>) -> QueryResult<()> {
        out.push_sql("SELECT * FROM path_components WHERE full_path IN ");
        out.push_sql("(");
        {
            out.push_sql("WITH RECURSIVE paths(full_path, rest_path) AS ");
            out.push_sql("(");
            {
                out.push_sql("SELECT '/' AS full_path, substr(");
                out.push_bind_param::<Text, _>(&self.path_string)?;
                out.push_sql(", 2) as rest_path ");

                out.push_sql("UNION ");

                out.push_sql("SELECT paths.full_path || substr(paths.rest_path, 1, instr(paths.rest_path, '/')) as full_path, ");
                out.push_sql(
                    "substr(paths.rest_path, instr(paths.rest_path, '/') + 1) as rest_path ",
                );
                out.push_sql("FROM paths ");
            }
            out.push_sql(") ");
            out.push_sql("SELECT full_path FROM paths ")
        }
        out.push_sql(")");

        Ok(())
    }
}
impl Query for AllPathComponents {
    type SqlType = path_components::SqlType;
}
impl RunQueryDsl<SqliteConnection> for AllPathComponents {}

/// Construct a query to load a single database item.
/// This includes the path_component, item, fs_metadata and mod_metadata belonging to the item.
#[derive(Debug, Clone, QueryId)]
pub struct ItemLoader<PQ, IQ> {
    pub path_query: PQ,
    pub item_query: IQ,
}
pub type ItemLoaderResult = (
    PathComponent,
    Item,
    Option<FileSystemMetadata>,
    Option<ModMetadata>,
);

impl<PQ, IQ> QueryFragment<Sqlite> for ItemLoader<PQ, IQ>
where
    PQ: QueryFragment<Sqlite>,
    IQ: QueryFragment<Sqlite>,
{
    fn walk_ast(&self, mut out: AstPass<Sqlite>) -> QueryResult<()> {
        out.push_sql(" SELECT *");

        out.push_sql(" FROM (");
        self.path_query.walk_ast(out.reborrow())?;
        out.push_sql(") AS path_components");

        out.push_sql(" INNER JOIN (");
        self.item_query.walk_ast(out.reborrow())?;
        out.push_sql(") AS items ON path_components.id = items.path_component_id");

        out.push_sql(" LEFT JOIN file_system_metadatas ON items.id = file_system_metadatas.id");
        out.push_sql(" LEFT JOIN mod_metadatas ON items.id = mod_metadatas.id");
        out.push_sql(" ORDER BY path_components.full_path ASC");

        Ok(())
    }
}

impl<PQ, IQ> Query for ItemLoader<PQ, IQ> {
    type SqlType = (
        path_components::SqlType,
        items::SqlType,
        Nullable<file_system_metadatas::SqlType>,
        Nullable<mod_metadatas::SqlType>,
    );
}
impl<PQ, IQ> RunQueryDsl<SqliteConnection> for ItemLoader<PQ, IQ> {}
