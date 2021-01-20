use diesel::query_builder::*;
use diesel::result::QueryResult;
use diesel::sql_types::*;
use diesel::sqlite::Sqlite;

use super::*;

/// Construct a query to load a single database item.
/// This includes the path_component, item, fs_metadata and mod_metadata belonging to the item.
#[derive(QueryId)]
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
