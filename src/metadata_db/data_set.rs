use rusqlite;

#[derive(Debug)]
pub struct DataSet {
    pub id: i64,
    pub unique_name: String,
    pub human_name: String,
}

impl DataSet {
    pub fn map_rows(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok(DataSet {
            id: row.get(row.column_index("id")?)?,
            unique_name: row.get(row.column_index("unique_name")?)?,
            human_name: row.get(row.column_index("human_name")?)?,
        })
    }

    pub fn create(connection: &rusqlite::Connection, unique_name: &str) -> rusqlite::Result<Self> {
        let mut query = connection.prepare(
            "
            INSERT INTO data_set(unique_name, human_name)
            VALUES (?, '')
        ",
        )?;
        let row_id = query.insert(rusqlite::params![unique_name])?;

        Ok(DataSet {
            id: row_id,
            unique_name: unique_name.to_string(),
            human_name: String::new(),
        })
    }

    pub fn get(connection: &rusqlite::Connection) -> rusqlite::Result<Option<DataSet>> {
        let mut query = connection.prepare(
            "
            SELECT * FROM data_set LIMIT 1
        ",
        )?;

        let mut data_set = query.query_map(rusqlite::params![], Self::map_rows)?;

        if let Some(result) = data_set.next() {
            Ok(Some(result?))
        } else {
            Ok(None)
        }
    }
}
