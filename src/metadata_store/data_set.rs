use rusqlite;

#[derive(Debug)]
pub struct DataSet {
    id: i64,
    pub unique_name: String,
    pub human_name: String,
}

impl DataSet {
    pub fn get_id(&self) -> i64 {
        self.id
    }

    pub fn create(connection: &rusqlite::Connection, unique_name: &str) -> rusqlite::Result<()> {
        let mut query = connection.prepare(
            "
            INSERT INTO data_set(unique_name, human_name)
            VALUES (:unique_name, '')
        ",
        )?;

        query.execute_named(rusqlite::named_params! {
            ":unique_name": &unique_name,
        })?;

        Ok(())
    }

    pub fn get(connection: &rusqlite::Connection) -> rusqlite::Result<Option<DataSet>> {
        let mut query = connection.prepare(
            "
            SELECT id, unique_name, human_name FROM data_set LIMIT 1
        ",
        )?;

        let mut data_set = query.query_map(rusqlite::params![], |row| {
            Ok(DataSet {
                id: row.get(0)?,
                unique_name: row.get(1)?,
                human_name: row.get(2)?,
            })
        })?;

        if let Some(result) = data_set.next() {
            Ok(Some(result?))
        } else {
            Ok(None)
        }
    }

    pub fn update(&self, connection: &rusqlite::Connection) -> rusqlite::Result<()> {
        let mut query = connection.prepare(
            "
            UPDATE data_set
            SET unique_name = :unique_name, human_name = :human_name
            WHERE id = :id
        ",
        )?;

        query.execute_named(rusqlite::named_params! {
            ":id" : &self.id,
            ":unique_name" : &self.unique_name,
            ":human_name" : &self.human_name
        })?;

        Ok(())
    }
}
