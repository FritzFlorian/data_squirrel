use super::data_set::DataSet;
use chrono::NaiveDateTime;
use rusqlite;
use std::path::PathBuf;

#[derive(Debug)]
pub struct DataStore {
    id: i64,
    data_set_id: i64,

    unique_name: String,
    human_name: String,
    creation_date: chrono::NaiveDateTime,
    path_on_device: String,
    location_note: String,
    is_this_store: bool,
    version: i64,
}

impl DataStore {
    pub fn map_rows(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok(DataStore {
            id: row.get(row.column_index("id")?)?,
            data_set_id: row.get(row.column_index("data_set_id")?)?,

            unique_name: row.get(row.column_index("unique_name")?)?,
            human_name: row.get(row.column_index("human_name")?)?,
            creation_date: row.get(row.column_index("creation_date")?)?,
            path_on_device: row.get(row.column_index("path_on_device")?)?,
            location_note: row.get(row.column_index("location_note")?)?,
            is_this_store: row.get(row.column_index("is_this_store")?)?,
            version: row.get(row.column_index("version")?)?,
        })
    }

    pub fn create(
        connection: &rusqlite::Connection,
        data_set: &DataSet,
        unique_name: &str,
        human_name: &str,
        creation_date: chrono::NaiveDateTime,
        path_on_device: &str,
        location_note: &str,
        is_this_store: bool,
        version: i64,
    ) -> rusqlite::Result<Self> {
        let mut query = connection.prepare(
            "
            INSERT INTO data_store (data_set_id, unique_name, human_name, creation_date, 
                                    path_on_device, location_note, is_this_store, version)
            VALUES(:data_set_id, :unique_name, :human_name, :creation_date,
                   :path_on_device, :location_note, :is_this_store, :version)
        ",
        )?;

        query.execute_named(rusqlite::named_params! {
            ":data_set_id": data_set.id,
            ":unique_name": &unique_name,
            ":human_name": &human_name,
            ":creation_date": &creation_date,
            ":path_on_device": &path_on_device,
            ":location_note": &location_note,
            ":is_this_store": is_this_store,
            ":version": version,
        })?;

        Ok(Self::get(&connection, &data_set, unique_name)?.unwrap())
    }

    pub fn get_all(
        connection: &rusqlite::Connection,
        data_set: &DataSet,
        unique_name: Option<&str>,
    ) -> rusqlite::Result<Vec<Self>> {
        let mut query = if unique_name.is_some() {
            connection.prepare(
                "
            SELECT id, data_set_id, unique_name, human_name, creation_date, path_on_device,
                   location_note, is_this_store, version
            FROM data_store
            WHERE unique_name = :unique_name AND data_set_id = :data_set_id
            ",
            )?
        } else {
            connection.prepare(
                "
            SELECT id, data_set_id, unique_name, human_name, creation_date, path_on_device,
                   location_note, is_this_store, version
            FROM data_store
            WHERE data_set_id = :data_set_id
            ",
            )?
        };

        let result: Result<Vec<_>, _> = query.query_map_named(rusqlite::named_params! {":data_set_id": data_set.id, ":unique_name": unique_name.unwrap_or("") }, Self::map_rows)?.collect();

        result
    }

    pub fn get(
        connection: &rusqlite::Connection,
        data_set: &DataSet,
        unique_name: &str,
    ) -> rusqlite::Result<Option<Self>> {
        let mut matching_data_stores = Self::get_all(&connection, &data_set, Some(&unique_name))?;
        if matching_data_stores.len() == 1 {
            Ok(Some(matching_data_stores.remove(0)))
        } else {
            Ok(None)
        }
    }
}
