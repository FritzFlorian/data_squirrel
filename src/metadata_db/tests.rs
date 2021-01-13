use super::*;
use chrono::NaiveDateTime;

fn open_metadata_store() -> MetadataDB {
    MetadataDB::open(":memory:").unwrap()
}

fn insert_sample_data_set(metadata_store: &MetadataDB) -> (DataSet, DataStore) {
    let data_set = metadata_store.create_data_set("abc").unwrap();
    let data_store = insert_data_store(&metadata_store, &data_set, "abc", true);

    (data_set, data_store)
}

fn insert_data_store(
    metadata_store: &MetadataDB,
    data_set: &DataSet,
    unique_name: &str,
    this_store: bool,
) -> DataStore {
    metadata_store
        .create_data_store(&data_store::InsertFull {
            data_set_id: data_set.id,
            unique_name: &unique_name,
            human_name: &"",
            is_this_store: this_store,
            time: 0,

            creation_date: &NaiveDateTime::from_timestamp(0, 0),
            path_on_device: &"/",
            location_note: &"",
        })
        .unwrap()
}

fn insert_data_item(metadata_store: &MetadataDB, name: &str, is_file: bool) {
    metadata_store
        .update_local_data_item(
            &RelativePath::from_path(name),
            NaiveDateTime::from_timestamp(0, 0),
            NaiveDateTime::from_timestamp(0, 0),
            is_file,
            "",
        )
        .unwrap();
}
fn delete_data_item(metadata_store: &MetadataDB, name: &str) {
    metadata_store
        .delete_local_data_item(&RelativePath::from_path(name))
        .unwrap();
}
fn assert_mod_time(metadata_store: &MetadataDB, name: &str, key: i64, value: i64) {
    let item = metadata_store
        .get_local_data_item(&RelativePath::from_path(name))
        .unwrap();
    match item.content {
        ItemType::FILE { .. } => assert_eq!(item.mod_time()[&key], value),
        ItemType::FOLDER { .. } => assert_eq!(item.mod_time()[&key], value),
        ItemType::DELETION => panic!("Must not check mod times on deletions"),
    };
}
fn assert_sync_time(metadata_store: &MetadataDB, name: &str, key: i64, value: i64) {
    let item = metadata_store
        .get_local_data_item(&RelativePath::from_path(name))
        .unwrap();
    assert_eq!(item.sync_time[&key], value);
}

#[test]
fn insert_and_query_data_set() {
    let metadata_store = open_metadata_store();

    assert!(metadata_store.get_data_set().is_err());

    metadata_store.create_data_set("abc").unwrap();
    let data_set = metadata_store.get_data_set().unwrap();
    assert_eq!(data_set.unique_name, "abc");
    assert_eq!(data_set.human_name, "");

    metadata_store.update_data_set_name("testing").unwrap();
    let data_set = metadata_store.get_data_set().unwrap();
    assert_eq!(data_set.unique_name, "abc");
    assert_eq!(data_set.human_name, "testing");
}

#[test]
fn enforces_single_data_set() {
    let metadata_store = open_metadata_store();

    metadata_store.create_data_set("abc").unwrap();
    match metadata_store.create_data_set("xyz") {
        Err(MetadataDBError::ViolatesDBConsistency { .. }) => (),
        _ => panic!("Must not have more than one data_set in DB!"),
    }
}

#[test]
fn correctly_enter_data_items() {
    let metadata_store = open_metadata_store();
    let (_data_set, data_store) = insert_sample_data_set(&metadata_store);

    // Individual inserts have correct mod times
    assert_mod_time(&metadata_store, "", data_store.id, 0);

    insert_data_item(&metadata_store, "sub", false);
    assert_mod_time(&metadata_store, "sub", data_store.id, 1);

    insert_data_item(&metadata_store, "sub/folder", false);
    assert_mod_time(&metadata_store, "sub/folder", data_store.id, 2);

    insert_data_item(&metadata_store, "sub/folder/file", false);
    assert_mod_time(&metadata_store, "sub/folder/file", data_store.id, 3);

    // Parent folders get updated correctly
    assert_mod_time(&metadata_store, "", data_store.id, 3);
    assert_mod_time(&metadata_store, "sub", data_store.id, 3);
    assert_mod_time(&metadata_store, "sub/folder", data_store.id, 3);

    // The database is invariant on capitalization when searching or inserting items
    assert_mod_time(&metadata_store, "", data_store.id, 3);
    assert_mod_time(&metadata_store, "sUb", data_store.id, 3);
    assert_mod_time(&metadata_store, "sub/FolDer", data_store.id, 3);

    insert_data_item(&metadata_store, "sUb", false);
    assert_mod_time(&metadata_store, "sub", data_store.id, 4);

    // Check if child queries work
    let children = metadata_store
        .get_local_child_data_items(&RelativePath::from_path(""))
        .unwrap();
    assert_eq!(children.len(), 1);
    assert!(children[0].is_folder());
    assert_eq!(children[0].path_component, "sub");

    // Delete items (partially, we did not 'clean up' deletion notices jet).
    delete_data_item(&metadata_store, "sub/folder/file");
    delete_data_item(&metadata_store, "sub/folder");
    delete_data_item(&metadata_store, "sub");
    let children = metadata_store
        .get_local_child_data_items(&RelativePath::from_path(""))
        .unwrap();
    assert_eq!(children.len(), 1);
    assert!(children[0].is_deletion());
    assert_eq!(children[0].path_component, "sub");

    // Create new files 'over' an previous deletion notice.
    insert_data_item(&metadata_store, "SUB", false);
    assert_mod_time(&metadata_store, "sub", data_store.id, 8);

    // TODO: Clean up deletion notices and re-query child items!
}

#[test]
fn correctly_inserts_synced_data_items() {
    // We use our usual local, sample data set and store and create an additional remote one.
    let metadata_store = open_metadata_store();
    let (data_set, local_store) = insert_sample_data_set(&metadata_store);
    let remote_store = insert_data_store(&metadata_store, &data_set, "remote", false);

    // Insert some sample items (/sub/folder/file)
    insert_data_item(&metadata_store, "sub", false);
    insert_data_item(&metadata_store, "sub/folder", false);
    insert_data_item(&metadata_store, "sub/folder/file", true);

    // First of, lets try bumping some synchronization vector times.
    let sync_time = VersionVector::from_initial_values(vec![(&remote_store.id, 10)]);
    metadata_store
        .max_sync_times_recursive(&RelativePath::from_path("sub"), &sync_time)
        .unwrap();
    assert_sync_time(&metadata_store, "", remote_store.id, 0);
    assert_sync_time(&metadata_store, "sub", remote_store.id, 10);
    assert_sync_time(&metadata_store, "sub/folder/file", remote_store.id, 10);

    // Also try to 'partially' bump the sync times.
    let sync_time =
        VersionVector::from_initial_values(vec![(&local_store.id, 5), (&remote_store.id, 7)]);
    metadata_store
        .max_sync_times_recursive(&RelativePath::from_path(""), &sync_time)
        .unwrap();
    assert_sync_time(&metadata_store, "", remote_store.id, 7);
    assert_sync_time(&metadata_store, "sub", remote_store.id, 10);
    assert_sync_time(&metadata_store, "sub/folder/file", remote_store.id, 10);

    assert_sync_time(&metadata_store, "", local_store.id, 5);
    assert_sync_time(&metadata_store, "sub", local_store.id, 5);
    assert_sync_time(&metadata_store, "sub/folder/file", local_store.id, 5);

    // Let's query an item, change it and re-synchronize it into our local db
    let mut file = metadata_store
        .get_local_data_item(&RelativePath::from_path("sub/folder/file"))
        .unwrap();

    // ...this should be as if the second store overwrites the local one with a new version.
    let new_mod_time = VersionVector::from_initial_values(vec![(&remote_store.id, 42)]);
    let new_sync_time = VersionVector::from_initial_values(vec![(&remote_store.id, 1024)]);
    file.sync_time = new_sync_time;

    file.content = ItemType::FILE {
        metadata: file.metadata().clone(),
        creation_time: file.creation_time().clone(),
        last_mod_time: new_mod_time,
    };

    metadata_store
        .sync_local_data_item(&RelativePath::from_path("sub/folder/file"), &file)
        .unwrap();

    // Check if the synced item looks right.
    let file_after_update = metadata_store
        .get_local_data_item(&RelativePath::from_path("sub/folder/file"))
        .unwrap();
    assert_eq!(file_after_update.sync_time[&local_store.id], 5);
    assert_eq!(file_after_update.sync_time[&remote_store.id], 1024);
    assert_eq!(file_after_update.mod_time()[&local_store.id], 0);
    assert_eq!(file_after_update.mod_time()[&remote_store.id], 42);
    let root_item_after_update = metadata_store
        .get_local_data_item(&RelativePath::from_path(""))
        .unwrap();
    assert_eq!(root_item_after_update.mod_time()[&local_store.id], 3);
    assert_eq!(root_item_after_update.mod_time()[&remote_store.id], 42);

    // Try a more complicated case where we change a folder to be a file
    let mut folder = metadata_store
        .get_local_data_item(&RelativePath::from_path("sub/folder"))
        .unwrap();

    let new_sync_time = VersionVector::from_initial_values(vec![(&remote_store.id, 2048)]);
    folder.sync_time = new_sync_time;
    folder.content = ItemType::FILE {
        metadata: folder.metadata().clone(),
        creation_time: folder.creation_time().clone(),
        last_mod_time: folder.last_mod_time().clone(),
    };

    metadata_store
        .sync_local_data_item(&RelativePath::from_path("sub/folder"), &folder)
        .unwrap();

    // We expect the file below to be implicitly deleted and have the appropriate sync time.
    let item_after_update = metadata_store
        .get_local_data_item(&RelativePath::from_path("sub/folder/file"))
        .unwrap();
    assert!(item_after_update.is_deletion());
    assert_eq!(item_after_update.sync_time[&remote_store.id], 2048);

    // Another interesting case is if we receive a single deletion notice.
    let mut root = metadata_store
        .get_local_data_item(&RelativePath::from_path(""))
        .unwrap();

    let new_sync_time = VersionVector::from_initial_values(vec![(&remote_store.id, 4096)]);
    root.sync_time = new_sync_time;

    metadata_store
        .sync_local_data_item(&RelativePath::from_path("sub/folder"), &root)
        .unwrap();

    let root_after_update = metadata_store
        .get_local_data_item(&RelativePath::from_path("sub/folder/file"))
        .unwrap();
    assert!(root_after_update.is_deletion());
    assert_eq!(root_after_update.sync_time[&remote_store.id], 4096,);
    let file_item_after_update = metadata_store
        .get_local_data_item(&RelativePath::from_path("sub/folder/file"))
        .unwrap();
    assert!(file_item_after_update.is_deletion());
    assert_eq!(file_item_after_update.sync_time[&remote_store.id], 4096,);
}