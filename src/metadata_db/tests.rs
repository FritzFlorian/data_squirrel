use super::*;
use chrono::NaiveDateTime;

pub fn open_metadata_store() -> MetadataDB {
    MetadataDB::open(":memory:").unwrap()
}

pub fn insert_sample_data_set(metadata_store: &MetadataDB) -> (DataSet, DataStore) {
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

            creation_date: &NaiveDateTime::from_timestamp(0, 0),
            path_on_device: &"/",
            location_note: &"",

            is_this_store: this_store,
            is_transfer_store: false,

            time: 0,
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
            false,
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
        .get_local_data_item(&RelativePath::from_path(name), true)
        .unwrap();
    match item.content {
        ItemType::FILE { .. } => assert_eq!(item.mod_time()[&key], value),
        ItemType::FOLDER { .. } => assert_eq!(item.mod_time()[&key], value),
        ItemType::DELETION => panic!("Must not check mod times on deletions!"),
        ItemType::IGNORED { .. } => panic!("Must not check mod times on ignored items!"),
    };
}
fn assert_sync_time(metadata_store: &MetadataDB, name: &str, key: i64, value: i64) {
    let item = metadata_store
        .get_local_data_item(&RelativePath::from_path(name), true)
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
fn clean_up_simple_deletions() {
    let metadata_store = open_metadata_store();
    let (_data_set, _data_store) = insert_sample_data_set(&metadata_store);

    insert_data_item(&metadata_store, "file-1", true);
    insert_data_item(&metadata_store, "folder-1", false);
    insert_data_item(&metadata_store, "folder-1/file-2", true);
    insert_data_item(&metadata_store, "folder-1/file-3", true);
    assert_eq!(metadata_store.count_items_in_db().unwrap(), 5);

    delete_data_item(&metadata_store, "file-1");
    delete_data_item(&metadata_store, "folder-1/file-2");
    delete_data_item(&metadata_store, "folder-1/file-3");
    assert_eq!(metadata_store.count_items_in_db().unwrap(), 5);

    metadata_store.clean_up_local_sync_times().unwrap();
    metadata_store.clean_up_deleted_items().unwrap();
    assert_eq!(metadata_store.count_items_in_db().unwrap(), 2);

    delete_data_item(&metadata_store, "folder-1");
    metadata_store.clean_up_local_sync_times().unwrap();
    metadata_store.clean_up_deleted_items().unwrap();
    assert_eq!(metadata_store.count_items_in_db().unwrap(), 1);

    assert_eq!(metadata_store.count_path_components_in_db().unwrap(), 5);
    metadata_store.clean_up_path_components().unwrap();
    assert_eq!(metadata_store.count_path_components_in_db().unwrap(), 1);
}

#[test]
fn clean_up_synced_deletions() {
    let metadata_store = open_metadata_store();
    let (data_set, local_store) = insert_sample_data_set(&metadata_store);
    let remote_store = insert_data_store(&metadata_store, &data_set, "remote", false);

    // Insert the same local items.
    insert_data_item(&metadata_store, "file-0", true);
    insert_data_item(&metadata_store, "file-1", true);
    insert_data_item(&metadata_store, "folder-1", false);
    insert_data_item(&metadata_store, "folder-1/file-2", true);
    insert_data_item(&metadata_store, "folder-1/file-3", true);
    assert_eq!(metadata_store.count_items_in_db().unwrap(), 6);

    // This time bump some sync times before we perform the deletion.
    // This should force us to keep some more notices.
    bump_sync_time(
        &metadata_store,
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 10)]),
        "file-1",
    );
    bump_sync_time(
        &metadata_store,
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 10)]),
        "folder-1",
    );
    bump_sync_time(
        &metadata_store,
        VersionVector::from_initial_values(vec![(&local_store.id, 15), (&remote_store.id, 15)]),
        "folder-1/file-3",
    );

    delete_data_item(&metadata_store, "file-0");
    delete_data_item(&metadata_store, "file-1");
    delete_data_item(&metadata_store, "folder-1/file-2");
    delete_data_item(&metadata_store, "folder-1/file-3");
    assert_eq!(metadata_store.count_items_in_db().unwrap(), 6);

    // We MUST keep 'file-1' and 'file-3', as their sync time differs from their parent sync time.
    // We MUST keep 'folder-1' and '', as they are not deleted. Makes 4 total.
    metadata_store.clean_up_local_sync_times().unwrap();
    // Take a intermediate step: here we should be able to identify the 'significant sync times'
    let significant_items = metadata_store
        .find_significant_sync_times(&local_store)
        .unwrap();
    assert_eq!(significant_items.len(), 4);
    assert!(significant_items
        .iter()
        .any(|item| item.0.to_path_buf().to_str().unwrap() == "file-1"));
    assert!(significant_items
        .iter()
        .any(|item| item.0.to_path_buf().to_str().unwrap() == "folder-1/file-3"));
    assert!(significant_items
        .iter()
        .any(|item| item.0.to_path_buf().to_str().unwrap() == "folder-1"));
    assert!(significant_items
        .iter()
        .any(|item| item.0.to_path_buf().to_str().unwrap() == ""));
    // Continue our clean-up and check it.
    metadata_store.clean_up_deleted_items().unwrap();
    metadata_store.clean_up_path_components().unwrap();
    assert_eq!(metadata_store.count_items_in_db().unwrap(), 4);
    assert_eq!(metadata_store.count_path_components_in_db().unwrap(), 4);
}

#[test]
fn correctly_enter_data_items() {
    let metadata_store = open_metadata_store();
    let (_data_set, data_store) = insert_sample_data_set(&metadata_store);

    // Individual inserts have correct mod times
    assert_mod_time(&metadata_store, "", data_store.id, 0);

    insert_data_item(&metadata_store, "sub", false);
    assert_mod_time(&metadata_store, "sub", data_store.id, 3);

    insert_data_item(&metadata_store, "sub/folder", false);
    assert_mod_time(&metadata_store, "sub/folder", data_store.id, 4);

    insert_data_item(&metadata_store, "sub/folder/file", true);
    assert_mod_time(&metadata_store, "sub/folder/file", data_store.id, 5);

    // Parent folders get updated correctly
    assert_mod_time(&metadata_store, "", data_store.id, 5);
    assert_mod_time(&metadata_store, "sub", data_store.id, 5);
    assert_mod_time(&metadata_store, "sub/folder", data_store.id, 5);

    // The database is invariant on capitalization when searching or inserting items
    assert_mod_time(&metadata_store, "", data_store.id, 5);
    assert_mod_time(&metadata_store, "sUb", data_store.id, 5);
    assert_mod_time(&metadata_store, "sub/FolDer", data_store.id, 5);

    insert_data_item(&metadata_store, "sUb", false);
    assert_mod_time(&metadata_store, "sub", data_store.id, 6);

    // Check if child queries work
    let children = metadata_store
        .get_local_child_items(&RelativePath::from_path(""), true)
        .unwrap();
    assert_eq!(children.len(), 1);
    assert!(children[0].is_folder());
    assert_eq!(children[0].path.name(), "sUb");

    // Delete items (partially, we did not 'clean up' deletion notices jet).
    delete_data_item(&metadata_store, "sub/folder/file");
    delete_data_item(&metadata_store, "sub/folder");
    delete_data_item(&metadata_store, "sub");
    let children = metadata_store
        .get_local_child_items(&RelativePath::from_path(""), true)
        .unwrap();
    assert_eq!(children.len(), 1);
    assert!(children[0].is_deletion());
    assert_eq!(children[0].path.name(), "sub");

    // Create new files 'over' an previous deletion notice.
    insert_data_item(&metadata_store, "SUB", false);
    assert_mod_time(&metadata_store, "sub", data_store.id, 10);

    // TODO: Clean up deletion notices and re-query child items!
}

#[test]
fn handle_file_names_with_same_prefix() {
    let metadata_store = open_metadata_store();
    let (_data_set, _data_store) = insert_sample_data_set(&metadata_store);

    // Regression test, before it would fail if two entries in the same directory where the
    // prefix of the targeted item.
    insert_data_item(&metadata_store, "sub", false);
    insert_data_item(&metadata_store, "sub/file..", true);
    insert_data_item(&metadata_store, "sub/file.", true);
    // We would find all 3, 'sub', 'sub/file.' and 'sub/file..' with the
    // bug in place, while we actually only want to search for items that directly match the target
    // path of 'sub/file...' without any extra post-fixes.
    insert_data_item(&metadata_store, "sub/file...", true);
}

#[test]
fn correctly_persevere_case_sensitivity() {
    // We expect the metadata DB to KEEP case sensitivity in file names when returning
    // an entry, but at the same time we expect it to be invariant to case sensitivity when
    // searching for an in the db.
    let metadata_store = open_metadata_store();
    let (_data_set, _data_store) = insert_sample_data_set(&metadata_store);

    // Insert some sample data with different cases (keep all paths intact)
    insert_data_item(&metadata_store, "sUB", false);
    insert_data_item(&metadata_store, "sUB/fOLDER", false);
    insert_data_item(&metadata_store, "sUB/fOLDER/fILE", true);

    // Query should work with any case sensitivity.
    let file = metadata_store
        .get_local_data_item(&RelativePath::from_path("sUB/fOLDER/fILE"), true)
        .unwrap();
    assert_eq!(file.path.name(), "fILE");
    assert_eq!(file.metadata().case_sensitive_name, "fILE");
    let file = metadata_store
        .get_local_data_item(&RelativePath::from_path("Sub/Folder/File"), true)
        .unwrap();
    assert_eq!(file.path.name(), "fILE");
    assert_eq!(file.metadata().case_sensitive_name, "fILE");

    // Inserts should work with any case sensitivity
    insert_data_item(&metadata_store, "sub/FOLDER/tEST", false);

    // Query of multiple children should work as expected.

    // Check if child queries work
    let children = metadata_store
        .get_local_child_items(&RelativePath::from_path("SUB/FOLDER"), true)
        .unwrap();
    assert_eq!(children.len(), 2);
    assert!(children.iter().any(|child| {
        child.path.name() == "fILE" && child.metadata().case_sensitive_name == "fILE"
    }));
    assert!(children.iter().any(|child| {
        child.path.name() == "tEST" && child.metadata().case_sensitive_name == "tEST"
    }));
}

fn bump_sync_time(metadata_store: &MetadataDB, sync_time: VersionVector<i64>, path: &str) {
    let mut target_data_item = metadata_store
        .get_local_data_item(&RelativePath::from_path(path), true)
        .unwrap();
    for (store_id, time) in sync_time.iter() {
        target_data_item.sync_time[&store_id] = *time;
    }
    metadata_store
        .sync_local_data_item(&RelativePath::from_path(path), &target_data_item)
        .unwrap();
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
    bump_sync_time(
        &metadata_store,
        VersionVector::from_initial_values(vec![(&remote_store.id, 100)]),
        "sub",
    );

    // We duplicate some sync times when performing the sync on the target item.
    let cleaned_items = metadata_store.clean_up_local_sync_times().unwrap();
    assert_eq!(cleaned_items, 1);

    assert_sync_time(&metadata_store, "", remote_store.id, 0);
    assert_sync_time(&metadata_store, "sub", remote_store.id, 100);
    assert_sync_time(&metadata_store, "sub/folder/file", remote_store.id, 100);

    // Also try to 'partially' bump the sync times.
    bump_sync_time(
        &metadata_store,
        VersionVector::from_initial_values(vec![(&local_store.id, 50), (&remote_store.id, 70)]),
        "",
    );

    assert_sync_time(&metadata_store, "", remote_store.id, 70);
    assert_sync_time(&metadata_store, "sub", remote_store.id, 100);
    assert_sync_time(&metadata_store, "sub/folder/file", remote_store.id, 100);

    assert_sync_time(&metadata_store, "", local_store.id, 50);
    assert_sync_time(&metadata_store, "sub", local_store.id, 50);
    assert_sync_time(&metadata_store, "sub/folder/file", local_store.id, 50);

    // We should not yet see any duplicated sync times, as we only change parent items directly.
    let cleaned_items = metadata_store.clean_up_local_sync_times().unwrap();
    assert_eq!(cleaned_items, 0);

    // Let's query an item, change it and re-synchronize it into our local db
    let mut file = metadata_store
        .get_local_data_item(&RelativePath::from_path("sub/folder/file"), true)
        .unwrap();

    // ...this should be as if the second store overwrites the local one with a new version.
    let new_mod_time = VersionVector::from_initial_values(vec![(&remote_store.id, 420)]);
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
        .get_local_data_item(&RelativePath::from_path("sub/folder/file"), true)
        .unwrap();
    assert_eq!(file_after_update.sync_time[&local_store.id], 50);
    assert_eq!(file_after_update.sync_time[&remote_store.id], 1024);
    assert_eq!(file_after_update.mod_time()[&local_store.id], 0);
    assert_eq!(file_after_update.mod_time()[&remote_store.id], 420);
    let root_item_after_update = metadata_store
        .get_local_data_item(&RelativePath::from_path(""), true)
        .unwrap();
    assert_eq!(root_item_after_update.mod_time()[&local_store.id], 5);
    assert_eq!(root_item_after_update.mod_time()[&remote_store.id], 420);

    // Try a more complicated case where we change a folder to be a file
    let mut folder = metadata_store
        .get_local_data_item(&RelativePath::from_path("sub/folder"), true)
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

    // Delete duplicated sync times, we simply expect it to not break anything here.
    metadata_store.clean_up_local_sync_times().unwrap();

    // We expect the file below to be implicitly deleted and have the appropriate sync time.
    let item_after_update = metadata_store
        .get_local_data_item(&RelativePath::from_path("sub/folder/file"), true)
        .unwrap();
    assert!(item_after_update.is_deletion());
    assert_eq!(item_after_update.sync_time[&remote_store.id], 2048);

    // Another interesting case is if we receive a single deletion notice.
    let mut root = metadata_store
        .get_local_data_item(&RelativePath::from_path(""), true)
        .unwrap();

    let new_sync_time = VersionVector::from_initial_values(vec![(&remote_store.id, 4096)]);
    root.sync_time = new_sync_time;

    metadata_store
        .sync_local_data_item(&RelativePath::from_path("sub/folder"), &root)
        .unwrap();

    let root_after_update = metadata_store
        .get_local_data_item(&RelativePath::from_path("sub/folder/file"), true)
        .unwrap();
    assert!(root_after_update.is_deletion());
    assert_eq!(root_after_update.sync_time[&remote_store.id], 4096,);
    let file_item_after_update = metadata_store
        .get_local_data_item(&RelativePath::from_path("sub/folder/file"), true)
        .unwrap();
    assert!(file_item_after_update.is_deletion());
    assert_eq!(file_item_after_update.sync_time[&remote_store.id], 4096,);
}

#[test]
fn transfer_significant_sync_times() {
    let metadata_store = open_metadata_store();
    let (data_set, local_store) = insert_sample_data_set(&metadata_store);
    let remote_store = insert_data_store(&metadata_store, &data_set, "remote", false);

    // Insert some items to assign sync times to.
    insert_data_item(&metadata_store, "file-0", true);
    insert_data_item(&metadata_store, "file-1", true);
    insert_data_item(&metadata_store, "folder-1", false);
    insert_data_item(&metadata_store, "folder-1/file-2", true);
    insert_data_item(&metadata_store, "folder-1/file-3", true);

    // Enter sync times for the local data store.
    // Significant ones should be "", "file-1", "folder-1/file-3".
    bump_sync_time(
        &metadata_store,
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 5)]),
        "",
    );
    bump_sync_time(
        &metadata_store,
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 5)]),
        "file-0",
    );
    bump_sync_time(
        &metadata_store,
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 8)]),
        "file-1",
    );
    bump_sync_time(
        &metadata_store,
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 5)]),
        "folder-1",
    );
    bump_sync_time(
        &metadata_store,
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 5)]),
        "folder-1/file-2",
    );
    bump_sync_time(
        &metadata_store,
        VersionVector::from_initial_values(vec![(&local_store.id, 15), (&remote_store.id, 15)]),
        "folder-1/file-3",
    );

    // Cleaning of sync times is required to detect significant sync times.
    // (In the future we might want to hold this 'clean sync times' property at all times,
    //  thus sparing the rather expensive clean-up operation).
    metadata_store.clean_up_local_sync_times().unwrap();
    // Snapshot the local significant sync times.
    let local_significant = metadata_store
        .find_significant_sync_times(&local_store)
        .unwrap();
    // Delete some local items and clean up their db entries
    // (this tests the creating of path_components in the enter_significant_sync_times_for call).
    delete_data_item(&metadata_store, "file-0");
    delete_data_item(&metadata_store, "folder-1/file-3");
    metadata_store.optimize_db().unwrap();

    // Simulate that we get to know the remote stores view of the system (which was what we
    // initially setup for this test before deleting the items).
    metadata_store
        .enter_significant_sync_times(&remote_store, local_significant)
        .unwrap();

    // Queries should now return the expected, initial state.
    assert_remote_sync_time(
        &metadata_store,
        &remote_store,
        "",
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 5)]),
    );
    assert_remote_sync_time(
        &metadata_store,
        &remote_store,
        "file-0",
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 5)]),
    );
    assert_remote_sync_time(
        &metadata_store,
        &remote_store,
        "file-1",
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 8)]),
    );
    assert_remote_sync_time(
        &metadata_store,
        &remote_store,
        "folder-1",
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 5)]),
    );
    assert_remote_sync_time(
        &metadata_store,
        &remote_store,
        "folder-1/file-2",
        VersionVector::from_initial_values(vec![(&local_store.id, 10), (&remote_store.id, 5)]),
    );
    assert_remote_sync_time(
        &metadata_store,
        &remote_store,
        "folder-1/file-3",
        VersionVector::from_initial_values(vec![(&local_store.id, 15), (&remote_store.id, 15)]),
    );
}

fn assert_remote_sync_time(
    metadata_store: &MetadataDB,
    data_store: &DataStore,
    path: &str,
    sync_time: VersionVector<i64>,
) {
    let db_sync_time = metadata_store
        .find_sync_time(&data_store, &RelativePath::from_path(path))
        .unwrap();
    for (store_id, time) in sync_time.iter() {
        assert_eq!(db_sync_time[store_id], *time);
    }
}

#[test]
fn store_inclusion_rules() {
    let metadata_store = open_metadata_store();
    let (_data_set, data_store) = insert_sample_data_set(&metadata_store);

    let mut rules = metadata_store.get_inclusion_rules(&data_store).unwrap();
    assert_eq!(
        rules,
        vec![DBInclusionRule {
            include: true,
            rule: glob::Pattern::new("**").unwrap()
        }]
    );

    rules.push(DBInclusionRule {
        include: false,
        rule: glob::Pattern::new("/file-1").unwrap(),
    });
    metadata_store
        .set_inclusion_rules(&data_store, &rules)
        .unwrap();
    let required_rules = metadata_store.get_inclusion_rules(&data_store).unwrap();
    assert_eq!(rules, required_rules);

    rules.pop();
    rules.pop();
    rules.push(DBInclusionRule {
        include: true,
        rule: glob::Pattern::new("/sub_dir").unwrap(),
    });

    metadata_store
        .set_inclusion_rules(&data_store, &rules)
        .unwrap();
    let required_rules = metadata_store.get_inclusion_rules(&data_store).unwrap();
    assert_eq!(rules, required_rules);
}
