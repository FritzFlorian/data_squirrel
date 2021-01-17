use super::*;
use fs_interaction::virtual_fs::{InMemoryFS, FS};
use std::fs::File;
use std::io::Write;
use std::thread::sleep;
use std::time::Duration;

#[test]
fn create_data_store() {
    let test_dir = tempfile::tempdir().unwrap();
    let data_store =
        DefaultDataStore::create(test_dir.path(), "XYZ-123", "XYZ", "local-data-store").unwrap();

    let data_set = data_store.db_access.get_data_set().unwrap();
    assert_eq!(data_set.unique_name, "XYZ-123");
    assert_eq!(data_set.human_name, "XYZ");

    let this_data_store = data_store.db_access.get_local_data_store().unwrap();
    assert_eq!(
        this_data_store.path_on_device,
        test_dir.path().canonicalize().unwrap().to_str().unwrap()
    );
    assert_eq!(this_data_store.human_name, "local-data-store");
}

#[test]
fn re_open_data_store() {
    let test_dir = tempfile::tempdir().unwrap();

    // Should succeed in creating a new data-store in the empty directory.
    let data_store_1 =
        DefaultDataStore::create(&test_dir.path(), "XYZ", "XYZ", "local-data-store").unwrap();
    drop(data_store_1);

    // Should fail because we can not re-create in this directory.
    assert!(DefaultDataStore::create(&test_dir.path(), "XYZ", "XYZ", "local-data-store").is_err());

    // Should succeed to open the just opened data-store.
    let _data_store_2 = DefaultDataStore::open(test_dir.path()).unwrap();

    // Should fail, as the data store is already opened.
    assert!(DefaultDataStore::open(test_dir.path()).is_err());
}

#[test]
fn scan_data_store_directory() {
    let in_memory_fs = virtual_fs::InMemoryFS::new();
    let data_store_1 =
        DataStore::create_with_fs("", "XYZ", "XYZ", "local-data-store", in_memory_fs.clone())
            .unwrap();

    // Initial data set
    in_memory_fs.create_dir("sUb-1", false).unwrap();
    in_memory_fs.create_dir("sUb-1/sub-1-1", false).unwrap();
    in_memory_fs.create_dir("sUb-2", false).unwrap();

    in_memory_fs.create_file("file-1").unwrap();
    in_memory_fs.create_file("file-2").unwrap();
    in_memory_fs.create_file("sUb-1/file-1").unwrap();

    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 6,
            changed_items: 0,
            new_items: 6,
            deleted_items: 0
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 6);

    // Detect new and changed files
    in_memory_fs.create_file("file-3").unwrap();
    in_memory_fs
        .test_set_file_content("file-1", Vec::from("hello"))
        .unwrap();
    in_memory_fs.test_increase_file_mod_time("file-1").unwrap();

    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 7,
            changed_items: 1,
            new_items: 1,
            deleted_items: 0
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 8);

    // Detect deleted files and directories
    in_memory_fs.remove_file("file-1").unwrap();
    in_memory_fs.remove_file("sUb-1/file-1").unwrap();
    in_memory_fs.remove_dir("sUb-1/sub-1-1").unwrap();
    in_memory_fs.remove_dir("sUb-1").unwrap();

    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 3,
            changed_items: 0,
            new_items: 0,
            deleted_items: 4
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 12);

    // Re-add some
    in_memory_fs.create_file("file-1").unwrap();
    in_memory_fs.create_dir("sUb-1", false).unwrap();
    in_memory_fs.create_file("sUb-1/file-1").unwrap();
    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 6,
            changed_items: 0,
            new_items: 3,
            deleted_items: 0
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 15);

    // Changes in capitalization should be recognized as metadata changes
    in_memory_fs.rename("file-1", "FILE-1").unwrap();
    in_memory_fs.rename("sUb-1", "SUB-1").unwrap();
    in_memory_fs.rename("SUB-1/file-1", "SUB-1/FILE-1").unwrap();

    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 6,
            changed_items: 3,
            new_items: 0,
            deleted_items: 0
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 18);
    let changes = data_store_1.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 6,
            changed_items: 0,
            new_items: 0,
            deleted_items: 0
        }
    );
    assert_eq!(data_store_1.local_time().unwrap(), 18);
}

fn dir_should_contain<FS: virtual_fs::FS>(fs: &FS, path: &str, expected_content: Vec<&str>) {
    let dir_entries = fs.list_dir(path).unwrap();
    for expected_item in expected_content {
        assert!(dir_entries
            .iter()
            .any(|item| item.file_name == expected_item));
    }
}

fn dir_should_not_contain<FS: virtual_fs::FS>(
    fs: &FS,
    path: &str,
    not_expected_content: Vec<&str>,
) {
    let dir_entries = fs.list_dir(path).unwrap();
    for not_expected_item in not_expected_content {
        assert!(!dir_entries
            .iter()
            .any(|item| item.file_name == not_expected_item));
    }
}

fn create_in_memory_store() -> (InMemoryFS, DataStore<InMemoryFS>) {
    let fs = virtual_fs::InMemoryFS::new();
    let data_store =
        DataStore::create_with_fs("", "XYZ", "XYZ", "source-data-store", fs.clone()).unwrap();

    (fs, data_store)
}

#[test]
fn unidirectional_sync() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    // Initial Data Set - Local Data Store
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_dir("sub-1/sub-1-1", false).unwrap();
    fs_1.create_dir("sub-2", false).unwrap();
    fs_1.create_file("file-1").unwrap();
    fs_1.create_file("file-2").unwrap();
    fs_1.create_file("sub-1/file-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_2
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    // We should have the files on the second store
    dir_should_contain(&fs_2, "", vec!["sub-1", "sub-2", "file-1", "file-2"]);
    let changes = data_store_2.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 6,
            changed_items: 0,
            new_items: 0,
            deleted_items: 0
        }
    );

    // Lets do some non-conflicting changes in both stores
    fs_2.test_set_file_content("file-2", "testing".to_owned().into_bytes())
        .unwrap();
    fs_2.test_increase_file_mod_time("file-2").unwrap();

    fs_1.create_file("file-3").unwrap();
    fs_1.remove_file("file-1").unwrap();

    // Fully scan and sync them
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_2
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    data_store_1
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""))
        .unwrap();

    // The contents should now match without any conflicts
    dir_should_contain(&fs_1, "", vec!["sub-1", "sub-2", "file-2", "file-3"]);
    dir_should_contain(&fs_2, "", vec!["sub-1", "sub-2", "file-2", "file-3"]);
}

#[test]
#[should_panic(expected = "Must not sync if disk content is not correctly indexed in DB")]
fn panics_when_trying_to_sync_without_index_1() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    // Initial Data Set - Local Data Store
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_file("file-1").unwrap();
    fs_2.create_file("file-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    // Rename item on receiving data store after scan operation
    fs_2.rename("file-1", "FILE-1").unwrap();
    data_store_2
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
}

#[test]
#[should_panic(expected = "Must not sync if disk content is not correctly indexed in DB")]
fn panics_when_trying_to_sync_without_index_2() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    // Initial Data Set - Local Data Store
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_file("file-1").unwrap();
    fs_2.create_file("file-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    // Delete item on receiving data store after scan operation
    fs_2.remove_file("file-1").unwrap();
    data_store_2
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
}

#[test]
#[should_panic(expected = "Must not sync if disk content is not correctly indexed in DB")]
fn panics_when_trying_to_sync_without_index_3() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    // Initial Data Set - Local Data Store
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_file("file-1").unwrap();
    fs_2.create_file("file-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    // Delete item on sending data store after scan operation
    fs_1.remove_file("file-1").unwrap();
    data_store_2
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
}

#[test]
#[should_panic(expected = "Must not sync if disk content is not correctly indexed in DB")]
fn panics_when_trying_to_sync_without_index_4() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();

    // Initial Data Set - Local Data Store
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_file("file-1").unwrap();
    fs_2.create_file("file-1").unwrap();

    // Index it and sync it to the remote data store
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    // Modify item on sending data store after scan operation
    fs_1.test_set_file_content("file-1", b"test".to_vec())
        .unwrap();
    data_store_2
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
}

#[test]
fn metadata_set_correctly_after_sync() {
    // We experienced a bug where after sycing a file from A -> B the transmitted file is
    // detected to have a change when then scanning the disk content on B.
    // For example, say you create test.txt on A, sync it to B, then the next scan on B will
    // detect a change in test.txt, even though it was not changed locally.

    let test_dir_1 = tempfile::tempdir().unwrap();
    let test_dir_2 = tempfile::tempdir().unwrap();

    let data_store_1 =
        DefaultDataStore::create(test_dir_1.path(), "XYZ", "XYZ", "source-data-store").unwrap();
    let data_store_2 =
        DefaultDataStore::create(test_dir_2.path(), "XYZ", "XYZ", "source-data-store").unwrap();

    // Create file in store 1 and sync it to store 2
    File::create(test_dir_1.path().join("test.txt"))
        .unwrap()
        .write_all(b"hello!")
        .unwrap();
    data_store_1.perform_full_scan().unwrap();

    // ..give some time for the time stamps of the newly created file to be different.
    sleep(Duration::from_millis(10));
    data_store_2
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""))
        .unwrap();

    // A scan on store 2 should now NOT result in any changes
    let changes = data_store_2.perform_full_scan().unwrap();
    assert_eq!(
        changes,
        ScanResult {
            indexed_items: 1,
            changed_items: 0,
            new_items: 0,
            deleted_items: 0
        }
    );
}

#[test]
fn multi_target() {
    let (fs_1, data_store_1) = create_in_memory_store();
    let (fs_2, data_store_2) = create_in_memory_store();
    let (fs_3, data_store_3) = create_in_memory_store();

    // Initial Data Set
    fs_1.create_dir("sub-1", false).unwrap();
    fs_1.create_file("sub-1/file-1").unwrap();

    fs_2.create_dir("sub-2", false).unwrap();
    fs_2.create_file("sub-2/file-1").unwrap();

    // Index all
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_3.perform_full_scan().unwrap();

    // Sync from 1 to 3...
    data_store_3
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_3, "", vec!["sub-1"]);
    dir_should_contain(&fs_3, "sub-1", vec!["file-1"]);
    // ...then from 3 to 2 (so effectively from 1 to 2)
    data_store_2
        .sync_from_other_store(&data_store_3, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_2, "", vec!["sub-1", "sub-2"]);
    dir_should_contain(&fs_2, "sub-1", vec!["file-1"]);
    dir_should_contain(&fs_2, "sub-2", vec!["file-1"]);

    // Finally, finish the sync-circle (from 2 to 1)
    data_store_1
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_1, "", vec!["sub-1", "sub-2"]);
    dir_should_contain(&fs_1, "sub-1", vec!["file-1"]);
    dir_should_contain(&fs_1, "sub-2", vec!["file-1"]);

    // Let's do some more complicated changes
    fs_1.create_file("sub-1/file-2").unwrap();
    fs_2.remove_file("sub-1/file-1").unwrap();
    fs_2.remove_file("sub-2/file-1").unwrap();

    // Index all
    data_store_1.perform_full_scan().unwrap();
    data_store_2.perform_full_scan().unwrap();
    data_store_3.perform_full_scan().unwrap();

    // Get all changes to store 3
    data_store_3
        .sync_from_other_store(&data_store_1, &RelativePath::from_path(""))
        .unwrap();
    data_store_3
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_3, "", vec!["sub-1", "sub-2"]);
    dir_should_contain(&fs_3, "sub-1", vec!["file-2"]);
    dir_should_not_contain(&fs_3, "sub-1", vec!["file-1"]);
    dir_should_not_contain(&fs_3, "sub-2", vec!["file-1"]);

    // Re-create an independent 'sub-1/file-1' on store 2
    fs_2.create_file("sub-1/file-1").unwrap();
    data_store_2.perform_full_scan().unwrap();

    // Sync back (we expect to have all changes from store 2 and the file-1 still exists)
    data_store_3
        .sync_from_other_store(&data_store_2, &RelativePath::from_path(""))
        .unwrap();
    dir_should_contain(&fs_3, "", vec!["sub-1", "sub-2"]);
    dir_should_contain(&fs_3, "sub-1", vec!["file-2", "file-1"]);
    dir_should_not_contain(&fs_3, "sub-2", vec!["file-1"]);
}

#[test]
fn convert_from_and_to_external_version_vectors() {
    let (_fs_1, data_store_1) = create_in_memory_store();
    let (_fs_2, data_store_2) = create_in_memory_store();

    let data_store_1_name = data_store_1
        .db_access
        .get_local_data_store()
        .unwrap()
        .unique_name;

    let (mapper_1, mapper_2) = data_store_1.sync_data_store_lists(&data_store_2).unwrap();

    // Create a vector local to store 1
    let data_store_1_id = data_store_1
        .db_access
        .get_data_store(&data_store_1_name)
        .unwrap()
        .unwrap()
        .id;
    let mut vector_on_store_1 = VersionVector::new();
    vector_on_store_1[&data_store_1_id] = 42;

    // Simulate the 'externalize and internalize' procedure to transfer it to store 2.
    let internalized_vector_on_store_2 = mapper_2.external_to_internal(&vector_on_store_1);

    // Check it...
    let data_store_2_id = data_store_2
        .db_access
        .get_data_store(&data_store_1_name)
        .unwrap()
        .unwrap()
        .id;
    assert_eq!(internalized_vector_on_store_2[&data_store_2_id], 42);

    // Transfer it back
    let internalized_vector_on_store_1 =
        mapper_1.external_to_internal(&internalized_vector_on_store_2);
    assert_eq!(internalized_vector_on_store_1[&data_store_1_id], 42);
}
